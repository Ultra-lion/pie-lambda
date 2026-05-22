import multiprocessing
import uvicorn
import json
import os
import sys
import asyncio
import datetime

import socket
import subprocess
import signal


from control_plane_db import ControlPlaneDB
COMPONENT_COMMANDS = {
    "LOAD_BALANCER": [sys.executable, "control_plane/load_balance_lambdas.py"],
    "SCALER": [sys.executable, "control_plane/scale_lambda_dockers.py"],
    "DNS_SERVER": [sys.executable, "control_plane/internal_dns.py"],
    "EVENTS_HANDLER": [sys.executable, "control_plane/handle_lambda_events_queue.py"]
}

def restart_process(name):
    cmd = COMPONENT_COMMANDS.get(name)
    if not cmd:
        print(f"Unknown component: {name}")
        return None
    
    # Start the process in the background
    return subprocess.Popen(cmd)



def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def parse_timestamp(ts_string):
    # SQLite default format is YYYY-MM-DD HH:MM:SS
    return datetime.datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")

async def watchdog_loop(processes):
    db = ControlPlaneDB()
    while True:
        await asyncio.sleep(10)
        async with db.db_connection() as conn:
            cursor = await conn.execute("SELECT * FROM control_plane_health")
            health_stats = {row['component_name']: row for row in await cursor.fetchall()}

        for name, proc in processes.items():
            # 1. Check if the process crashed hard
            if proc.poll() is not None:
                print(f"ALARM: {name} crashed. Restarting...")
                processes[name] = await restart_process(name)
                continue

            # 2. Check if the process is "Ghosted" (Frozen/Deadlocked)
            stat = health_stats.get(name)
            if not stat or (datetime.now() - parse_timestamp(stat['last_heartbeat'])).total_seconds() > 20:
                print(f"ALARM: {name} is frozen. Sending SIGKILL to PID {stat['pid']}...")
                try:
                    os.kill(stat['pid'], signal.SIGKILL)
                except ProcessLookupError:
                    pass
                processes[name] = await restart_process(name)


# Ensure the root directory is in the path for internal imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from internal_dns import run_server as run_dns_server
from load_balance_lambdas import app

def start_load_balancer(config):
    print("🔒 Starting HTTPS Control Plane (Load Balancer) on port 443...")
    # NOTE: Be sure your certs are volume-mounted to these paths!
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=443, 
        ssl_keyfile="/app/control_plane/certs/server.key", 
        ssl_certfile="/app/control_plane/certs/server.crt"
    )

def start_dns_interceptor(config):
    print("🌐 Starting DNS Interceptor on port 53...")
    run_dns_server(config)

async def main():
    db_manager = ControlPlaneDB()
    await db_manager.initialize_db()
    # 3. Start Child Processes (Sync - but called from Async)
    processes = {}
    for name in COMPONENT_COMMANDS:
        # We use a sync restart_process here because Popen is non-blocking
        processes[name] = restart_process(name)
    # 4. Start the Watchdog (Async - happens on the MAIN loop)
    # This blocks forever and keeps the loop alive
    try:
        await watchdog_loop(processes)
    finally:
        # 5. Final Cleanup
        print("Control plane shutting down. Killing children...")
        for name, p in processes.items():
            p.terminate()
    

if __name__ == "__main__":
    # Load configuration
    config_path = os.getenv("CONFIG_PATH", "config.json")
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"⚠️ Config not found at {config_path}, using defaults.")
        config = {}
    
    config['control_plane_ip'] = get_local_ip()
    os.environ['CONTROL_PLANE_IP'] = config['control_plane_ip']

    print(f"""
    PIE-LAMBDA IS ONLINE: Set your Docker Container's DNS to {config['control_plane_ip']} or your Boto3 endpoint to http://{config['control_plane_ip']}
    """)

    asyncio.run(main())
