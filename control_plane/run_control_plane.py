from datetime import datetime
import json
import os
import sys
import asyncio
import datetime

import subprocess
import signal


from utils import get_local_ip, parse_timestamp

from control_plane_db import ControlPlaneDB
COMPONENT_COMMANDS = {
    "LOAD_BALANCER": [sys.executable, "load_balance_lambdas.py"],
    "WORKER_MANAGER": [sys.executable, "worker_manager.py"],
    "SCALER": [sys.executable, "scale_lambda_dockers.py"],
    "DNS_SERVER": [sys.executable, "internal_dns.py"],
    "EVENTS_HANDLER": [sys.executable, "handle_lambda_events_queue.py"]
}

def restart_process(name):
    cmd = COMPONENT_COMMANDS.get(name)
    if not cmd:
        print(f"Unknown component: {name}")
        return None
    env = os.environ.copy()
    env['CONTROL_PLANE_IP'] = os.getenv('CONTROL_PLANE_IP', '127.0.0.1')
    # Start the process in the background
    return subprocess.Popen(cmd, env=env)




WATCHDOG_LOOP_TIME = 5
PROCESS_KILL_TIME = 10
STARTUP_TIME_LIMIT=30

WATCHDOG_FAILURE_LIMIT = 3*len(COMPONENT_COMMANDS)
FAILURE_RESET_LIMIT = 1000


async def watchdog_loop(processes):
    db = ControlPlaneDB()
    failure_count = 0
    last_interval = datetime.datetime.now()
    while True:
        await asyncio.sleep(WATCHDOG_LOOP_TIME)
        if (datetime.datetime.now() - last_interval).total_seconds() > FAILURE_RESET_LIMIT:
            failure_count = 0
            last_interval = datetime.datetime.now()
        
        health_stats = await db.get_all_health_stats()

        for name, proc in processes.items():
            # 1. Check if the process crashed hard
            if proc.poll() is not None:
                print(f"ALARM: {name} crashed. Restarting...")
                processes[name] = restart_process(name)
                continue

            # 2. Check if the process is "Ghosted" (Frozen/Deadlocked)
            stat = health_stats.get(name)
            if not stat or (datetime.datetime.now() - parse_timestamp(stat['last_heartbeat'])).total_seconds() > PROCESS_KILL_TIME:
                print(f"ALARM: {name} is frozen. Sending SIGKILL to PID {stat['pid']}...")
                try:
                    os.kill(stat['pid'], signal.SIGKILL)
                except ProcessLookupError:
                    pass
                processes[name] = restart_process(name)
                failure_count += 1
                if failure_count > WATCHDOG_FAILURE_LIMIT:
                    raise Exception("Control plane components are not healthy after watchdog failure limit")


# Ensure the root directory is in the path for internal imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def main():
    db_manager = ControlPlaneDB()
    await db_manager.initialize_db()
    await db_manager.clear_control_plane_health_stats()
    # 3. Start Child Processes (Sync - but called from Async)
    processes = {}
    for name in COMPONENT_COMMANDS:
        # We use a sync restart_process here because Popen is non-blocking
        processes[name] = restart_process(name)
    
    start_timestamp = datetime.datetime.now()

    all_process_names = list(COMPONENT_COMMANDS.keys())

    while True:
        if (datetime.datetime.now() - start_timestamp).total_seconds() > STARTUP_TIME_LIMIT:
            raise Exception("Control plane components are not healthy after startup time limit")
        
        health_stats = await db_manager.get_all_health_stats()

        health_stats_components = list(health_stats.keys())

        if set(all_process_names) == set(health_stats_components):
            break
        
        await asyncio.sleep(WATCHDOG_LOOP_TIME)
            

    
    # 4. Start the Watchdog (Async - happens on the MAIN loop)
    # This blocks forever and keeps the loop alive
    try:
        await watchdog_loop(processes)
    finally:
        await db_manager.clear_control_plane_health_stats()
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
