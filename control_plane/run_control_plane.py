import json
import os
import sys
import asyncio
import datetime

import subprocess
import signal
import psutil


from utils import get_local_ip, parse_timestamp

config = {}

try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except Exception:
    pass

from control_plane_db import ControlPlaneDB
COMPONENT_COMMANDS = {
    "LOAD_BALANCER": [sys.executable, "load_balance_lambdas.py"],
    "WORKER_MANAGER": [sys.executable, "worker_manager.py"],
    "SCALER": [sys.executable, "scale_lambda_dockers.py"],
    "DNS_SERVER": [sys.executable, "internal_dns.py"],
    "EVENTS_HANDLER": [sys.executable, "handle_lambda_events_queue.py"]
}

DB_MULTIPLEXER = {
    "DB_MULTIPLEXER": ["node", "pglite-socket-db.mjs"],
}



def kill_processes_on_ports(ports):
    """
    Finds and kills all processes listening or communicating on specified ports.
    :param ports: A list or set of port integers (e.g., [8080, 3000])
    """
    # Ensure input is a set for faster O(1) lookups
    target_ports = set(ports)
    killed_count = 0

    # Iterate through all running processes
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            # Retrieve internet connections (TCP/UDP) for the process
            connections = proc.net_connections(kind='inet')
            
            for conn in connections:
                # Check if the local port matches our target list
                if conn.laddr and conn.laddr.port in target_ports:
                    print(f"Found process '{proc.info['name']}' (PID: {proc.pid}) on port {conn.laddr.port}")
                    
                    # Terminate the process gently, then force if it won't die
                    proc.terminate()
                    
                    # Wait up to 3 seconds for the process to exit
                    gone, alive = psutil.wait_procs([proc], timeout=0.1)
                    
                    if alive:
                        print(f"Process {proc.pid} refused to exit. Forcing kill...")
                        proc.kill() # Sends SIGKILL / terminates forcefully
                    
                    killed_count += 1
                    break # Move to the next process once this one is handled
                    
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Skip system processes or processes that closed unexpectedly
            continue

    print(f"Cleanup complete. Total processes terminated: {killed_count}")


def restart_process(name):
    cmd = COMPONENT_COMMANDS.get(name) or DB_MULTIPLEXER.get(name)
    if not cmd:
        print(f"Unknown component: {name}")
        return None
    match name:
        case "LOAD_BALANCER":
            if config.get("do_ssl", True):
                kill_processes_on_ports([443])
            else:
                kill_processes_on_ports([444])
        case "WORKER_MANAGER":
            kill_processes_on_ports([80])
        case "DNS_SERVER":
            kill_processes_on_ports([53])
        case "DB_MULTIPLEXER":
            kill_processes_on_ports([6957])
    
    env = os.environ.copy()
    env['CONTROL_PLANE_IP'] = os.getenv('CONTROL_PLANE_IP', '127.0.0.1')
    # Excellence: Pass the config path down to components
    env['CONFIG_PATH'] = os.getenv('CONFIG_PATH', 'config.json')
    # Start the process in the background
    return subprocess.Popen(cmd, env=env)




WATCHDOG_LOOP_TIME = config.get("watchdog_loop_time", 10)
PROCESS_KILL_TIME = config.get("process_kill_time", 10)
STARTUP_TIME_LIMIT=config.get("startup_time_limit", 20)

WATCHDOG_FAILURE_LIMIT = config.get("watchdog_failure_limit", 3*len(COMPONENT_COMMANDS))
FAILURE_RESET_LIMIT = config.get("failure_reset_limit", 10)


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
                failure_count += 1
                processes[name] = restart_process(name)
                continue
            
            if name == "DB_MULTIPLEXER":
                try:
                    reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", 6957), timeout=1)
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    print(f"ALARM: DB_MULTIPLEXER is not healthy. Restarting...")
                    failure_count += 1
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

    processes = {}

    processes["DB_MULTIPLEXER"] = restart_process("DB_MULTIPLEXER")

    while True:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", 6957), timeout=1)
            writer.close()
            await writer.wait_closed()
            print(f"DB_MULTIPLEXER Started")
            break
        except Exception as e:
            print(f"ALARM: DB_MULTIPLEXER Not Started")
            await asyncio.sleep(1)


    db_manager = ControlPlaneDB()
    await db_manager.initialize_db()
    await db_manager.clear_control_plane_health_stats()
    # 3. Start Child Processes (Sync - but called from Async)
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
        
        await asyncio.sleep(1)
            
    all_ready = True
    for name, p in processes.items():
        if p.poll() is not None:
            all_ready = False
            break
    
    if not all_ready:
        raise Exception("Control plane components are not healthy after startup time limit")
    print("EVERYTHING READY")
    # 4. Start the Watchdog (Async - happens on the MAIN loop)
    # This blocks forever and keeps the loop alive
    try:
        # await watchdog_loop(processes)
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        # ... then wait for the watchdog OR the stop event ...
        watchdog_task = asyncio.create_task(watchdog_loop(processes))
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            [watchdog_task, stop_task], 
            return_when=asyncio.FIRST_COMPLETED
        )
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
