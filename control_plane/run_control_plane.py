import multiprocessing
import uvicorn
import json
import os
import sys
import asyncio

from control_plane_db import ControlPlaneDB
import socket

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

    db_manager = ControlPlaneDB()
    asyncio.run(db_manager.initialize_db())

    # Kick off both services
    processes = [
        multiprocessing.Process(target=start_load_balancer, args=(config,)),
        multiprocessing.Process(target=start_dns_interceptor, args=(config,))
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()
