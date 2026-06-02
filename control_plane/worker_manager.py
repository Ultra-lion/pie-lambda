from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from control_plane_db import ControlPlaneDB
import asyncio
import uvicorn
import os
import datetime
from datetime import timezone
import json

from logger_utils import log

config = {}

try:
    with open("config.json", "r") as f:
        config = json.load(f)
except Exception:
    pass


LAMBDA_TIMEOUT = config.get("lambda_timeout_mins", 5)

control_plane_db = None

available_workers = {}  # { lambda_name: asyncio.Queue[ip] }
worker_events = {}      # { ip: asyncio.Event }
worker_payloads = {}    # { ip: payload }
response_events = {}    # { request_id: asyncio.Event }
registered_lambdas = {} # { ip: lambda_name }

scaler_client = None
background_tasks = set()

class ScalerClient:
    def __init__(self):
        self.writer = None
        self.reader = None
        self.socket_conn = None
        self.lock = asyncio.Lock()
        self.initializing = False
        self.available = False

    async def initialize(self):
        if self.initializing:
            return
        self.initializing = True
        try:
            log("LoadBalancer", "ScalerClient.initialize", status="waiting_for_socket")
            while not os.path.exists("/tmp/scaler.sock"):
                await asyncio.sleep(0.1)
            
            async with self.lock:
                try:
                    if self.writer:
                        try:
                            self.writer.close()
                            await self.writer.wait_closed()
                        except Exception:
                            pass
                    
                    self.reader, self.writer = await asyncio.open_unix_connection("/tmp/scaler.sock")
                    log("LoadBalancer", "ScalerClient.initialize", status="connected")
                    self.available = True
                except Exception as e:
                    log("LoadBalancer", "ScalerClient.initialize", status="failed", error=str(e))
                    self.available = False
        finally:
            self.initializing = False

    async def poke_scaler(self, message="scale pls"):
        if not self.available or not self.writer:
            asyncio.create_task(self.initialize())
            return
        
        log("LoadBalancer", "ScalerClient.poke_scaler", message=message)
        try:
            async with self.lock:
                self.writer.write(message.encode())
                await self.writer.drain()
        except Exception as e:
            log("LoadBalancer", "ScalerClient.poke_scaler", error=str(e))
            self.available=False
            self.writer = None
            asyncio.create_task(self.initialize())
        


async def start_heartbeat(component_name):
    db = ControlPlaneDB()
    pid = os.getpid()
    
    while True:
        await db.update_health_stats(component_name, pid)
        await asyncio.sleep(5)
    
@asynccontextmanager
async def startup_event(app: FastAPI):
    log("WORKER_MANAGER", "startup_event", status="starting")
    loop = asyncio.get_running_loop()

    global scaler_client
    scaler_client = ScalerClient()

    await scaler_client.initialize()


    global control_plane_db
    control_plane_db = ControlPlaneDB()

    # Recover state from DB
    await sync_available_workers_from_db()

    heartbeat_task = loop.create_task(start_heartbeat("WORKER_MANAGER"))
    heartbeat_task.add_done_callback(background_tasks.discard)

    log("LoadBalancer", "startup_event", status="ready")
    yield

    log("LoadBalancer", "startup_event", status="shutting_down")
    heartbeat_task.cancel()


app = FastAPI(lifespan=startup_event)




@app.post("/register/container")
async def register_container(request: Request):
    container_data = await request.json()
    ip = container_data.get("ip_address")
    lambda_name = container_data.get("lambda_name")
    
    if lambda_name not in available_workers:
        available_workers[lambda_name] = asyncio.Queue()
    
    registered_lambdas[ip] = lambda_name
    await available_workers[lambda_name].put(ip)
    log("WorkerManager", "register_container", ip=ip, lambda_name=lambda_name)
    return {"status": "accepted"}

@app.post("/proxy_request/{lambda_name}/{request_id}")
async def proxy_request(request: Request, request_id:str, lambda_name:str):
    global control_plane_db, scaler_client
    
    # 1. Wait for a worker
    if lambda_name not in available_workers:
        available_workers[lambda_name] = asyncio.Queue()
    
    try:
        # Poke scaler to ensure we have workers starting
        await scaler_client.poke_scaler()
        
        # Wait for an available worker IP (10s timeout before scaling check)
        worker_ip = await asyncio.wait_for(available_workers[lambda_name].get(), timeout=30.0)
        
        # 2. Dispatch payload
        payload = await request.json()
        payload['request_id'] = request_id
        
        # Mark busy in DB before waking worker
        await control_plane_db.mark_instance_as_busy(worker_ip, request_id)
        
        worker_payloads[worker_ip] = payload
        worker_events[worker_ip].set()
        
        # 3. Wait for response
        response_events[request_id] = asyncio.Event()
        await asyncio.wait_for(response_events[request_id].wait(), timeout=LAMBDA_TIMEOUT * 60)
        
        res = await control_plane_db.get_request_status(request_id)
        return json.loads(res['response_data']) if res['response_data'] else {}
        
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Lambda timeout or no workers available")
    finally:
        response_events.pop(request_id, None)

@app.get("/{sdk_date}/runtime/invocation/next")
async def runtime_invocation_next(request: Request):
    global control_plane_db, scaler_client
    lambda_ip = request.client.host
    
    # CRITICAL CHECK 1: Check for disconnection *before* even registering or waiting
    if await request.is_disconnected():
        log("WorkerManager", "runtime_invocation_next", status="disconnected_early", ip=lambda_ip)
        return None

    # Ensure state is clean
    worker_events[lambda_ip] = asyncio.Event()
    
    # Notify Scaler we are ready to be registered
    await scaler_client.poke_scaler(f"REGISTER {lambda_ip}")
    
    payload = None
    request_id = None
    try:
        # Wait for proxy_request to assign a task. Use a long timeout matching LAMBDA_TIMEOUT.
        await asyncio.wait_for(worker_events[lambda_ip].wait(), timeout=LAMBDA_TIMEOUT * 60)
        
        # CRITICAL CHECK 2: After being woken up, verify connection is still alive
        if await request.is_disconnected():
            log("WorkerManager", "runtime_invocation_next", status="disconnected_after_wakeup", ip=lambda_ip)
            # If disconnected here, a payload was assigned by proxy_request.
            # We need to retrieve it and mark the corresponding request as pending again.
            payload = worker_payloads.pop(lambda_ip, None)
            if payload:
                request_id = payload.get('request_id')
                if request_id:
                    # Revert request status in DB so it can be picked up by another worker
                    await control_plane_db.update_lambda_request(request_id, {"status": "pending"})
                    log("WorkerManager", "runtime_invocation_next", status="reverted_request_status", request_id=request_id)
            return None 

        payload = worker_payloads.pop(lambda_ip) # Retrieve the assigned payload
        request_id = payload.get('request_id')
        
        headers = {
            "Lambda-Runtime-Aws-Request-Id": str(request_id),
            "Lambda-Runtime-Deadline-Ms": str(LAMBDA_TIMEOUT * 60 * 1000),
        }
        return JSONResponse(content=payload, headers=headers)
    except asyncio.TimeoutError:
        # This means the worker waited for the full LAMBDA_TIMEOUT * 60 seconds
        # and no task was assigned. It's effectively idle.
        log("WorkerManager", "runtime_invocation_next", status="timeout_waiting_for_task", ip=lambda_ip)
        return None 
    finally:
        # Ensure the event and any assigned payload are cleaned up regardless of outcome
        worker_events.pop(lambda_ip, None)
        worker_payloads.pop(lambda_ip, None) # In case payload was assigned but not sent

@app.post("/{sdk_date}/runtime/invocation/{request_id}/response")
async def runtime_invocation_response(request_id: str, request: Request):
    lambda_ip = request.client.host
    resp_body = await request.json()
    
    await control_plane_db.update_lambda_request(request_id, {"status": "processed", "response_data": json.dumps(resp_body)})
    await control_plane_db.mark_instance_as_available(lambda_ip)
    
    # Wake up proxy_request
    if request_id in response_events:
        response_events[request_id].set()
    
    # Tell scaler to verify and re-register us
    await scaler_client.poke_scaler(f"REGISTER {lambda_ip}")
    return {"status": "accepted"}

@app.post("/{sdk_date}/runtime/invocation/{request_id}/error")
async def runtime_invocation_error(request_id: str, request: Request):
    log("worker_manager", "runtime_invocation_error", request_id=request_id)
    # Handle lambda errors here
    await control_plane_db.update_lambda_request(request_id, {"status": "failed", "response_data": json.dumps(await request.json())})
    await control_plane_db.mark_instance_as_available(request.client.host)
    
    return {"status": "accepted"}









def run_http():
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=80,
        log_level="DEBUG"
    )

if __name__=="__main__":
    run_http()