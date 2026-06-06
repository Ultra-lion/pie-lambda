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

listening_workers = {} # key-> lambda_ip: value-> (asyncio.Event, request_connection)
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

    heartbeat_task = loop.create_task(start_heartbeat("WORKER_MANAGER"))
    heartbeat_task.add_done_callback(background_tasks.discard)

    log("LoadBalancer", "startup_event", status="ready")
    yield

    log("LoadBalancer", "startup_event", status="shutting_down")
    heartbeat_task.cancel()


app = FastAPI(lifespan=startup_event)


@app.post("/proxy_request/{lambda_name}/{request_id}")
async def proxy_request(request: Request, request_id:str, lambda_name:str):
    global control_plane_db, scaler_client
    
    payload_base = await request.json()
    
    start_time = datetime.datetime.now(timezone.utc)
    try:
        
        worker_ip = None
        worker_event = None
        worker_request_connection = None
        
        # Pre-register the response event to avoid missing fast worker responses
        response_events[request_id] = asyncio.Event()

        while True:
            elapsed = (datetime.datetime.now(timezone.utc) - start_time).total_seconds()
            if elapsed > (LAMBDA_TIMEOUT * 60):
                raise HTTPException(status_code=504, detail="no workers available")
            
            # Note: Fixed method name to plural and handling dict return
            worker_data = await control_plane_db.get_available_containers(lambda_name)
            log("WorkerManager", "proxy_request", status="trying to get worker", lambda_name=lambda_name, request_id=request_id)
            if not worker_data:
                # Poke scaler to ensure we have workers starting
                await scaler_client.poke_scaler()
                await asyncio.sleep(1)
                continue

            worker_ip = worker_data['ip_address']
            worker_event, worker_request_connection = listening_workers.get(worker_ip, (None, None))
            log("WorkerManager", "proxy_request", status="got worker", ip=worker_ip)
            if not worker_event:
                # Gap: Worker exists in DB but hasn't reached /next registration yet
                log("WorkerManager", "proxy_request", status="worker_registration_gap", ip=worker_ip)
                await asyncio.sleep(1)
                continue
            
            if await worker_request_connection.is_disconnected():
                # Ghost: Connection is dead, mark as failed so scaler reaps it
                log("WorkerManager", "proxy_request", status="worker_ghost_detected", ip=worker_ip)
                await control_plane_db.mark_instance_as_failed(worker_ip)
                continue

            # If we reach here, we have a valid worker IP and its event
            payload = payload_base.copy()
            payload['request_id'] = request_id
            worker_payloads[worker_ip] = payload
            
            # Update request status to in_progress now that it's bound to a worker
            await control_plane_db.update_lambda_request(request_id, {"status": "in_progress"})
            worker_event.set()
            log("WorkerManager", "proxy_request", status="worker event set", ip=worker_ip)
            # 3. Wait for response
            try:
                await asyncio.wait_for(response_events[request_id].wait(), timeout=LAMBDA_TIMEOUT * 60)
            except asyncio.TimeoutError:
                pass 
            
            res = await control_plane_db.get_request_status(request_id)
            if res.get("status")=="pending" and res.get("response_data")=="worker_disconnected":
                continue
            elif res.get("status") in ["pending","in_progress"]:
                raise HTTPException(status_code=504, detail="workers took too long")
            

            return json.loads(res['response_data']) if res['response_data'] else {}
        
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Lambda timeout or no workers available")
    finally:
        response_events.pop(request_id, None)

@app.get("/{sdk_date}/runtime/invocation/next")
async def runtime_invocation_next(request: Request):
    global control_plane_db, scaler_client
    lambda_ip = request.client.host
    
    # Accuracy check: don't queue if already gone
    if await request.is_disconnected():
        log("WorkerManager", "runtime_invocation_next", status="disconnected_early", ip=lambda_ip)
        return None
    
    payload = None
    request_id = None
    try:
        event = asyncio.Event()
        listening_workers[lambda_ip] = (event, request)
        while True:
            try:
                ip_address = await control_plane_db.mark_instance_as_available(lambda_ip)
                if not ip_address:
                    await asyncio.sleep(1)
                    continue
                break
            except Exception as e:
                await asyncio.sleep(1)
                continue
        
        while True:
            try:
                log("WorkerManager", "runtime_invocation_next", status="waiting for request", ip=lambda_ip)
                await asyncio.wait_for(event.wait(), timeout=LAMBDA_TIMEOUT * 60)
                log("WorkerManager", "runtime_invocation_next", status="got for request", ip=lambda_ip)
                if await request.is_disconnected():
                    log("WorkerManager", "runtime_invocation_next", status="disconnected_after_wakeup", ip=lambda_ip)
                    payload = worker_payloads.pop(lambda_ip, None)
                    await control_plane_db.mark_instance_as_failed(lambda_ip)
                    if payload:
                        request_id = payload.get('request_id')
                        if request_id:
                            await control_plane_db.update_lambda_request(request_id, {"status": "pending", "response_data": "worker_disconnected"})
                            log("WorkerManager", "runtime_invocation_next", status="reverted_request_status", request_id=request_id)
                    return None 

                payload = worker_payloads.pop(lambda_ip) 
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
                continue
    finally:
        listening_workers.pop(lambda_ip, None)

@app.post("/{sdk_date}/runtime/invocation/{request_id}/response")
async def runtime_invocation_response(request_id: str, request: Request):
    lambda_ip = request.client.host
    resp_body = await request.json()
    
    await control_plane_db.update_lambda_request(request_id, {"status": "processed", "response_data": json.dumps(resp_body)})
    
    # Wake up proxy_request
    if request_id in response_events:
        response_events[request_id].set()
    
    return {"status": "accepted"}

@app.post("/{sdk_date}/runtime/init/error")
async def runtime_init_error(sdk_date: str, request: Request):
    lambda_ip = request.client.host
    error_payload = await request.json()
    await control_plane_db.mark_instance_as_failed(lambda_ip)
    log("WorkerManager", "runtime_init_error", ip=lambda_ip, error=error_payload)
    return {"status": "accepted"}

@app.post("/2020-01-01/extension/register")
async def extension_register(request: Request):
    # Returning a dummy extension ID to satisfy runtimes that check for it
    return JSONResponse(content={"functionName": "lambda", "functionVersion": "$LATEST", "handler": "handler"}, headers={"Lambda-Extension-Identifier": "dummy-ext-id"})

@app.put("/2022-07-01/telemetry")
@app.put("/2020-08-15/logs")
async def telemetry_blackhole():
    return {"status": "accepted"}

@app.post("/{sdk_date}/runtime/invocation/{request_id}/error")
async def runtime_invocation_error(request_id: str, request: Request):
    log("worker_manager", "runtime_invocation_error", request_id=request_id)
    lambda_ip = request.client.host
    # Handle lambda errors here
    await control_plane_db.update_lambda_request(request_id, {"status": "failed", "response_data": json.dumps(await request.json())})

    # Wake up proxy_request
    if request_id in response_events:
        response_events[request_id].set()
    
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