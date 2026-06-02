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

available_lambdas = {}

lambdas_pending_registration = {}

registered_lambdas = {}

lambda_request_events={}
lambda_request_payloads = {}
lambda_request_responses = {}

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

    async def poke_scaler(self):
        if not self.available or not self.writer:
            asyncio.create_task(self.initialize())
            return
        
        log("LoadBalancer", "ScalerClient.poke_scaler")
        try:
            async with self.lock:
                self.writer.write("scale pls".encode())
                await self.writer.drain()
                log("LoadBalancer", "ScalerClient.poke_scaler", status="poked")
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
    global scaler_client
    available_lambda_ip = None
    available_lambda_event=None
    availability_start_time = datetime.datetime.now()
    while not available_lambda_ip:
        if datetime.datetime.now() - availability_start_time > datetime.timedelta(minutes=LAMBDA_TIMEOUT):
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="The upstream server failed to respond in time."
            )
        available_lambda_ip = next(iter(available_lambdas.get(lambda_name,{})),None)
        if not available_lambda_ip:
            await scaler_client.poke_scaler()
            await asyncio.sleep(0.5)
        else:
            available_lambda_tuple = available_lambdas[lambda_name].pop(available_lambda_ip,None)
            if not available_lambda_tuple:
                available_lambda_ip=None
                continue
            available_lambda_event, lambda_connection_request = available_lambda_tuple
            if await lambda_connection_request.is_disconnected():
                available_lambda_ip=None
                continue
            if not available_lambda_event:
                available_lambda_ip=None
                continue
            else:
                break
    availibility_end_time = datetime.datetime.now()
    availability_time_taken_seconds = (availibility_end_time - availability_start_time).total_seconds()
    log("Worker Manager", "proxy_request", availability_time_taken_seconds=availability_time_taken_seconds)

    lambda_request_events[request_id] = asyncio.Event()
    try:
        lambda_payload = await request.json()
        if "request_id" not in lambda_payload:
            lambda_payload['request_id'] =  request_id
        lambda_request_payloads[available_lambda_ip] = (lambda_payload, datetime.datetime.now(timezone.utc))  
        available_lambda_event.set()
        
        await asyncio.wait_for(lambda_request_events[request_id].wait(),timeout=(LAMBDA_TIMEOUT*60 - availability_time_taken_seconds))
        return lambda_request_responses.pop(request_id,None)
    except asyncio.TimeoutError:
        lambda_request_payloads.pop(available_lambda_ip,None)
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="The upstream server failed to respond in time."
        )
    finally:
        lambda_request_events.pop(request_id,None)
        lambda_request_responses.pop(request_id,None)


@app.post("/register/container")
async def register_container(request: Request):
    log("LoadBalancer", "register_container")
    container_data = await request.json()
    log("LoadBalancer", "register_container", container_data=container_data)

    container_ip = container_data.get("ip_address")
    lambda_name = container_data.get("lambda_name")

    # Always update the registration to handle IP reuse scenarios
    registered_lambdas[container_ip] = lambda_name

    if container_ip in lambdas_pending_registration:
        event = lambdas_pending_registration.pop(container_ip)
        if event:
            event.set()

    return {"status": "accepted"}

    



# 2. Handle RIE Runtime APIs (Lambda-side)
@app.get("/{sdk_date}/runtime/invocation/next")
async def runtime_invocation_next(request: Request):
    global control_plane_db
    lambda_ip = request.client.host
    log("LoadBalancer", "runtime_invocation_next", status="polling_for_work")
    # Implement long-polling logic here to hand work to containers
    if lambda_ip not in registered_lambdas:
        event = asyncio.Event()
        lambdas_pending_registration[lambda_ip] = event
        try:
            while lambda_ip not in registered_lambdas:
                if await request.is_disconnected():
                    log("WorkerManager", "registration", status="disconnected", ip=lambda_ip)
                    return None
                try:
                    await asyncio.wait_for(event.wait(), timeout=0.5)
                    event.clear()
                    del lambdas_pending_registration[lambda_ip]
                    break
                except asyncio.TimeoutError:
                    container = await control_plane_db.get_lambda_container_by_ip(lambda_ip)
                    if container:
                        registered_lambdas[lambda_ip] = container['lambda_name']
                        event.clear()
                        del lambdas_pending_registration[lambda_ip]
                        break
        except Exception as e:
            log("LoadBalancer", "runtime_invocation_next", error=str(e))
        finally:
            lambdas_pending_registration.pop(lambda_ip, None)
    await control_plane_db.mark_instance_as_available(lambda_ip)
    lambda_name = registered_lambdas[lambda_ip]
    if lambda_name not in available_lambdas:
        available_lambdas[lambda_name]={}
    available_lambdas[lambda_name][lambda_ip] = (asyncio.Event(), request)
    
    tupl = available_lambdas.get(lambda_name,{}).get(lambda_ip,None)
    event, req = tupl
    try:
        while True:
            # cleanup loop if a lambda disconnects
            if await request.is_disconnected():
                log("WorkerManager", "polling", status="disconnected", ip=lambda_ip)
                available_lambdas[lambda_name].pop(lambda_ip, None)
                registered_lambdas.pop(lambda_ip, None)
                lambda_request_payloads.pop(lambda_ip, None)
                return None

            try:
                await asyncio.wait_for(event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                continue

    except Exception as e:
        return None
    finally:
        if lambda_name in available_lambdas:
            tupl = available_lambdas.get(lambda_name,{}).get(lambda_ip,None)
            if tupl and tupl[0] == event:
                available_lambdas[lambda_name].pop(lambda_ip, None)

    lambda_payload_tuple = lambda_request_payloads.pop(lambda_ip,None)
    if lambda_payload_tuple:
        lambda_payload, lambda_request_start_time = lambda_payload_tuple
        request_id = lambda_payload.pop("request_id",None)
        await control_plane_db.mark_instance_as_busy(lambda_ip, request_id)
        if not request_id:
            await control_plane_db.update_lambda_request(request_id, {"status": "failed", "error_data": "Invalid request"})
            raise HTTPException(status_code=422, detail="unprocessable entity")
        headers = {
            "Lambda-Runtime-Aws-Request-Id": str(request_id),
            "Lambda-Runtime-Deadline-Ms": str(LAMBDA_TIMEOUT * 60 * 1000), # Example: 5 minutes from now
        }
        if datetime.datetime.now(timezone.utc) - lambda_request_start_time < datetime.timedelta(seconds=(LAMBDA_TIMEOUT*60 - 1)):
            await control_plane_db.update_lambda_request(request_id, {"status": "in_progress"})
        else:
            await control_plane_db.update_lambda_request(request_id, {"status": "failed", "error_data": "Request timed out"})
            raise HTTPException(status_code=504, detail="Request timed out")
        return JSONResponse(content=lambda_payload, headers=headers)
    
    raise HTTPException(status_code=502, detail="Bad Gateway")

@app.post("/{sdk_date}/runtime/invocation/{request_id}/response")
async def runtime_invocation_response(request_id: str, request: Request):
    log("worker_manager", "runtime_invocation_response", request_id=request_id)
    # Handle lambda results here
    lambda_request_event = lambda_request_events.pop(request_id,None)
    await control_plane_db.mark_instance_as_available(request.client.host)
    if lambda_request_event:
        lambda_request_responses[request_id] = await request.json()
        lambda_request_event.set()
    return {"status": "accepted"}

@app.post("/{sdk_date}/runtime/invocation/{request_id}/error")
async def runtime_invocation_error(request_id: str, request: Request):
    log("worker_manager", "runtime_invocation_error", request_id=request_id)
    # Handle lambda errors here
    lambda_request_event = lambda_request_events.pop(request_id,None)
    await control_plane_db.mark_instance_as_available(request.client.host)
    if lambda_request_event:
        lambda_request_responses[request_id] = await request.json()
        lambda_request_event.set()
    
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