import datetime
import httpx
from fastapi import FastAPI, Request, HTTPException, status
from contextlib import asynccontextmanager
from urllib.parse import urlparse, unquote
from control_plane_db import ControlPlaneDB
import asyncio
import uvicorn
import uuid
import os
import json
import time
import datetime


from logger_utils import log


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

    async def initialize(self):
        log("LoadBalancer", "ScalerClient.initialize", status="waiting_for_socket")
        while not os.path.exists("/tmp/scaler.sock"):
            await asyncio.sleep(0.1)
        self.reader, self.writer = await asyncio.open_unix_connection("/tmp/scaler.sock")
        log("LoadBalancer", "ScalerClient.initialize", status="connected")


    async def poke_scaler(self, request_id):
        log("LoadBalancer", "ScalerClient.poke_scaler", request_id=request_id)
        try:
            async with self.lock:
                self.writer.write(f"{request_id}".encode())
                await self.writer.drain()
                log("LoadBalancer", "ScalerClient.poke_scaler", request_id=request_id, status="poked")
        except Exception as e:
            log("LoadBalancer", "ScalerClient.poke_scaler", request_id=request_id, error=str(e))
            print(e)
        


async def start_heartbeat(component_name):
    db = ControlPlaneDB()
    pid = os.getpid()
    
    while True:
        async with db.db_connection() as conn:
            await conn.execute(
                "REPLACE INTO control_plane_health (component_name, pid, last_heartbeat) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (component_name, pid)
            )
            await conn.commit()
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
    while not available_lambda_ip:
        available_lambda_ip = next(iter(available_lambdas.get(lambda_name,{})),None)
        if not available_lambda_ip:
            await scaler_client.poke_scaler()
            await asyncio.sleep(0.1)
        else:
            available_lambda_event = available_lambdas[lambda_name].pop(available_lambda_ip)
            break
    
    lambda_request_events[request_id]=asyncio.Event()
    lambda_request_payloads[available_lambda_ip] = await request.json()
    available_lambda_event.set()
    try:
        await asyncio.wait_for(lambda_request_events[request_id].wait())
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="The upstream server failed to respond in time."
        )
    
    return lambda_request_responses.pop(request_id)


@app.post("/register/container")
async def register_container(request: Request):
    log("LoadBalancer", "register_container")
    container_data = await request.json()
    log("LoadBalancer", "register_container", container_data=container_data)

    container_ip = container_data.get("ip_address")
    lambda_name = container_data.get("lambda_name")

    if container_ip in lambdas_pending_registration:
        registered_lambdas[container_ip] = lambda_name
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
        lambdas_pending_registration[lambda_ip] = asyncio.Event()
        try:
            while lambda_ip not in registered_lambdas:
                if await request.is_disconnected():
                    return None
                try:
                    asyncio.wait_for(lambdas_pending_registration[lambda_ip].wait(), timeout=0.1)
                    lambdas_pending_registration[lambda_ip].clear()
                    del lambdas_pending_registration[lambda_ip]
                    break
                except asyncio.TimeoutError:
                    container = await control_plane_db.get_available_lambda_instance
                    if container:
                        registered_lambdas[lambda_ip] = container['lambda_name']
                        lambdas_pending_registration[lambda_ip].clear()
                        del lambdas_pending_registration[lambda_ip]
                        break
        except Exception as e:
            log("LoadBalancer", "runtime_invocation_next", error=str(e))
        finally:
            lambdas_pending_registration.pop(lambda_ip, None)
            registered_lambdas.pop(lambda_ip, None)

    lambda_name = registered_lambdas[lambda_ip]
    if lambda_name not in available_lambdas:
        available_lambdas[lambda_name]={}
    available_lambdas[lambda_name][lambda_ip] = asyncio.Event()

    try:
        while True:
            # cleanup loop if a lambda disconnects
            if await request.is_disconnected():
                available_lambdas[lambda_name].pop(lambda_ip, None)
                registered_lambdas.pop(lambda_ip, None)
                lambda_request_events.pop(lambda_ip, None)
                lambda_request_payloads.pop(lambda_ip, None)
                lambda_request_responses.pop(lambda_ip, None)
                return None


            try:
                asyncio.wait_for(available_lambdas[lambda_name][lambda_ip].wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                continue

    except Exception as e:
        return None
    finally:
        lambda_request_payloads.pop(lambda_ip, None)
        available_lambdas[lambda_name].pop(lambda_ip, None)

    lambda_payload = lambda_request_payloads.pop(lambda_ip,None)

    return lambda_payload

@app.post("/{sdk_date}/runtime/invocation/{request_id}/response")
async def runtime_invocation_response(request_id: str, request: Request):
    log("LoadBalancer", "runtime_invocation_response", request_id=request_id)
    # Handle lambda results here
    lambda_request_responses[request_id] = await request.json()
    lambda_request_event = lambda_request_events.pop(request_id)
    lambda_request_event.set()
    return {"status": "accepted"}

@app.post("/{sdk_date}/runtime/invocation/{request_id}/error")
async def runtime_invocation_error(request_id: str, request: Request):
    log("LoadBalancer", "runtime_invocation_error", request_id=request_id)
    # Handle lambda errors here

    lambda_request_responses[request_id] = await request.json()
    lambda_request_event = lambda_request_events.pop(request_id)
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