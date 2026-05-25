import datetime
import httpx
from fastapi import FastAPI, Request, BackgroundTasks
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


lambda_request_events = {}
lambda_request_responses = {}

scaler_client = None



background_tasks = set()


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

    global control_plane_db
    control_plane_db = ControlPlaneDB()

    heartbeat_task = loop.create_task(start_heartbeat("WORKER_MANAGER"))

    heartbeat_task.add_done_callback(background_tasks.discard)

    log("LoadBalancer", "startup_event", status="ready")
    yield

    log("LoadBalancer", "startup_event", status="shutting_down")
    heartbeat_task.cancel()


app = FastAPI(lifespan=startup_event)




@app.post("/proxy_request/{request_id}")
async def proxy_request(request: Request, request_id:str):
    lambda_id = next(iter(available_lambdas))
    lambda_event = lambda_request_events.pop(lambda_id)
    lambda_request_events[request_id]=asyncio.Event()
    lambda_event.set()
    await asyncio.wait_for(lambda_request_events[request_id].wait())
    return lambda_request_responses.pop(request_id)





# 2. Handle RIE Runtime APIs (Lambda-side)
@app.get("/{sdk_date}/runtime/invocation/next")
async def runtime_invocation_next():
    log("LoadBalancer", "runtime_invocation_next", status="polling_for_work")
    # Implement long-polling logic here to hand work to containers
    random_uuid = uuid.uuid4()
    available_lambdas[random_uuid] = asyncio.Event()

    await available_lambdas[random_uuid].wait()

    return {"status": "no_work_yet"}

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