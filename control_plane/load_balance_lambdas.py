import datetime
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from urllib.parse import urlparse, unquote
from control_plane_db import ControlPlaneDB
import asyncio
import uvicorn
import uuid
import os
import time
import datetime

from utils import parse_timestamp

from logger_utils import log



control_plane_db = None


background_tasks = set()

async def start_heartbeat(component_name):
    db = ControlPlaneDB()
    pid = os.getpid()
    
    while True:
        await db.update_health_stats(component_name, pid)
        await asyncio.sleep(5)
    
@asynccontextmanager
async def startup_event(app: FastAPI):
    log("LoadBalancer", "startup_event", status="starting")
    loop = asyncio.get_running_loop()
    global control_plane_db
    control_plane_db = ControlPlaneDB()
    heartbeat_task = loop.create_task(start_heartbeat("LOAD_BALANCER"))
    heartbeat_task.add_done_callback(background_tasks.discard)
    log("LoadBalancer", "startup_event", status="ready")
    yield
    log("LoadBalancer", "startup_event", status="shutting_down")
    heartbeat_task.cancel()


app = FastAPI(lifespan=startup_event)

def extract_lambda_name(path: str):
    parsed_url = urlparse(path)

    path = parsed_url.path

    segments = path.strip('/').split('/')

    if len(segments) < 3 or segments[1] !='functions':
        return None
    
    raw_identifier = segments[2]

    decoded_identifier = unquote(raw_identifier)

    if "function:" in decoded_identifier:
        decoded_identifier = decoded_identifier.split("function:")[-1]
    
    clean_name = decoded_identifier.split(":")[0]
    
    return clean_name





async def proxy_api_call(request: Request|dict = None, lambda_func_name: str = None, type:str = "RequestResponse"):
    log("LoadBalancer", "proxy_api_call", lambda_func_name=lambda_func_name, invocation_type=type)
    
    scaler_health = await control_plane_db.get_component_health("SCALER")
    if scaler_health:
        last_heartbeat = parse_timestamp(scaler_health['last_heartbeat']).timestamp()
    else:
        last_heartbeat = 0
        
    log("LoadBalancer", "proxy_api_call", scaler_last_heartbeat=last_heartbeat, current_time=time.time())
    
    if not scaler_health or (time.time() - last_heartbeat > 20):
        log("LoadBalancer", "proxy_api_call", status="scaler_dead_503")
        raise HTTPException(
            status_code=503,
            detail="Scaler Died"
        )  # Service Unavailable, Scaler is dead
    
    request_id = str(uuid.uuid4())
    log("LoadBalancer", "proxy_api_call", request_id=request_id)
    
    await control_plane_db.create_lambda_request(request_id, lambda_func_name, request, type)
    
    if type == "RequestResponse":
        try:
            payload_body = await request.json()
            payload_body['request_id'] = request_id
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method="POST",
                    url=f"http://127.0.0.1:80/proxy_request/{lambda_func_name}/{request_id}",
                    params=request.query_params,
                    json=payload_body,
                    timeout=60
                )
            
            log("LoadBalancer", "proxy_api_call", request_id=request_id, status="response_received", response_status=response.status_code)
            await control_plane_db.update_lambda_request(request_id, {"status": "success", "response_data": response.text})
            return response.content
        except Exception as e:
            raise e

    elif type == "Event":
        log("LoadBalancer", "proxy_api_call", request_id=request_id, status="event_type_accepted_202")
        return JSONResponse(
            content="Accepted",
            status_code=202
        )
    else:
        log("LoadBalancer", "proxy_api_call", request_id=request_id, status="invalid_type_error")
        raise ValueError("Invalid type")

async def get_lambda_images():
    return []

@app.post("/{sdk_date}/functions/{function_identifier:path}/invocations")
async def proxy_request(request: Request, function_identifier:str,sdk_date: str):
    log("LoadBalancer", "proxy_request", path=request.url.path)
    lambda_func_name = extract_lambda_name(request.url.path)
    log("LoadBalancer", "proxy_request", lambda_func_name=lambda_func_name)
    
    if not lambda_func_name:
       images = await get_lambda_images()
       log("LoadBalancer", "proxy_request", status="returning_lambda_images")
       return images

    lowercase_headers = {k.lower(): v for k, v in request.headers.items()}
    invocation_type = lowercase_headers.get('x-amz-invocation-type', 'RequestResponse')
    log("LoadBalancer", "proxy_request", invocation_type=invocation_type)

    return await proxy_api_call(request, lambda_func_name, invocation_type)



# 3. Management/Diagnostic Route
@app.get("/images")
async def list_available_images():
    log("LoadBalancer", "list_available_images")
    return await get_lambda_images()



def run_https():
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=443,
        ssl_keyfile="/app/control_plane/server.key", 
        ssl_certfile="/app/control_plane/server.crt",
        log_level="DEBUG"
    )

if __name__=="__main__":
    run_https()