import httpx
from fastapi import FastAPI, Request, BackgroundTasks
from urllib.parse import urlparse, unquote
from control_plane_db import ControlPlaneDB
import asyncio
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize the DB
    db = ControlPlaneDB()
    await db.get_conn() 
    yield


app = FastAPI(lifespan=lifespan)

control_plane_db = ControlPlaneDB()

async def get_next_server(lambda_func_name: str):
    lambda_server_ip = None
    while not lambda_server_ip:
        lambda_server_ip = control_plane_db.get_available_lambda_instance(lambda_func_name)
        if not lambda_server_ip:
            await asyncio.sleep(1)
    
    return lambda_server_ip
    

def extract_lambda_name(path: str):
    parsed_url = urlparse(path)

    path = parsed_url.path

    segments = path.strip('/').split('/')

    if len(segments) < 3 or segments[1] !='functions':
        raise ValueError("Non lambda Path sent to lambda load balancer")
    
    raw_identifier = segments[2]

    decoded_identifier = unquote(raw_identifier)

    if "function:" in decoded_identifier:
        decoded_identifier = decoded_identifier.split("function:")[-1]
    
    clean_name = decoded_identifier.split(":")[0]
    
    return clean_name


async def proxy_api_call(request: Request|dict = None, lambda_func_name: str = None, proxy_url: str = None, type:str = "RequestResponse"):
    control_plane_db.create_lambda_request(lambda_func_name, request)
    if type == "RequestResponse":
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=request.method,
                url=proxy_url,
                headers=request.headers,
            params=request.query_params,
            content=await request.body(),
        )
        control_plane_db.update_lambda_request(lambda_func_name, {"status": "success", "response": response.content})
        return response.content
    
    elif type == "Event":
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=request.method,
                url=proxy_url,
                headers=request.headers,
                params=request.query_params,
                content=await request.body(),
            )
        control_plane_db.update_lambda_request(lambda_func_name, {"status": "success", "response": response.content})
        return True
    
    else:
        raise ValueError("Invalid type")



@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_request(request: Request, path: str):
    lambda_func_name = extract_lambda_name(path)
    lambda_server_ip = await get_next_server(lambda_func_name)

    proxy_url = f"http://{lambda_server_ip}:8000/{path}"

    lowercase_headers = {k.lower(): v for k, v in request.headers.items()}
    invocation_type = lowercase_headers.get('x-amz-invocation-type')

    if invocation_type == "Event":
        BackgroundTasks.add_task(proxy_api_call, request, lambda_func_name, proxy_url, "Event")
    else:
        await proxy_api_call(request, lambda_func_name, proxy_url, "RequestResponse")
    
