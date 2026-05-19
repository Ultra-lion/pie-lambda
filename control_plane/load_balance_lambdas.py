import httpx
from fastapi import FastAPI, Request, BackgroundTasks
from urllib.parse import urlparse, unquote
from control_plane_db import ControlPlaneDB
import asyncio
import uvicorn
import uuid


app = FastAPI()

control_plane_db = ControlPlaneDB()

waiting_room = {}
    

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
    request_id = str(uuid.uuid4())
    control_plane_db.create_lambda_request(request_id, lambda_func_name, request)
    if type == "RequestResponse":
        try:
            instance = control_plane_db.get_available_lambda_instance(lambda_func_name)
            if not instance:
                waiting_room[request_id] = asyncio.Event()
                control_plane_db.create_scaleup_request(request_id, lambda_func_name)
                try:
                    await asyncio.wait_for(waiting_room[request_id].wait(), timeout=30)
                except asyncio.TimeoutError:
                    control_plane_db.update_lambda_request(lambda_func_name, {"status": "timeout", "response": "Lambda is busy"})
                    return "Lambda is busy"
            
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=request.method,
                    url=f"http://{instance.ip_address}:{instance.port}",
                    headers=request.headers,
                    params=request.query_params,
                    content=await request.body(),
                )
            control_plane_db.update_lambda_request(lambda_func_name, {"status": "success", "response": response.content})
            return response.content
        finally:
            control_plane_db.mark_instance_as_available(instance.instance_id)
            waiting_room.pop(request_id, None)

    elif type == "Event":
        return 202
    else:
        raise ValueError("Invalid type")

async def get_lambda_images():
    return []

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_request(request: Request, path: str):
    lambda_func_name = extract_lambda_name(path)
    print("lambda_func_name", lambda_func_name)
    if not lambda_func_name:
       return await get_lambda_images()

    lowercase_headers = {k.lower(): v for k, v in request.headers.items()}
    invocation_type = lowercase_headers.get('x-amz-invocation-type')

    return await proxy_api_call(request, lambda_func_name, invocation_type)
    
if __name__=="__main__":
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=443,
        ssl_keyfile="/app/control_plane/certs/server.key", 
        ssl_certfile="/app/control_plane/certs/server.crt",
        log_level="DEBUG"
    )