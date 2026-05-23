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



class ScalerClient:
    def __init__(self):
        self.writer = None
        self.reader = None
        self.socket_conn = None
        self.lock = asyncio.Lock()

    async def initialize(self):
        while not os.path.exists("/tmp/scaler.sock"):
            await asyncio.sleep(0.1)
        self.reader, self.writer = await asyncio.open_unix_connection("/tmp/scaler.sock")


    async def poke_scaler(self, request_id):
        try:
            async with self.lock:
                self.writer.write(f"{request_id}".encode())
                await self.writer.drain()
                self.writer.close()
                await self.writer.wait_closed()
        except Exception as e:
            print(e)
        

control_plane_db = None

waiting_room = {}

scaler_client = None





async def ipc_server():
    socket_path = "/tmp/lb.sock"
    if os.path.exists(socket_path):
        os.remove(socket_path)
    
    async def handle_poke_back(reader, writer):
        try:
            data = await reader.readuntil(b"\n")
            request_data = json.loads(data.decode())
            if request_data.get("type")=="ready":
                ready_ids = request_data.get("ready_ids")
                for ready_id in ready_ids:
                    if ready_id in waiting_room:
                        waiting_room[ready_id].set()
                    else:
                        asyncio.create_task(control_plane_db.release_stale_reservations(ready_id))
                        

        except Exception as e:
            print(e)
        finally:
            await writer.drain()
            writer.close()
            await writer.wait_closed()


    server = await asyncio.start_unix_server(
        handle_poke_back,
        socket_path,
    )

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
async def startup_event():
    asyncio.create_task(ipc_server())
    global scaler_client
    scaler_client = ScalerClient()
    await scaler_client.initialize()

    global control_plane_db
    control_plane_db = ControlPlaneDB()

    asyncio.create_task(start_heartbeat("LOAD_BALANCER"))




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
    
    scaler_health = await control_plane_db.get_component_health("SCALER")
    if not scaler_health or (time.time() - scaler_health['last_heartbeat'] > 20):
        return 503  # Service Unavailable - Scaler is dead
    
    
    
    request_id = str(uuid.uuid4())
    control_plane_db.create_lambda_request(request_id, lambda_func_name, request)
    if type == "RequestResponse":
        try:
            instance=None
            scaling_requested = False
            timeout_start = time.time()
            while not instance:
                if time.time() - timeout_start > 30:
                    del waiting_room[request_id]
                    return 504
                instance = control_plane_db.get_available_lambda_instance(request_id, lambda_func_name)
                if not instance and not scaling_requested:
                    scaling_requested = True
                    waiting_room[request_id] = asyncio.Event()
                    asyncio.create_task(scaler_client.poke_scaler(json.dumps({"request_id":request_id})))
                    try:
                        await asyncio.wait_for(waiting_room[request_id].wait(), timeout=30)
                        del waiting_room[request_id]
                        instance = control_plane_db.get_available_lambda_instance(request_id, lambda_func_name)
                        break
                    except asyncio.TimeoutError:
                        pass
            
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=request.method,
                    url=f"http://{instance.ip_address}:{instance.port}",
                    headers=request.headers,
                    params=request.query_params,
                    content=await request.body(),
                )
            await control_plane_db.update_lambda_request(lambda_func_name, {"status": "success", "response": response.content})
            return response.content
        finally:
            await control_plane_db.mark_instance_as_available(instance.instance_id)
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
        ssl_keyfile="/app/control_plane/server.key", 
        ssl_certfile="/app/control_plane/server.crt",
        log_level="DEBUG"
    )