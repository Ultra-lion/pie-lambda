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
import multiprocessing


from logger_utils import log



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
                self.writer.close()
                await self.writer.wait_closed()
                log("LoadBalancer", "ScalerClient.poke_scaler", request_id=request_id, status="poked")
        except Exception as e:
            log("LoadBalancer", "ScalerClient.poke_scaler", request_id=request_id, error=str(e))
            print(e)
        

control_plane_db = None

waiting_room = {}

scaler_client = None



background_tasks = set()

async def ipc_server():
    socket_path = "/tmp/lb.sock"
    if os.path.exists(socket_path):
        os.remove(socket_path)
    
    async def handle_poke_back(reader, writer):
        log("LoadBalancer", "ipc_server.handle_poke_back", status="received_connection")
        try:
            data = await reader.readuntil(b"\n")
            request_data = json.loads(data.decode())
            log("LoadBalancer", "ipc_server.handle_poke_back", payload=request_data)
            if request_data.get("type")=="ready":
                ready_ids = request_data.get("ready_ids")
                for ready_id in ready_ids:
                    if ready_id in waiting_room:
                        log("LoadBalancer", "ipc_server.handle_poke_back", action="release_waiting_room", request_id=ready_id)
                        waiting_room[ready_id].set()
                    else:
                        log("LoadBalancer", "ipc_server.handle_poke_back", action="release_stale_reservation", request_id=ready_id)
                        asyncio.create_task(control_plane_db.release_stale_reservations(ready_id))
                        

        except Exception as e:
            log("LoadBalancer", "ipc_server.handle_poke_back", error=str(e))
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
async def startup_event(app: FastAPI):
    log("LoadBalancer", "startup_event", status="starting")
    loop = asyncio.get_running_loop()
    ipc_task = loop.create_task(ipc_server())
    global scaler_client
    scaler_client = ScalerClient()

    await scaler_client.initialize()

    global control_plane_db
    control_plane_db = ControlPlaneDB()

    heartbeat_task = loop.create_task(start_heartbeat("LOAD_BALANCER"))

    ipc_task.add_done_callback(background_tasks.discard)
    heartbeat_task.add_done_callback(background_tasks.discard)

    log("LoadBalancer", "startup_event", status="ready")
    yield

    log("LoadBalancer", "startup_event", status="shutting_down")
    ipc_task.cancel()
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
        last_heartbeat = datetime.datetime.strptime(scaler_health['last_heartbeat'], "%Y-%m-%d %H:%M:%S").timestamp()
    else:
        last_heartbeat = 0
        
    log("LoadBalancer", "proxy_api_call", scaler_last_heartbeat=last_heartbeat, current_time=time.time())
    
    if not scaler_health or (time.time() - last_heartbeat > 20):
        log("LoadBalancer", "proxy_api_call", status="scaler_dead_503")
        return 503  # Service Unavailable, Scaler is dead
    
    request_id = str(uuid.uuid4())
    log("LoadBalancer", "proxy_api_call", request_id=request_id)
    
    await control_plane_db.create_lambda_request(request_id, lambda_func_name, request)
    
    if type == "RequestResponse":
        try:
            instance=None
            scaling_requested = False
            timeout_start = datetime.datetime.now()
            while not instance:
                if datetime.datetime.now() - timeout_start > datetime.timedelta(seconds=30):
                    log("LoadBalancer", "proxy_api_call", request_id=request_id, status="timeout_504")
                    del waiting_room[request_id]
                    return 504
                
                instance = await control_plane_db.get_available_lambda_instance(request_id, lambda_func_name)
                
                if not instance and not scaling_requested:
                    log("LoadBalancer", "proxy_api_call", request_id=request_id, status="scaling_required")
                    scaling_requested = True
                    waiting_room[request_id] = asyncio.Event()
                    asyncio.create_task(scaler_client.poke_scaler(json.dumps({"request_id":request_id})))
                    try:
                        await asyncio.wait_for(waiting_room[request_id].wait(), timeout=30)
                        log("LoadBalancer", "proxy_api_call", request_id=request_id, status="scaling_completed")
                        del waiting_room[request_id]
                        instance = await control_plane_db.get_available_lambda_instance(request_id, lambda_func_name)
                        break
                    except asyncio.TimeoutError:
                        log("LoadBalancer", "proxy_api_call", request_id=request_id, status="scaling_timeout")
                        pass
            
            log("LoadBalancer", "proxy_api_call", request_id=request_id, status="proxying_to_instance", instance_ip=instance['ip_address'], instance_port=instance['port'])
            
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=request.method,
                    url=f"http://{instance['ip_address']}:{instance['port']}",
                    headers=request.headers,
                    params=request.query_params,
                    content=await request.body(),
                )
            
            log("LoadBalancer", "proxy_api_call", request_id=request_id, status="response_received", response_status=response.status_code)
            await control_plane_db.update_lambda_request(request_id, {"status": "success", "response_data": response.text})
            return response.content
        finally:
            if instance:
                log("LoadBalancer", "proxy_api_call", request_id=request_id, status="marking_instance_available", instance_id=instance['container_id'])
                await control_plane_db.mark_instance_as_available(instance['container_id'])
            waiting_room.pop(request_id, None)

    elif type == "Event":
        log("LoadBalancer", "proxy_api_call", request_id=request_id, status="event_type_accepted_202")
        return 202
    else:
        log("LoadBalancer", "proxy_api_call", request_id=request_id, status="invalid_type_error")
        raise ValueError("Invalid type")

async def get_lambda_images():
    return []

@app.post("/{sdk_date}/functions/{function_identifier:path}/invocations")
async def proxy_request(request: Request, path: str, sdk_date: str):
    log("LoadBalancer", "proxy_request", path=path)
    lambda_func_name = extract_lambda_name(path)
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