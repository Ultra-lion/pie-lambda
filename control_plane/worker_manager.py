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

available_workers = {}  # { lambda_name: asyncio.Queue[tuple[str, asyncio.Event, Request|None]] }
worker_events = {}      # { ip: asyncio.Event }
worker_payloads = {}    # { ip: payload }
response_events = {}    # { request_id: asyncio.Event }
registered_lambdas = {} # { ip: lambda_name }
lambdas_pending_registration = {} # { ip: asyncio.Event }
workers_in_queue = set() # Track IPs currently waiting in available_workers queues

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
        
async def memory_garbage_collector():
    """Periodically reconciles memory state with DB to prevent 'Ghost Worker' drift."""
    while True:
        await asyncio.sleep(60) # Run every minute
        try:
            containers = await control_plane_db.get_all_containers()
            live_ips = {c['ip_address'] for c in containers if c['ip_address']}
            
            # Remove registered lambdas that are no longer in the DB
            stale_ips = set(registered_lambdas.keys()) - live_ips
            for ip in stale_ips:
                log("WorkerManager", "memory_gc", status="cleaning_stale_ip", ip=ip)
                registered_lambdas.pop(ip, None)
                worker_events.pop(ip, None)
                worker_payloads.pop(ip, None)
                workers_in_queue.discard(ip)
        except Exception as e:
            log("WorkerManager", "memory_gc", status="error", error=str(e))


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

    # Start memory GC
    loop.create_task(memory_garbage_collector())

    heartbeat_task = loop.create_task(start_heartbeat("WORKER_MANAGER"))
    heartbeat_task.add_done_callback(background_tasks.discard)

    log("LoadBalancer", "startup_event", status="ready")
    yield

    log("LoadBalancer", "startup_event", status="shutting_down")
    heartbeat_task.cancel()


app = FastAPI(lifespan=startup_event)




@app.post("/register/container")
async def register_container(request: Request):
    """
    Scaler-driven registration. 
    Populates the IP -> Lambda mapping cache to avoid DB hits during task loops.
    """
    data = await request.json()
    ip = data.get("ip_address")
    lambda_name = data.get("lambda_name")

    if ip and lambda_name:
        registered_lambdas[ip] = (lambda_name, False)
        log("WorkerManager", "register_container", status="cached", ip=ip, lambda_name=lambda_name)
        
        # Wake up any worker waiting in the identification window
        if ip in lambdas_pending_registration:
            event = lambdas_pending_registration.pop(ip)
            if event:
                event.set()
                log("WorkerManager", "register_container", status="signaled_pending_worker", ip=ip)
    else:
        log("WorkerManager", "register_container", status="invalid_payload", data=data)
    
    return {"status": "accepted"}



@app.post("/unregister/container")
async def unregister_container(request: Request):
    """
    reaper-driven unregistration. 
    removes the IP -> Lambda mapping cache to avoid DB hits during task loops.
    """
    data = await request.json()
    ip = data.get("ip_address")

    if ip:
        registered_lambdas.pop(ip)
        workers_in_queue.discard(ip)
        log("WorkerManager", "unregister_container", status="removed", ip=ip)
    else:
        log("WorkerManager", "unregister_container", status="invalid_payload", data=data)
    
    return {"status": "accepted"}

async def _try_register_worker(ip: str, request: Request = None, force_db: bool = False):
    """Internal helper to verify identity via DB and add to available queue."""
    if ip in workers_in_queue:
        return

    # Total time to attempt registration before giving up for this call
    MAX_REGISTRATION_ATTEMPTS_TIME = 30.0 # seconds
    RETRY_INTERVAL = 0.5 # seconds
    
    start_time = datetime.datetime.now(timezone.utc)
    
    # Flag to track if we created the pending registration event in this call
    created_pending_event = False
    if ip not in lambdas_pending_registration:
        lambdas_pending_registration[ip] = asyncio.Event()
        created_pending_event = True

    try:
        while (datetime.datetime.now(timezone.utc) - start_time).total_seconds() < MAX_REGISTRATION_ATTEMPTS_TIME:
            # LIVENESS CHECK: If the worker disconnected while we are trying to identify it, abort immediately.
            if request and await request.is_disconnected():
                log("WorkerManager", "internal_register", status="aborted_worker_disconnected", ip=ip)
                return
            
            lambda_name, is_db_verified = registered_lambdas.get(ip)

            # 1. Fast Path: Try memory cache
            if lambda_name and is_db_verified:
                break # Found in cache, exit loop

            # 2. Fallback to DB (if cache miss or forced)
            container = await control_plane_db.get_lambda_container_by_ip(ip)
            if container:
                lambda_name = container['lambda_name']
                registered_lambdas[ip] = (lambda_name,True)
                log("WorkerManager", "internal_register", status="found_in_db", ip=ip, lambda_name=lambda_name)
                break # Found in DB, exit loop

            # 3. Wait for Scaler signal (reactive)
            log("WorkerManager", "internal_register", status="waiting_for_scaler_signal", ip=ip, elapsed=(datetime.datetime.now(timezone.utc) - start_time).total_seconds())
            try:
                # Wait for the event, but respect the overall registration timeout
                remaining_timeout = MAX_REGISTRATION_ATTEMPTS_TIME - (datetime.datetime.now(timezone.utc) - start_time).total_seconds()
                if remaining_timeout <= 0:
                    break # No time left
                
                # Wait for a short interval or until the event is set
                await asyncio.wait_for(lambdas_pending_registration[ip].wait(), timeout=min(RETRY_INTERVAL, remaining_timeout))
                # If event was set, it means register_container was called and it would have popped it.
                # We should re-check cache/DB immediately.
                continue 
            except asyncio.TimeoutError:
                # Event not set within RETRY_INTERVAL, continue loop to re-check cache/DB
                pass
            
            await asyncio.sleep(RETRY_INTERVAL) # Small sleep before next attempt

        # If we exit the loop and lambda_name is still None, it means registration failed.
        if not lambda_name:
            log("WorkerManager", "internal_register", status="registration_aborted_after_retries", ip=ip)
            return

        if lambda_name:
            if lambda_name not in available_workers:
                available_workers[lambda_name] = asyncio.Queue()
            
            if ip not in worker_events:
                worker_events[ip] = asyncio.Event()
            else:
                worker_events[ip].clear()

            registered_lambdas[ip] = (lambda_name,True)
            workers_in_queue.add(ip)
            await available_workers[lambda_name].put((ip, worker_events[ip], request))
            await control_plane_db.mark_instance_as_available(ip) # Ensure DB reflects the ready state
            log("WorkerManager", "internal_register", ip=ip, lambda_name=lambda_name, status="successfully_registered")
            
    finally:
        if created_pending_event and ip in lambdas_pending_registration:
            lambdas_pending_registration.pop(ip, None)

@app.post("/proxy_request/{lambda_name}/{request_id}")
async def proxy_request(request: Request, request_id:str, lambda_name:str):
    global control_plane_db, scaler_client
    
    payload_base = await request.json()

    # 1. Wait for a worker
    if lambda_name not in available_workers:
        available_workers[lambda_name] = asyncio.Queue()
    
    start_time = datetime.datetime.now(timezone.utc)
    try:
        # Poke scaler to ensure we have workers starting
        await scaler_client.poke_scaler()
        
        worker_ip = None
        while True:
            elapsed = (datetime.datetime.now(timezone.utc) - start_time).total_seconds()
            timeout_val = max(0.1, 30.0 - elapsed)
            
            # Pop worker metadata from queue
            worker_ip, event, worker_request = await asyncio.wait_for(available_workers[lambda_name].get(), timeout=timeout_val)
            
            # LIVENESS CHECK: If the worker disconnected while in queue, skip it
            if worker_request and await worker_request.is_disconnected():
                log("WorkerManager", "proxy_request", status="worker_ghost_detected", ip=worker_ip)
                workers_in_queue.discard(worker_ip)
                worker_events.pop(worker_ip, None)
                continue
                
            # SSoT CHECK: Ensure DB still sees this worker as available
            success = await control_plane_db.mark_instance_as_busy(worker_ip, request_id)
            if not success:
                workers_in_queue.discard(worker_ip) # Cleanup if DB out of sync
                continue
            
            workers_in_queue.discard(worker_ip)

            # Verified worker found
            break
        
        payload = payload_base.copy()
        payload['request_id'] = request_id
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
    
    # Accuracy check: don't queue if already gone
    if await request.is_disconnected():
        log("WorkerManager", "runtime_invocation_next", status="disconnected_early", ip=lambda_ip)
        return None

    # Self-register if not already in queue
    if lambda_ip not in workers_in_queue:
        await _try_register_worker(lambda_ip, request)
    
    payload = None
    request_id = None
    try:
        # Ensure the event exists before waiting
        if lambda_ip not in worker_events:
             log("WorkerManager", "runtime_invocation_next", status="registration_failed_no_event", ip=lambda_ip)
             return None

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
        # Only clean up state if the worker disconnected or if a task was assigned 
        # (proxy_request discards it from workers_in_queue when a task is popped).
        # On a timeout, we keep it in the set so the next poll skips registration,
        # preserving its position in the FIFO queue and avoiding duplicates.
        if lambda_ip not in workers_in_queue or await request.is_disconnected():
            worker_events.pop(lambda_ip, None)
            worker_payloads.pop(lambda_ip, None)
            workers_in_queue.discard(lambda_ip)
            # Note: We do NOT pop registered_lambdas here as it is a cache managed by memory_gc.

@app.post("/{sdk_date}/runtime/invocation/{request_id}/response")
async def runtime_invocation_response(request_id: str, request: Request):
    lambda_ip = request.client.host
    resp_body = await request.json()
    
    await control_plane_db.update_lambda_request(request_id, {"status": "processed", "response_data": json.dumps(resp_body)})
    await control_plane_db.mark_instance_as_available(lambda_ip)
    
    # Wake up proxy_request
    if request_id in response_events:
        response_events[request_id].set()
    
    # Re-register worker directly
    await _try_register_worker(lambda_ip)
    return {"status": "accepted"}

@app.post("/{sdk_date}/runtime/init/error")
async def runtime_init_error(sdk_date: str, request: Request):
    lambda_ip = request.client.host
    error_payload = await request.json()
    print(error_payload)
    try:
        registered_lambdas.pop(lambda_ip)
    except KeyError:
        pass
    try:
        workers_in_queue.discard(lambda_ip)
    except KeyError:
        pass
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
    await control_plane_db.mark_instance_as_available(lambda_ip)

    # Wake up proxy_request
    if request_id in response_events:
        response_events[request_id].set()
    
    # Re-register worker directly
    await _try_register_worker(lambda_ip)
    
    return {"status": "accepted"}

async def sync_available_workers_from_db():
    """Initial startup sync to populate memory from DB state."""
    containers = await control_plane_db.get_all_containers()
    for c in containers:
        if c['status'] == 'available' and c['ip_address']:
            await _try_register_worker(c['ip_address'])







def run_http():
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=80,
        log_level="DEBUG"
    )

if __name__=="__main__":
    run_http()