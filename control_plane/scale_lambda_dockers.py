
import asyncio
import docker
import asyncio
import os
import json 
from datetime import datetime, timedelta
import uuid

from control_plane_db import ControlPlaneDB
from logger_utils import log
from utils import get_local_ip, BASE_SUBSTR, BASE_NETWORK_BRIDGE


class LambdaScaler:
    def __init__(self, config={}, individual_lambda_scale_limit=5):
        self.docker_client = docker.from_env()
        self.individual_lambda_scale_limit = individual_lambda_scale_limit
        self.control_plane_db = ControlPlaneDB()
        self.IPC_event = asyncio.Event()
        self.sync_requests_queue = asyncio.Queue()
        self.poke_back_queue = asyncio.Queue()
        self.loop = None
        self.docker_sdk_check_time = None
        self.ca_path = config.get("ca_path")
        if not self.ca_path:
            self.ca_path = "/home/rohan/Desktop/FUN-Projects/pie-lambda/certs/"
            # raise Exception("Cert path not found in config")
        
    def get_lambda_image_name(self, lambda_func_name):
        image_name = f"{BASE_SUBSTR}-{lambda_func_name}:latest"
        log("Scaler", "get_lambda_image_name", lambda_name=lambda_func_name, image_name=image_name)
        return image_name
    
    def generate_container_name(self, lambda_func_name):
        # This should return a unique name for the new container
        container_id = uuid.uuid4()
        return f"lambda-{lambda_func_name}-{container_id}"

    def scale_up_lambda(self, lambda_func_name, request_id_to_reserve_for=None):
        container_id = self.generate_container_name(lambda_func_name)
        log("Scaler", "scale_up_lambda", lambda_name=lambda_func_name, reserved_for=request_id_to_reserve_for)
        provisioning_row_id_future = asyncio.run_coroutine_threadsafe(self.control_plane_db.create_provisioning_container(lambda_func_name, request_id_to_reserve_for, container_id), self.loop)
        provisioning_row_id = provisioning_row_id_future.result()
        log("Scaler", "scale_up_lambda", status="row_created", provisioning_id=provisioning_row_id)
        control_plane_ip = get_local_ip()
        container = self.docker_client.containers.run(
            image= self.get_lambda_image_name(lambda_func_name),
            name= container_id,
            detach=True,
            network=BASE_NETWORK_BRIDGE,
            volumes={
                self.ca_path: {'bind': '/etc/ssl/certs/ca.crt', 'mode': 'ro'},
            },
            dns=[control_plane_ip],
            environment={
                "AWS_LAMBDA_RUNTIME_API": f"{control_plane_ip}",
                "LAMBDA_FUNC_NAME": lambda_func_name
            }
        )
        log("Scaler", "scale_up_lambda", status="container_started", container_id=container.id)
        container.reload()
        future = asyncio.run_coroutine_threadsafe(self.control_plane_db.add_lambda_deployed_instances(container.id, container.attrs['NetworkSettings']['IPAddress'], container.attrs['NetworkSettings']['Ports']['80/tcp'][0]['HostPort'], request_id_to_reserve_for), self.loop)
        future.result()
        if request_id_to_reserve_for:
            self.loop.call_soon_threadsafe(self.poke_back_queue.put_nowait, request_id_to_reserve_for)

    def scale_down_lambda(self, lambda_func_name, container_id):
        log("Scaler", "scale_down_lambda", lambda_name=lambda_func_name, container_id=container_id)
        try:
            self.docker_client.containers.get(container_id).stop()
            log("Scaler", "scale_down_lambda", container_id=container_id, status="stopped")
        except Exception as e:
            log("Scaler", "scale_down_lambda", container_id=container_id, status="stop_failed", error=str(e))
            pass
        try:
            self.docker_client.containers.get(container_id).remove(force=True)
            log("Scaler", "scale_down_lambda", container_id=container_id, status="removed")
        except Exception as e:
            log("Scaler", "scale_down_lambda", container_id=container_id, status="remove_failed", error=str(e))
            pass

    def provision_container(self, lambda_func_name, request_id_to_reserve_for=None):        
        self.scale_up_lambda(lambda_func_name, request_id_to_reserve_for)
    

    async def scaler_thread_loop(self):
        log("Scaler", "scaler_thread_loop", status="checking_requests")
        scale_up_requests = await self.control_plane_db.calculate_scaleup_requests()
        if scale_up_requests:
            log("Scaler", "scaler_thread_loop", request_count=len(scale_up_requests))
            scale_thread_tasks = []
            for scale_up_request in scale_up_requests:
                request_id_to_reserve_for = None
                try:
                    request_id_to_reserve_for = self.sync_requests_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass

                lambda_func_name = scale_up_request.get("lambda_name") # Fixed key from lambda_func_name to lambda_name
                required_containers = scale_up_request.get("required_containers")
                
                log("Scaler", "scaler_thread_loop", lambda_name=lambda_func_name, required=required_containers, reserved_id=request_id_to_reserve_for)
                
                for _ in range(required_containers):
                    scale_thread_tasks.append(asyncio.to_thread(self.provision_container, lambda_func_name, request_id_to_reserve_for))
            
            await asyncio.gather(*scale_thread_tasks)
            log("Scaler", "scaler_thread_loop", status="provisioning_batch_complete")
        
        
        ready_ids = []
        while not self.poke_back_queue.empty():
            ready_ids.append(self.poke_back_queue.get_nowait())
        if ready_ids:
            log("Scaler", "scaler_thread_loop", status="poking_lb", ready_ids=ready_ids)
            await self.poke_lb(ready_ids)
    
    def get_docker_containers(self):
        return self.docker_client.containers.list()

    async def check_docker_container_sdk(self):
        if self.docker_sdk_check_time is None or self.docker_sdk_check_time < datetime.now() - timedelta(seconds=30):
            log("Scaler", "check_docker_container_sdk", status="starting_sync")
            living_dockers = await asyncio.to_thread(self.get_docker_containers)
            self.docker_sdk_check_time = datetime.now()
            container_ids_to_not_delete=[]
            for container in living_dockers:
                container_ids_to_not_delete.append(container.id)
            
            all_db_containers = await self.control_plane_db.get_all_containers()
            db_container_ids = [c['container_id'] for c in all_db_containers]
            
            container_ids_to_delete = [cid for cid in db_container_ids if cid not in container_ids_to_not_delete]
            
            if container_ids_to_delete:
                log("Scaler", "check_docker_container_sdk", status="found_ghost_containers", count=len(container_ids_to_delete))
                await self.control_plane_db.remove_destroyed_containers(container_ids_to_delete)
            else:
                log("Scaler", "check_docker_container_sdk", status="db_in_sync")

    async def reaper_thread_loop(self):
        log("Scaler", "reaper_thread_loop", status="checking_idle_containers")
        containers_to_destroy = await self.control_plane_db.get_containers_to_destroy()
        if containers_to_destroy:
            log("Scaler", "reaper_thread_loop", status="found_containers_to_reap", count=len(containers_to_destroy))
            reaper_thread_tasks = []
            for container in containers_to_destroy:
                log("Scaler", "reaper_thread_loop", action="queuing_destruction", container_id=container['container_id'], lambda_name=container['lambda_name'])
                reaper_thread_tasks.append(asyncio.to_thread(self.scale_down_lambda, container['lambda_name'], container['container_id']))
            
            await asyncio.gather(*reaper_thread_tasks)
            
            destroyed_ids = [c['container_id'] for c in containers_to_destroy]
            await self.control_plane_db.remove_destroyed_containers(destroyed_ids)
            log("Scaler", "reaper_thread_loop", status="reaping_complete", count=len(destroyed_ids))
        else:
            log("Scaler", "reaper_thread_loop", status="no_containers_to_reap")

    async def poke_lb(self, ready_ids):
        if not ready_ids:
            return

        lb_socket_path = "/tmp/lb.sock"
        try:
            reader, writer = await asyncio.open_unix_connection(lb_socket_path)
            payload = json.dumps({"type":"ready", "ready_ids": ready_ids})
            writer.write(payload.encode()+b"\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"Error poking LB: {e}")
            
    async def ipc_server(self):
        socket_path = "/tmp/scaler.sock"
        if os.path.exists(socket_path):
            os.remove(socket_path)
        async def handle_poke(reader, writer):
            data = await reader.read(1024)
            log("Scaler", "ipc_server.handle_poke", status="received_data")
            if data:
                try:
                    request_data = json.loads(data.decode())
                    log("Scaler", "ipc_server.handle_poke", payload=request_data)
                    await self.sync_requests_queue.put(request_data.get("request_id"))
                    self.IPC_event.set()
                except Exception as e:
                    log("Scaler", "ipc_server.handle_poke", error=str(e))
                    print(f"Error parsing request: {data}")
            writer.write(b"OK")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle_poke, socket_path)
        async with server:
            await server.serve_forever()
       
    async def scaler_main_process(self):
        log("Scaler", "scaler_main_process", status="starting")
        while True:
            if not self.loop:
                self.loop = asyncio.get_event_loop()
            try:
                try:
                    await asyncio.wait_for(self.IPC_event.wait(), timeout=1)
                    log("Scaler", "scaler_main_process", status="triggered_by_ipc")
                    self.IPC_event.clear()
                except asyncio.TimeoutError:
                    # log("Scaler", "scaler_main_process", status="periodic_check") # Too noisy
                    pass

                await self.scaler_thread_loop()
                await asyncio.gather(self.reaper_thread_loop(), self.check_docker_container_sdk())
            except Exception as e:
                log("Scaler", "scaler_main_process", error=str(e))
                print(f"Error in main process: {e}")

    async def start_heartbeat(self, component_name):
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

    
async def main():
    lambda_scaler = LambdaScaler()
    await asyncio.gather(
        lambda_scaler.scaler_main_process(), 
        lambda_scaler.ipc_server(),
        lambda_scaler.start_heartbeat("SCALER")
    )
            
                
            



if __name__ == "__main__":
    asyncio.run(main())