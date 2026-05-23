
import asyncio
import docker
import asyncio
import os
import json 
from datetime import datetime, timedelta

from control_plane_db import ControlPlaneDB



class LambdaScaler:
    def __init__(self, config, individual_lambda_scale_limit=5):
        self.docker_client = docker.from_env()
        self.individual_lambda_scale_limit = individual_lambda_scale_limit
        self.control_plane_db = ControlPlaneDB()
        self.IPC_event = asyncio.Event()
        self.sync_requests_queue = asyncio.Queue()
        self.poke_back_queue = asyncio.Queue()
        self.loop = None
        self.docker_sdk_check_time = None
        self.ca_path = config.get("ca_path")
        
    def get_lambda_image_name(self, lambda_func_name):
        # This should probably query your DB's lambda_images table
        return f"{lambda_func_name}:latest" 
    
    def generate_container_name(self, lambda_func_name):
        # This should return a unique name for the new container
        import uuid
        return f"lambda-{lambda_func_name}-{uuid.uuid4().hex[:8]}"

    def scale_up_lambda(self, lambda_func_name, request_id_to_reserve_for=None):
        provisioning_row_id_future = asyncio.run_coroutine_threadsafe(self.control_plane_db.create_provisioning_container(lambda_func_name, request_id_to_reserve_for), self.loop)
        provisioning_row_id = provisioning_row_id_future.result()
        container = self.docker_client.containers.run(
            image= self.get_lambda_image_name(lambda_func_name),
            name= self.generate_container_name(lambda_func_name),
            detach=True,
            network="lambda_bridge",
            volumes={
                self.ca_path: {'bind': '/etc/ssl/certs/ca.crt', 'mode': 'ro'},
            },
            dns=["pie-lambda-control-plane"]
        )

        future = asyncio.run_coroutine_threadsafe(self.control_plane_db.add_lambda_deployed_instances(container.id, container.attrs['NetworkSettings']['IPAddress'], container.attrs['NetworkSettings']['Ports']['80/tcp'][0]['HostPort'], request_id_to_reserve_for), self.loop)
        future.result()
        if request_id_to_reserve_for:
            self.loop.call_soon_threadsafe(self.poke_back_queue.put_nowait, request_id_to_reserve_for)

    def scale_down_lambda(self, lambda_func_name, container_id):
        try:
            self.docker_client.containers.get(container_id).stop()
        except:
            pass
        try:
            self.docker_client.containers.get(container_id).remove(force=True)
        except:
            pass

    def provision_container(self, lambda_func_name, request_id_to_reserve_for=None):        
        self.scale_up_lambda(lambda_func_name, request_id_to_reserve_for)
    

    async def scaler_thread_loop(self):
       
        scale_up_requests = await self.control_plane_db.calculate_scaleup_requests()
        if scale_up_requests:
            scale_thread_tasks = []
            for scale_up_request in scale_up_requests:
                request_id_to_reserve_for = None
                try:
                    request_id_to_reserve_for = self.sync_requests_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass

                lambda_func_name = scale_up_request.get("lambda_func_name")
                required_containers = scale_up_request.get("required_containers")
                deployed_containers = await self.control_plane_db.get_available_lambda_instance(lambda_func_name)
                for _ in range(required_containers):
                    if len(deployed_containers) < self.individual_lambda_scale_limit:
                        scale_thread_tasks.append(asyncio.to_thread(self.provision_container, lambda_func_name, request_id_to_reserve_for))
            await asyncio.gather(*scale_thread_tasks)
        
        
        ready_ids = []
        while not self.poke_back_queue.empty():
            ready_ids.append(self.poke_back_queue.get_nowait())
        if ready_ids:
            await self.poke_lb(ready_ids)
    
    def get_docker_containers(self):
        return self.docker_client.containers.list()

    async def check_docker_container_sdk(self):
        if self.docker_sdk_check_time is None or self.docker_sdk_check_time < datetime.now() - timedelta(seconds=30):
            living_dockers = await asyncio.to_thread(self.get_docker_containers)
            self.docker_sdk_check_time = datetime.now()
            container_ids_to_not_delete=[]
            for container in living_dockers:
                container_ids_to_not_delete.append(container.id)
            all_db_container_ids = await self.control_plane_db.get_all_containers()
            container_ids_to_delete = [container_id for container_id in all_db_container_ids if container_id not in container_ids_to_not_delete]
            if container_ids_to_delete:
                await self.control_plane_db.remove_destroyed_containers(container_ids_to_delete)   

    async def reaper_thread_loop(self):
        containers_to_destroy = await self.control_plane_db.get_containers_to_destroy()# this will mark these containers unavailable
        reaper_thread_tasks = []
        for container in containers_to_destroy:
            reaper_thread_tasks.append(asyncio.to_thread(self.scale_down_lambda(container.lambda_name, container.container_id)))
        await asyncio.gather(*reaper_thread_tasks)
        await self.control_plane_db.remove_destroyed_containers([container.container_id for container in containers_to_destroy])# this will delete the rows for these containers

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
            request_data = json.loads(data.decode())
            await self.sync_requests_queue.put(request_data)
            self.IPC_event.set()
            writer.write(b"OK")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle_poke, socket_path)
        async with server:
            await server.serve_forever()
       
    async def scaler_main_process(self):
        
        while True:
            if not self.loop:
                self.loop = asyncio.get_event_loop()
            try:
                try:
                    await asyncio.wait_for(self.IPC_event.wait(), timeout=1)
                    self.IPC_event.clear()
                except asyncio.TimeoutError:
                    pass

                await self.scaler_thread_loop(), 
                await asyncio.gather(self.reaper_thread_loop(), self.check_docker_container_sdk())
            except Exception as e:
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