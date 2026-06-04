
import asyncio
import docker
import os
import time 
from datetime import datetime, timedelta, timezone
import uuid
import httpx
import tarfile
import io


from control_plane_db import ControlPlaneDB
from logger_utils import log
from utils import get_local_ip, BASE_SUBSTR, BASE_NETWORK_BRIDGE


class LambdaScaler:
    def __init__(self, config={}, individual_lambda_scale_limit=5):
        self.docker_client = docker.from_env()
        # Excellence: Load global scaling limits from config if available
        self.config = config
        self.individual_lambda_scale_limit = config.get("global_scale_limit", individual_lambda_scale_limit)
        self.created_container_stuck_time_mins = config.get("created_container_stuck_time_mins", 2)
        self.lambda_default_region = config.get("lambda_default_region", "us-east-1")
        self.lambda_default_access_key_id = config.get("lambda_default_access_key_id", "test")
        self.lambda_default_secret_access_key = config.get("lambda_default_secret_access_key", "test")
        self.docker_sdk_check_interval_mins = config.get("docker_sdk_check_interval_mins", 0.5)
        self.control_plane_db = ControlPlaneDB()
        self.IPC_event = asyncio.Event()
        self.loop = None
        self.docker_sdk_check_time = None
             
    def get_lambda_image_name(self, lambda_func_name):
        image_name = f"{BASE_SUBSTR}-{lambda_func_name}:latest"
        log("Scaler", "get_lambda_image_name", lambda_name=lambda_func_name, image_name=image_name)
        return image_name
    
    def generate_container_name(self, lambda_func_name):
        # This should return a unique name for the new container
        container_id = uuid.uuid4()
        return f"lambda-{lambda_func_name}-{container_id}"

    def scale_up_lambda(self, lambda_func_name):
        container_id = self.generate_container_name(lambda_func_name)
        log("Scaler", "scale_up_lambda", lambda_name=lambda_func_name)
        provisioning_row_id_future = asyncio.run_coroutine_threadsafe(self.control_plane_db.create_provisioning_container(lambda_func_name, container_id), self.loop)
        provisioning_row_id = provisioning_row_id_future.result()
        log("Scaler", "scale_up_lambda", status="row_created", provisioning_id=provisioning_row_id)
        control_plane_ip = get_local_ip()
        container = self.docker_client.containers.create(
            image= self.get_lambda_image_name(lambda_func_name),
            name= container_id,
            detach=True,
            network=BASE_NETWORK_BRIDGE,
            dns=[control_plane_ip],
            environment={
                "AWS_LAMBDA_RUNTIME_API": f"{control_plane_ip}",
                "LAMBDA_FUNC_NAME": lambda_func_name,
                "AWS_DEFAULT_REGION": self.lambda_default_region,
                "AWS_ACCESS_KEY_ID": self.lambda_default_access_key_id,
                "AWS_SECRET_ACCESS_KEY": self.lambda_default_secret_access_key,
                "AWS_CA_BUNDLE": "/tmp/ca.crt",
                
            },
            extra_hosts={"host.docker.internal": "host-gateway"}
        )
        log("Scaler", "scale_up_lambda", status="container_started", container_id=container.id)
        
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode='w') as tar:
            with open('/etc/ssl/certs/ca.crt', 'rb') as f:
                info = tarfile.TarInfo(name='ca.crt')
                info.size = len(f.read())
                f.seek(0)
                tar.addfile(info, f)
        # 3. Push and Start
        container.put_archive('/tmp/', stream.getvalue()) # or /etc/custom-ssl/
        
        container.start()
        
        container_ip = None

        while not container_ip:
            container.reload()
            try:
                container_ip = container.attrs['NetworkSettings']["Networks"][BASE_NETWORK_BRIDGE]["IPAddress"]
            except Exception as e:
                log("Scaler", "scale_up_lambda", container_ip=container_ip)
            time.sleep(0.1)

        # Push registration to WorkerManager immediately to warm the cache
        try:
            with httpx.Client() as client:
                client.post(
                    f"http://{control_plane_ip}:80/register/container",
                    json={"ip_address": container_ip, "lambda_name": lambda_func_name},
                    timeout=1.0
                )
        except Exception as e:
            log("Scaler", "scale_up_lambda", status="registration_push_failed", error=str(e))

        future = asyncio.run_coroutine_threadsafe(
            self.control_plane_db.add_lambda_deployed_instances(
                lambda_func_name,
                container.id, 
                container_ip, 
                provisioning_row_id
                ),
                self.loop)
        future.result()



    def scale_down_lambda(self, lambda_func_name, container_id):
        log("Scaler", "scale_down_lambda", lambda_name=lambda_func_name, container_id=container_id)
        container_ip=None
        try:
            container_ip = self.docker_client.containers.get(container_id).attrs['NetworkSettings']["Networks"][BASE_NETWORK_BRIDGE]["IPAddress"]
        except Exception as e:
            log("Scaler", "scale_down_lambda", container_ip=container_ip)
        
        try:
            self.docker_client.containers.get(container_id).stop()
            log("Scaler", "scale_down_lambda", container_id=container_id, status="stopped")
        except Exception as e:
            log("Scaler", "scale_down_lambda", container_id=container_id, status="stop_failed", error=str(e))
            pass
        control_plane_ip = get_local_ip()
        if container_ip:
            try:
                with httpx.Client() as client:
                    client.post(
                        f"http://{control_plane_ip}:80/unregister/container",
                        json={"ip_address": container_ip},
                        timeout=1.0
                    )
            except Exception as e:
                log("Scaler", "scale_down_lambda", status="unregistration_push_failed", error=str(e))

        try:
            self.docker_client.containers.get(container_id).remove(force=True)
            log("Scaler", "scale_down_lambda", container_id=container_id, status="removed")
        except Exception as e:
            log("Scaler", "scale_down_lambda", container_id=container_id, status="remove_failed", error=str(e))
            pass

    def provision_container(self, lambda_func_name):        
        self.scale_up_lambda(lambda_func_name)
    

    async def scaler_thread_loop(self):
        log("Scaler", "scaler_thread_loop", status="checking_requests")
        scale_up_requests = await self.control_plane_db.calculate_scaleup_requests()
        if scale_up_requests:
            log("Scaler", "scaler_thread_loop", request_count=len(scale_up_requests))
            scale_thread_tasks = []
            for scale_up_request in scale_up_requests:

                lambda_func_name = scale_up_request.get("lambda_name") # Fixed key from lambda_func_name to lambda_name
                required_containers = scale_up_request.get("required_containers")
                
                log("Scaler", "scaler_thread_loop", lambda_name=lambda_func_name, required=required_containers)
                
                for _ in range(required_containers):
                    scale_thread_tasks.append(asyncio.to_thread(self.provision_container, lambda_func_name))
            
            await asyncio.gather(*scale_thread_tasks)
            log("Scaler", "scaler_thread_loop", status="provisioning_batch_complete")
       
    
    def get_docker_containers(self):
        return self.docker_client.containers.list()

    def get_destroy_dead_pie_lambda_dockers(self):
        unhealthy_statuses = ['exited', 'dead']
        all_containers = self.docker_client.containers.list(all=True)
        reaped = []

        for container in all_containers:
            if "control-plane" in container.name:
                continue

            if any(BASE_SUBSTR.lower() in tag.lower() for tag in (container.image.tags or [])):
                if container.status in unhealthy_statuses:
                    try:
                        container.remove(force=True)
                        reaped.append(container)
                    except Exception as e:
                        log("Scaler", "reaper.safety_check", error=str(e))
        
        return reaped

    async def delete_exited_pie_lambda_containers(self):
        stopped_pie_lambda_containers = await asyncio.to_thread(self.get_destroy_dead_pie_lambda_dockers)
        container_ids_to_delete = [container.id for container in stopped_pie_lambda_containers]
        await self.control_plane_db.remove_destroyed_containers(container_ids_to_delete)

    async def check_docker_container_sdk(self):
        if self.docker_sdk_check_time is None or self.docker_sdk_check_time < datetime.now() - timedelta(minutes=self.docker_sdk_check_interval_mins):
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
            
    async def ipc_server(self):
        socket_path = "/tmp/scaler.sock"
        if os.path.exists(socket_path):
            os.remove(socket_path)
        async def handle_poke(reader, writer):
            try:
                while True:
                    data = await reader.read(1024)
                    if not data:
                        break
                    msg = data.decode().strip()
                    log("Scaler", "ipc_server.handle_poke", msg=msg)                    
                    self.IPC_event.set()

                    writer.write(b"OK")
                    await writer.drain()
                    
            except asyncio.CancelledError:
                pass
            finally:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    log("Scaler", "ipc_server.handle_poke", error=str(e))
        

        server = await asyncio.start_unix_server(handle_poke, socket_path)
        async with server:
            await server.serve_forever()
    
    async def delete_stuck_requests(self):
        log("SCALER.RequestHandler","delete_stuck_requests")
        await self.control_plane_db.delete_stuck_requests()
       
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
                await asyncio.gather(
                    self.reaper_thread_loop(), 
                    self.check_docker_container_sdk(),
                    self.delete_exited_pie_lambda_containers(),
                    self.delete_stuck_requests(),
                    )
            except Exception as e:
                log("Scaler", "scaler_main_process", error=str(e))
                print(f"Error in main process: {e}")

    async def start_heartbeat(self, component_name):
        db = ControlPlaneDB()
        pid = os.getpid()
        
        while True:
            await db.update_health_stats(component_name, pid)
            await asyncio.sleep(5)

    
async def main():
    # Excellence: Load the config file on startup
    import json
    config_path = os.getenv("CONFIG_PATH", "config.json")
    config = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except Exception as e:
            print(f"Error loading config in Scaler: {e}")
            
    lambda_scaler = LambdaScaler(config=config)
    await asyncio.gather(
        lambda_scaler.scaler_main_process(), 
        lambda_scaler.ipc_server(),
        lambda_scaler.start_heartbeat("SCALER")
    )
            
                
            



if __name__ == "__main__":
    asyncio.run(main())