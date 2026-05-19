
import docker
import asyncio
import threading
import time

from control_plane_db import ControlPlaneDB


class LambdaScaler:
    def __init__(self, individual_lambda_scale_limit=5):
        self.docker_client = docker.from_env()
        self.individual_lambda_scale_limit = individual_lambda_scale_limit
        self.control_plane_db = ControlPlaneDB()
        
    def get_lambda_image_name(self, lambda_func_name):
        pass
    
    def get_existing_lambda_containers(self, lambda_func_name):
        pass

    def scale_up_lambda(self, lambda_func_name, request_id):
        container = self.docker_client.containers.run(
            image= self.get_lambda_image_name(lambda_func_name),
            name= self.get_existing_lambda_containers(lambda_func_name),
            detach=True,
            network="lambda_bridge",
            dns=["pie-lambda-control-plane"]
        )

        self.control_plane_db.add_lambda_deployed_instances(container.id, container.attrs['NetworkSettings']['IPAddress'], container.attrs['NetworkSettings']['Ports']['80/tcp'][0]['HostPort'])


    def scale_down_lambda(self, lambda_func_name, container_id):
        self.docker_client.containers.get(container_id).stop()
        self.docker_client.containers.get(container_id).remove()
    
    def provision_container(self, lambda_func_name):        
        self.scale_up_lambda(lambda_func_name)
    

    async def scaler_thread_loop(self):
       
        scale_up_requests = await self.control_plane_db.calculate_scaleup_requests()
        scale_thread_tasks = []
        for scale_up_request in scale_up_requests:
            lambda_func_name = scale_up_request.get(lambda_func_name)
            required_containers = scale_up_request.get(required_containers)
            deployed_containers = await self.control_plane_db.get_available_lambda_instance(lambda_func_name)
            for _ in range(required_containers):
                if len(deployed_containers) < self.individual_lambda_scale_limit:
                    scale_thread_tasks.append(asyncio.to_thread(self.provision_container(lambda_func_name)))
        await asyncio.gather(*scale_thread_tasks)
    
    async def reaper_thread_loop(self):
        containers_to_destroy = await self.control_plane_db.get_containers_to_destroy()# this will mark these containers unavailable
        reaper_thread_tasks = []
        for container in containers_to_destroy:
            reaper_thread_tasks.append(asyncio.to_thread(self.scale_down_lambda(container.lambda_name, container.container_id)))
        await asyncio.gather(*reaper_thread_tasks)
        await self.control_plane_db.remove_destroyed_containers([container.container_id for container in containers_to_destroy])# this will delete the rows for these containers


       
    async def main_process(self):
        
        while True:
            try:
                await asyncio.gather(self.scaler_thread_loop(), self.reaper_thread_loop())
            except Exception as e:
                print(f"Error in main process: {e}")
            await asyncio.sleep(1)
                
            



if __name__ == "__main__":
    lambda_scaler = LambdaScaler()
    asyncio.run(lambda_scaler.main_process())