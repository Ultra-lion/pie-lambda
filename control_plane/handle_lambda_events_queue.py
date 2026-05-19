
import httpx
from control_plane_db import ControlPlaneDB
import asyncio
import httpx
import asyncio

class LambdaQueueHandler:
    def __init__(self):
        self.control_plane_db = ControlPlaneDB()
        self.limit = httpx.Limits(max_connections=500, max_keepalive_connections=100)
        self.https_client = None
    
    async def __aenter__(self):
        self.https_client = httpx.AsyncClient(limits=self.limit, timeout=30)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.https_client.close()

    async def proxy_api_calls(self, lamdba_name, container_ip, payload, request):
        try:
            response = await self.https_client.request(
                        method=request.method,
                        url=f"http://{container_ip}:80",
                        headers=request.headers,
                        params=request.query_params,
                        content=await request.body(),
                    )
            return response
        except Exception as e:
            return None

    async def handle_enqueued_events(self):
        while True:
            try:
                enqueued_events = await self.control_plane_db.get_enqueued_events()
                available_containers = await self.control_plane_db.get_available_lambda_instance_for_assignment(enqueued_events)# This will return the available containers and do atomic checkout to mark them unavailable considering the no of events that needs processing up to a limit
                possible_handler_events = enqueued_events[:len(available_containers)]
                await self.control_plane_db.mark_requests_as_processing(possible_handler_events)
                handler_tasks = []
                for event in enqueued_events:
                    handler_tasks.append(asyncio.create_task(self.proxy_api_calls(event.lambda_name, available_containers[event.request_id].ip_address, event.payload, event.request_id)))
                await asyncio.gather(*handler_tasks)
                await self.control_plane_db.mark_requests_as_processed(possible_handler_events)
            except Exception as e:
                print(f"Error in handle_enqueued_events: {e}")
            await asyncio.sleep(1)



if __name__ == "__main__":
    lambda_queue_handler = LambdaQueueHandler()
    asyncio.run(lambda_queue_handler.handle_enqueued_events())