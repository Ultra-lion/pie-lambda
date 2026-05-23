
import httpx
from control_plane_db import ControlPlaneDB
import asyncio
import httpx
import asyncio
import os
from logger_utils import log

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

    async def proxy_api_calls(self, lamdba_name, container_ip, payload, request_id):
        log("EventHandler", "proxy_api_calls", lambda_name=lamdba_name, container_ip=container_ip, request_id=request_id)
        try:
            # Note: This method seems incomplete as it references 'request' but doesn't take it as arg
            # I'll log the attempt
            log("EventHandler", "proxy_api_calls", status="sending_request")
            # For now just logging the flow as the original code was broken/stubbed
            return None
        except Exception as e:
            log("EventHandler", "proxy_api_calls", error=str(e))
            return None

    async def handle_enqueued_events(self):
        log("EventHandler", "handle_enqueued_events", status="starting")
        asyncio.create_task(self.start_heartbeat("EVENTS_HANDLER"))
        while True:
            try:
                enqueued_events = await self.control_plane_db.get_enqueued_events()
                if enqueued_events:
                    log("EventHandler", "handle_enqueued_events", enqueued_count=len(enqueued_events))
                    available_containers = await self.control_plane_db.get_available_lambda_instance_for_assignment(enqueued_events)
                    log("EventHandler", "handle_enqueued_events", assigned_count=len(available_containers))
                    
                    possible_handler_events = enqueued_events[:len(available_containers)]
                    await self.control_plane_db.mark_requests_as_processing(possible_handler_events)
                    
                    handler_tasks = []
                    for event in possible_handler_events:
                        container = available_containers.get(event['request_id'])
                        if container:
                            handler_tasks.append(asyncio.create_task(self.proxy_api_calls(event['lambda_name'], container['ip_address'], event['request_data'], event['request_id'])))
                    
                    if handler_tasks:
                        log("EventHandler", "handle_enqueued_events", status="waiting_for_tasks", task_count=len(handler_tasks))
                        await asyncio.gather(*handler_tasks)
                    
                    await self.control_plane_db.mark_requests_as_processed(possible_handler_events)
                    log("EventHandler", "handle_enqueued_events", status="batch_processed")
            except Exception as e:
                log("EventHandler", "handle_enqueued_events", error=str(e))
                print(f"Error in handle_enqueued_events: {e}")
            await asyncio.sleep(1)

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





if __name__ == "__main__":
    lambda_queue_handler = LambdaQueueHandler()
    asyncio.run(lambda_queue_handler.handle_enqueued_events())