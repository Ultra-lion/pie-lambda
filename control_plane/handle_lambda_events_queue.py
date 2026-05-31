
import httpx
from control_plane_db import ControlPlaneDB
import asyncio
import asyncio
import os
import json
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

    async def proxy_api_calls(self, lamdba_name, payload, request_id):
        log("EventHandler", "proxy_api_calls", lambda_name=lamdba_name, request_id=request_id)
        try:
            headers={}
            query_params={}
            payload = json.loads(payload) if isinstance(payload, str) else payload
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method="POST",
                    url=f"http://127.0.0.1:80/proxy_request/{lamdba_name}/{request_id}",
                    headers=headers,
                    params=query_params,
                    json=payload,
                    timeout=60,
                )
            
            await self.control_plane_db.update_lambda_request(request_id, {"status": "success", "response_data": response.text})
            
        except Exception as e:
            log("EventHandler", "proxy_api_calls", error=str(e))
            await self.control_plane_db.update_lambda_request(request_id, {"status": "failure", "response_data": str(e)})


    async def handle_enqueued_events(self):
        log("EventHandler", "handle_enqueued_events", status="starting")
        asyncio.create_task(self.start_heartbeat("EVENTS_HANDLER"))
        while True:
            try:
                enqueued_events = await self.control_plane_db.get_enqueued_events()
                if enqueued_events:
                    log("EventHandler", "handle_enqueued_events", enqueued_count=len(enqueued_events))
                    requests = [event.get("request_id") for event in enqueued_events]
                    await self.control_plane_db.mark_requests_as_processing(requests)
                    
                    for event in enqueued_events:
                        asyncio.create_task(self.proxy_api_calls(event['lambda_name'], event['request_data'], event['request_id']))
                
                    
                    log("EventHandler", "handle_enqueued_events", status="batch_processed")
            except Exception as e:
                log("EventHandler", "handle_enqueued_events", error=str(e))
                print(f"Error in handle_enqueued_events: {e}")
                
            await asyncio.sleep(1)

    async def start_heartbeat(self, component_name):
        db = ControlPlaneDB()
        pid = os.getpid()
        
        while True:
            await db.update_health_stats(component_name, pid)
            await asyncio.sleep(5)





if __name__ == "__main__":
    lambda_queue_handler = LambdaQueueHandler()
    asyncio.run(lambda_queue_handler.handle_enqueued_events())