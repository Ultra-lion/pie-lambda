
import httpx
from control_plane_db import ControlPlaneDB
import asyncio
import asyncio
import os
import json
from logger_utils import log

config = {}

try:
    with open("config.json", "r") as f:
        config = json.load(f)
except Exception:
    pass

LAMBDA_TIMEOUT = config.get("lambda_timeout_mins", 5)


class LambdaQueueHandler:
    def __init__(self):
        self.control_plane_db = ControlPlaneDB()
        self.active_tasks = set()

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
                    timeout=(60*LAMBDA_TIMEOUT+5),# plus 5 seconds to not overlap with worker_manager and cause race conditions
                )
            
            await self.control_plane_db.update_lambda_request(request_id, {"status": "processed", "response_data": response.text})
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            log("EventHandler", "proxy_api_calls", status="retrying", error=str(e))
            await self.control_plane_db.update_lambda_request(request_id, {"status": "pending", "response_data": f"System Busy: {str(e)}", "increment_retry": True})
        except Exception as e:
            log("EventHandler", "proxy_api_calls", error=str(e))
            await self.control_plane_db.update_lambda_request(request_id, {"status": "pending", "response_data": str(e), "increment_retry": True})


    async def handle_enqueued_events(self):
        log("EventHandler", "handle_enqueued_events", status="starting")
        heartbeat_task = asyncio.create_task(self.start_heartbeat("EVENTS_HANDLER"))
        self.active_tasks.add(heartbeat_task)
        heartbeat_task.add_done_callback(self.active_tasks.discard)
        while True:
            try:
                enqueued_events = await self.control_plane_db.get_enqueued_events()
                if enqueued_events:
                    log("EventHandler", "handle_enqueued_events", enqueued_count=len(enqueued_events))
                    requests = [event.get("request_id") for event in enqueued_events]
                    
                    
                    for event in enqueued_events:
                        task = asyncio.create_task(self.proxy_api_calls(event['lambda_name'], event['request_data'], event['request_id']))
                        self.active_tasks.add(task)
                        task.add_done_callback(self.active_tasks.discard)
                
                    
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