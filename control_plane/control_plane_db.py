import aiosqlite
from contextlib import asynccontextmanager
import asyncio
from logger_utils import log


DEFAULT_CONTAINERS_LIMIT = 10

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class ControlPlaneDB(metaclass=SingletonMeta):
    def __init__(self,individual_lambda_scale_limit=None):
        self.db = "pie_lambda.db"
        self.individual_lambda_scale_limit = individual_lambda_scale_limit or DEFAULT_CONTAINERS_LIMIT


    @asynccontextmanager
    async def db_connection(self):
        # log("ControlPlaneDB", "db_connection", status="opening") # Too noisy if left active
        async with aiosqlite.connect(self.db) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA busy_timeout=10000;")
            yield db
            

    async def initialize_db(self):
        log("ControlPlaneDB", "initialize_db", status="starting")
        async with self.db_connection() as db:

            await db.execute("""
            CREATE TABLE IF NOT EXISTS lambda_images (
                image_id TEXT PRIMARY KEY,
                lambda_name TEXT NOT NULL,
                image_name TEXT NOT NULL,
                image_tag TEXT NOT NULL,
                image_digest TEXT NOT NULL,
                image_size TEXT NOT NULL,
                image_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                image_last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            await db.execute("""
            CREATE TABLE IF NOT EXISTS containers (
                container_id TEXT PRIMARY KEY,
                lambda_name TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                reserved_for_request TEXT,
                port INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                
            """)

            await db.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                request_id TEXT PRIMARY KEY,
                lambda_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                priority INTEGER NOT NULL,
                request_data TEXT NOT NULL,
                response_data TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            await db.execute("""
            CREATE TABLE IF NOT EXISTS control_plane_health (
                component_name TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)


            await db.execute("CREATE INDEX IF NOT EXISTS idx_containers_status ON containers(lambda_name, status);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_containers_last_used ON containers(last_used_at, status);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_requests ON requests(status, priority);")

            await db.commit()
        log("ControlPlaneDB", "initialize_db", status="completed")
    


    async def count_deployed_lambda_instances(self):
        log("ControlPlaneDB", "count_deployed_lambda_instances")
        async with self.db_connection() as db:
            await db.execute("SELECT COUNT(*) FROM containers group by lambda_name")
            res = await db.fetchall()
            log("ControlPlaneDB", "count_deployed_lambda_instances", result_count=len(res))
            return res


    async def count_deployed_lambda_instance(self, lambda_name):
        log("ControlPlaneDB", "count_deployed_lambda_instance", lambda_name=lambda_name)
        async with self.db_connection() as db:
            res = await db.execute("SELECT COUNT(*) FROM containers where lambda_name = ?", (lambda_name,))
            row = await res.fetchone()
            count = row[0] if row else 0
            log("ControlPlaneDB", "count_deployed_lambda_instance", lambda_name=lambda_name, count=count)
            return count
    
    async def create_provisioning_container(self, lambda_name, request_id, container_id):
        log("ControlPlaneDB", "create_provisioning_container", lambda_name=lambda_name, request_id=request_id)
        async with self.db_connection() as db:
            res = await db.execute("INSERT INTO containers (lambda_name, container_id, ip_address, port, status, reserved_for_request) VALUES (?, ?, ?, ?, ?, ?) RETURNING container_id", (lambda_name, container_id, "test", "test", "test", None)) 
            result = await res.fetchone()
            await db.commit()
            log("ControlPlaneDB", "create_provisioning_container", lambda_name=lambda_name, container_id=result[0])
            return result[0]

    async def add_lambda_deployed_instances(self, lambda_name, container_id, ip_address, reserved_for_request=None, provisioning_row_id=None):
        log("ControlPlaneDB", "add_lambda_deployed_instances", lambda_name=lambda_name, container_id=container_id, ip=ip_address, reserved=reserved_for_request)
        status="available"
        if reserved_for_request:
            status="reserved"
        if provisioning_row_id:
            async with self.db_connection() as db:
                await db.execute("UPDATE containers SET container_id = ?, ip_address = ?, status = ?, reserved_for_request = ? WHERE container_id = ?", (container_id, ip_address, status, reserved_for_request, provisioning_row_id)) 
                await db.commit()
                log("ControlPlaneDB", "add_lambda_deployed_instances", status="updated_existing", provisioning_row_id=provisioning_row_id)
        else:
            async with self.db_connection() as db:
                await db.execute("INSERT INTO containers (lambda_name, container_id, ip_address, port, status, reserved_for_request) VALUES (?, ?, ?, ?, ?, ?)", (lambda_name, container_id, ip_address, status, reserved_for_request)) 
                await db.commit()
                log("ControlPlaneDB", "add_lambda_deployed_instances", status="inserted_new")

    async def remove_lambda_deployed_instances(self, container_ids):
        log("ControlPlaneDB", "remove_lambda_deployed_instances", container_ids=container_ids)
        async with self.db_connection() as db:
            placeholders = ','.join(['?'] * len(container_ids))
            await db.execute(f"DELETE FROM containers WHERE container_id in ({placeholders})", container_ids) 
            await db.commit()
            log("ControlPlaneDB", "remove_lambda_deployed_instances", status="deleted")
    
    
    async def get_lambda_deployed_instances(self, lambda_func_name, status):
        log("ControlPlaneDB", "get_lambda_deployed_instances", lambda_name=lambda_func_name, status=status)
        async with self.db_connection() as db:
            await db.execute("SELECT * FROM containers WHERE lambda_name = ? and status = ?", (lambda_func_name, status)) 
            res = await db.fetchall()
            log("ControlPlaneDB", "get_lambda_deployed_instances", result_count=len(res))
            return res
    
    
    async def mark_instance_as_busy(self, instance_id, request_id):
        log("ControlPlaneDB", "mark_instance_as_busy", instance_id=instance_id, request_id=request_id)
        async with self.db_connection() as db:
            result = await db.execute("UPDATE containers SET status = 'busy', last_used_at = CURRENT_TIMESTAMP, reserved_for_request = ? WHERE container_id = ? and status = 'available'", (request_id, instance_id))
            if result.rowcount == 0:
                log("ControlPlaneDB", "mark_instance_as_busy", instance_id=instance_id, status="failed_rowcount_0")
                return False
            await db.execute("UPDATE requests SET status = 'busy', last_used_at = CURRENT_TIMESTAMP WHERE request_id = ?", (request_id,))
            await db.commit()
            log("ControlPlaneDB", "mark_instance_as_busy", instance_id=instance_id, status="success")
            return True

    async def mark_instance_as_available(self, instance_id):
        log("ControlPlaneDB", "mark_instance_as_available", instance_id=instance_id)
        async with self.db_connection() as db:
            await db.execute("UPDATE containers SET status = 'available', last_used_at = CURRENT_TIMESTAMP WHERE container_id = ?", (instance_id,))
            await db.commit()
            log("ControlPlaneDB", "mark_instance_as_available", status="updated")

    async def get_containers_to_destroy(self):
        log("ControlPlaneDB", "get_containers_to_destroy", status="checking")
        async with self.db_connection() as db:
            res = await db.execute("""
            UPDATE containers SET status = 'destroying'
            where container_id in 
            (
            SELECT container_id FROM containers 
            WHERE 
                (status = 'available' AND COALESCE(last_used_at, created_at) < datetime('now', '-5 minutes'))
            OR 
                (status = 'busy' AND COALESCE(last_used_at, created_at) < datetime('now', '-30 minutes'))
            OR 
                (status = 'reserved' AND COALESCE(last_used_at, created_at) < datetime('now', '-1 minutes'))
            )
            RETURNING *;
            """)
            rows = await res.fetchall()
            await db.commit()
            log("ControlPlaneDB", "get_containers_to_destroy", found_count=len(rows))
            return rows
            
    async def remove_destroyed_containers(self, container_ids):
        if not container_ids:
            return
        log("ControlPlaneDB", "remove_destroyed_containers", container_ids=container_ids)
        async with self.db_connection() as db:
            placeholders = ','.join(['?'] * len(container_ids))
            await db.execute(f"DELETE FROM containers WHERE container_id IN ({placeholders})", container_ids) 
            await db.commit()
            log("ControlPlaneDB", "remove_destroyed_containers", status="deleted")

    async def create_lambda_request(self, request_id, lambda_func_name, request):
        log("ControlPlaneDB", "create_lambda_request", request_id=request_id, lambda_name=lambda_func_name)
        # Handle both dict and FastAPI Request objects
        if hasattr(request, "get"):
            event_type = request.get("event_type", "RequestResponse")
            priority = request.get("priority", 1)
            request_data = request.get("request_data", "")
            response_data = request.get("response_data", "")
            status = request.get("status", "pending")
        else:
            # Fallback for raw Request objects
            event_type = "RequestResponse"
            priority = 1
            request_data = ""
            response_data = ""
            status = "pending"

        async with self.db_connection() as db:
            await db.execute("INSERT INTO requests (request_id, lambda_name, event_type, priority, request_data, response_data, status) VALUES (?, ?, ?, ?, ?, ?, ?)", (request_id, lambda_func_name, event_type, priority, request_data, response_data, status))
            await db.commit()
            log("ControlPlaneDB", "create_lambda_request", status="created")

    async def update_lambda_request(self, request_id, payload):
        log("ControlPlaneDB", "update_lambda_request", request_id=request_id, payload=payload)
        status = payload.get("status")
        response_data = payload.get("response_data")
        async with self.db_connection() as db:
            await db.execute("UPDATE requests SET status = ?, response_data = ? WHERE request_id = ?", (status, response_data, request_id))
            await db.commit()
            log("ControlPlaneDB", "update_lambda_request", status="updated")

    async def delete_stuck_requests(self):
        async with self.db_connection() as db:
            await db.execute("""
                UPDATE requests 
                SET status = 'failed',
                WHERE request_id in (
                    SELECT request_id FROM requests 
                    WHERE 
                    (status in ('pending', 'in_progress') and event_type='RequestResponse' and created_at < datetime('now', '-5 minutes'))
                    OR (status in ('pending', 'in_progress') and event_type='Event' and created_at < datetime('now', '-120 minutes'))
                    
                )
            """)

    async def get_all_containers(self):
        log("ControlPlaneDB", "get_all_containers")
        async with self.db_connection() as db:
            cursor = await db.execute("SELECT * FROM containers")
            res = await cursor.fetchall()
            log("ControlPlaneDB", "get_all_containers", result_count=len(res))
            return res
    
    async def get_available_lambda_instance(self, request_id, lambda_func_name):
        log("ControlPlaneDB", "get_available_lambda_instance", request_id=request_id, lambda_name=lambda_func_name)
        async with self.db_connection() as db:
            res = await db.execute("""
            UPDATE containers 
            SET status = 'busy', 
                last_used_at = CURRENT_TIMESTAMP 
            WHERE container_id = (
                SELECT container_id FROM containers 
                WHERE (reserved_for_request = ? OR status = 'available')
                AND lambda_name = ?
                ORDER BY (reserved_for_request = ?) DESC, created_at ASC
                LIMIT 1
            )
            RETURNING *;
            """, (request_id, lambda_func_name, request_id))
            instance = await res.fetchone()
            if instance:
                log("ControlPlaneDB", "get_available_lambda_instance", status="found", instance_id=instance['container_id'])
            else:
                log("ControlPlaneDB", "get_available_lambda_instance", status="not_found")
            return instance
    
    async def get_available_lambda_instance_for_assignment(self, events):
        log("ControlPlaneDB", "get_available_lambda_instance_for_assignment", event_count=len(events))
        # 1. Group events by lambda_name
        events_by_lambda = {}
        for event in events:
            # Note: you used event.lambda_name but it's a dict/Row right? 
            # If it's a dictionary/row use event['lambda_name'], else event.lambda_name
            l_name = event['lambda_name'] 
            if l_name not in events_by_lambda:
                events_by_lambda[l_name] = []
            events_by_lambda[l_name].append(event)
            
        assigned_containers = {}

        async with self.db_connection() as db:
            # 2. Process each lambda cluster atomically
            for lambda_name, lambda_events in events_by_lambda.items():
                required_count = len(lambda_events)
                log("ControlPlaneDB", "get_available_lambda_instance_for_assignment", subtask="claiming", lambda_name=lambda_name, count=required_count)
                
                # claim up to 'required_count' containers
                res = await db.execute("""
                    UPDATE containers 
                    SET status = 'busy', 
                        last_used_at = CURRENT_TIMESTAMP 
                    WHERE container_id IN (
                        SELECT container_id FROM containers 
                        WHERE status = 'available' 
                        AND lambda_name = ?
                        ORDER BY created_at ASC
                        LIMIT ?
                    )
                    RETURNING *;
                """, (lambda_name, required_count))
                
                claimed_rows = await res.fetchall()
                log("ControlPlaneDB", "get_available_lambda_instance_for_assignment", subtask="claimed", lambda_name=lambda_name, count=len(claimed_rows))
                
                # 3. Pair them! (Up to the amount we successfully claimed)
                for i, container_row in enumerate(claimed_rows):
                    matched_event = lambda_events[i]
                    # Map the request_id to the container dict/row
                    assigned_containers[matched_event['request_id']] = container_row
                    
            await db.commit()
            
        # Returns a dict: { request_id: container_row }
        log("ControlPlaneDB", "get_available_lambda_instance_for_assignment", total_assigned=len(assigned_containers))
        return assigned_containers

    async def release_stale_reservations(self, request_id):
        log("ControlPlaneDB", "release_stale_reservations", request_id=request_id)
        async with self.db_connection() as db:
            await db.execute("""
            UPDATE containers 
            SET status = 'available', reserved_for_request = NULL 
            WHERE status = 'reserved' and  reserved_for_request = ?;
            """, (request_id,))
            await db.commit()
            log("ControlPlaneDB", "release_stale_reservations", status="released")

    async def calculate_scaleup_requests(self):
        log("ControlPlaneDB", "calculate_scaleup_requests", status="starting")
        async with self.db_connection() as db:
            res_requests = await db.execute("""
            SELECT lambda_name, COUNT(*) as required_containers
            FROM requests
            WHERE 
            (status = 'pending' and event_type='RequestResponse' and created_at > datetime('now', '-5 minutes'))
            OR (status = 'pending' and event_type='Event' and created_at > datetime('now', '-120 minutes'))
            GROUP BY lambda_name
            ORDER BY MAX(priority) DESC, MIN(created_at) ASC;
            """)
            pending_requests = await res_requests.fetchall()
            res_counts = await db.execute("SELECT lambda_name, status, COUNT(*) as container_count FROM containers WHERE status in ('available','provisioning', 'reserved', 'busy') GROUP BY lambda_name, status")
            containers_counts = await res_counts.fetchall()
            
            stats = {}
            for row in containers_counts:
                ln = row['lambda_name']
                st = row['status']
                cnt = row['container_count']
                if ln not in stats:
                    stats[ln] = {'available': 0, 'provisioning': 0, 'reserved': 0, 'busy': 0}
                stats[ln][st] = cnt
            results = []
            # 2. Iterate through pending requests and calculate shortfall
            for req in pending_requests:
                name = req['lambda_name']
                pending_cnt = req['required_containers']
                
                # Get current numbers for this lambda
                s = stats.get(name, {'available': 0, 'provisioning': 0, 'reserved': 0, 'busy': 0})
                total_existing = sum(s.values())
                
                # How many MORE do we need to satisfy the queue?
                needed = pending_cnt - s['provisioning']
                
                # How many MORE are we allowed to build?
                allowed = max(0, self.individual_lambda_scale_limit - total_existing)
                
                to_create = min(needed, allowed)
                
                if to_create > 0:
                    results.append({
                        "lambda_name": name,
                        "required_containers": to_create
                    })
            log("ControlPlaneDB", "calculate_scaleup_requests", result_count=len(results))
            return results
    
    async def get_component_health(self, component_name):
        # log("ControlPlaneDB", "get_component_health", component=component_name) # Too noisy
        async with self.db_connection() as db:
            result = await db.execute("SELECT * FROM control_plane_health WHERE component_name = ?", (component_name,))
            return await result.fetchone()
    
    async def get_enqueued_events(self):
        log("ControlPlaneDB", "get_enqueued_events")
        async with self.db_connection() as db:
            result = await db.execute(f"SELECT * FROM requests WHERE status = 'pending' and event_type = 'event' limit {self.individual_lambda_scale_limit}")
            res = await result.fetchall()
            log("ControlPlaneDB", "get_enqueued_events", result_count=len(res))
            return res

    async def mark_requests_as_processing(self, requests):
        log("ControlPlaneDB", "mark_requests_as_processing", count=len(requests))
        async with self.db_connection() as db:
            placeholders = ','.join(['?'] * len(requests))
            await db.execute(f"UPDATE requests SET status = 'processing' WHERE request_id IN ({placeholders})", requests)
            await db.commit()
            log("ControlPlaneDB", "mark_requests_as_processing", status="updated")
    
    async def mark_requests_as_processed(self, requests):
        log("ControlPlaneDB", "mark_requests_as_processed", count=len(requests))
        async with self.db_connection() as db:
            placeholders = ','.join(['?'] * len(requests))
            await db.execute(f"UPDATE requests SET status = 'processed' WHERE request_id IN ({placeholders})", requests)
            await db.commit()
            log("ControlPlaneDB", "mark_requests_as_processed", status="updated")

if __name__=="__main__":
    test_db = ControlPlaneDB()

    asyncio.run(test_db.initialize_db())