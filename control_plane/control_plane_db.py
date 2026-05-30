import asyncpg
from contextlib import asynccontextmanager
import asyncio
from logger_utils import log


DEFAULT_CONTAINERS_LIMIT = 1

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class ControlPlaneDB(metaclass=SingletonMeta):
    def __init__(self,individual_lambda_scale_limit=None):
        self.individual_lambda_scale_limit = individual_lambda_scale_limit or DEFAULT_CONTAINERS_LIMIT
        self.pool = None

    @asynccontextmanager
    async def db_connection(self):
        # log("ControlPlaneDB", "db_connection", status="opening") # Too noisy if left active
        if not self.pool:
            self.pool = await asyncpg.create_pool(
            min_size=1,
            max_size=5
        )
        try:
            async with self.pool.acquire() as conn:
                yield conn
        except Exception as e:
            log("ControlPlaneDB", "db_connection", status="error", error=str(e))
            try:
                self.pool = await asyncpg.create_pool(
                    min_size=1,
                    max_size=5
                )
                
            finally:
                raise e
                
            

            

    async def initialize_db(self):
        log("ControlPlaneDB", "initialize_db", status="starting")
        async with self.db_connection() as db:
            async with db.transaction():
                await db.execute("""
                CREATE TABLE IF NOT EXISTS containers (
                    container_id TEXT PRIMARY KEY,
                    lambda_name TEXT NOT NULL,
                    ip_address TEXT,
                    port INTEGER,
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
                    priority INTEGER ,
                    request_data TEXT,
                    response_data TEXT,
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

        log("ControlPlaneDB", "initialize_db", status="completed")
    


    async def count_deployed_lambda_instances(self):
        log("ControlPlaneDB", "count_deployed_lambda_instances")
        async with self.db_connection() as db:
            res = await db.fetch("SELECT COUNT(*) FROM containers GROUP BY lambda_name")
            log("ControlPlaneDB", "count_deployed_lambda_instances", result_count=len(res))
            return res


    async def count_deployed_lambda_instance(self, lambda_name):
        log("ControlPlaneDB", "count_deployed_lambda_instance", lambda_name=lambda_name)
        async with self.db_connection() as db:
            count = await db.fetchval("SELECT COUNT(*) FROM containers WHERE lambda_name = $1", lambda_name)
            count = count or 0
            log("ControlPlaneDB", "count_deployed_lambda_instance", lambda_name=lambda_name, count=count)
            return count
    
    async def create_provisioning_container(self, lambda_name, container_id):
        log("ControlPlaneDB", "create_provisioning_container", lambda_name=lambda_name)
        async with self.db_connection() as db:
            result = await db.fetchrow("INSERT INTO containers (lambda_name, container_id, ip_address, status) VALUES ($1, $2, $3, $4) RETURNING container_id", lambda_name, container_id, None, "provisioning") 
            log("ControlPlaneDB", "create_provisioning_container", lambda_name=lambda_name, container_id=result[0])
            return result[0]

    async def add_lambda_deployed_instances(self, lambda_name, container_id, ip_address, provisioning_row_id=None):
        log("ControlPlaneDB", "add_lambda_deployed_instances", lambda_name=lambda_name, container_id=container_id, ip=ip_address)
        status = "available"
        if provisioning_row_id:
            async with self.db_connection() as db:
                await db.execute("UPDATE containers SET container_id = $1, ip_address = $2, status = $3 WHERE container_id = $4", container_id, ip_address, status, provisioning_row_id) 
                log("ControlPlaneDB", "add_lambda_deployed_instances", status="updated_existing", provisioning_row_id=provisioning_row_id)
        else:
            async with self.db_connection() as db:
                await db.execute("INSERT INTO containers (lambda_name, container_id, ip_address, port, status) VALUES ($1, $2, $3, $4, $5)", lambda_name, container_id, ip_address, None, status) 
                log("ControlPlaneDB", "add_lambda_deployed_instances", status="inserted_new")

    async def remove_lambda_deployed_instances(self, container_ids):
        log("ControlPlaneDB", "remove_lambda_deployed_instances", container_ids=container_ids)
        async with self.db_connection() as db:
            await db.execute("DELETE FROM containers WHERE container_id = ANY($1::text[])", container_ids) 
            log("ControlPlaneDB", "remove_lambda_deployed_instances", status="deleted")
    
    
    async def get_lambda_deployed_instances(self, lambda_func_name, status):
        log("ControlPlaneDB", "get_lambda_deployed_instances", lambda_name=lambda_func_name, status=status)
        async with self.db_connection() as db:
            res = await db.fetch("SELECT * FROM containers WHERE lambda_name = $1 AND status = $2", lambda_func_name, status) 
            log("ControlPlaneDB", "get_lambda_deployed_instances", result_count=len(res))
            return res
    
    
    async def mark_instance_as_busy(self, ip_address, request_id=None):
        log("ControlPlaneDB", "mark_instance_as_busy", ip_address=ip_address, request_id=request_id)
        async with self.db_connection() as db:
            async with db.transaction():
                result = await db.fetchrow("UPDATE containers SET status = 'busy', last_used_at = CURRENT_TIMESTAMP WHERE ip_address = $1 AND status = 'available' RETURNING container_id", ip_address)
                if result is None:
                    log("ControlPlaneDB", "mark_instance_as_busy", ip_address=ip_address, status="failed_rowcount_0")
                    return False
                if request_id:
                    await db.execute("UPDATE requests SET status = 'busy', last_used_at = CURRENT_TIMESTAMP WHERE request_id = $1", request_id)
                log("ControlPlaneDB", "mark_instance_as_busy", ip_address=ip_address, status="success")
                return True

    async def mark_instance_as_available(self, ip_address):
        log("ControlPlaneDB", "mark_instance_as_available", ip_address=ip_address)
        async with self.db_connection() as db:
            await db.execute("UPDATE containers SET status = 'available', last_used_at = CURRENT_TIMESTAMP WHERE ip_address = $1", ip_address)
            log("ControlPlaneDB", "mark_instance_as_available", status="updated")

    async def get_containers_to_destroy(self):
        log("ControlPlaneDB", "get_containers_to_destroy", status="checking")
        async with self.db_connection() as db:
            rows = await db.fetch("""
            UPDATE containers SET status = 'destroying'
            WHERE container_id IN 
            (
            SELECT container_id FROM containers 
            WHERE 
                (status = 'available' AND COALESCE(last_used_at, created_at) < NOW() - INTERVAL '5 minutes')
            OR 
                (status = 'busy' AND COALESCE(last_used_at, created_at) < NOW() - INTERVAL '30 minutes')
            OR 
                (status = 'provisioning' AND COALESCE(last_used_at, created_at) < NOW() - INTERVAL '1 minutes')
            )
            RETURNING *;
            """)
            log("ControlPlaneDB", "get_containers_to_destroy", found_count=len(rows))
            return rows
            
    async def remove_destroyed_containers(self, container_ids):
        if not container_ids:
            return
        log("ControlPlaneDB", "remove_destroyed_containers", container_ids=container_ids)
        async with self.db_connection() as db:
            await db.execute("DELETE FROM containers WHERE container_id = ANY($1::text[])", container_ids) 
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
            await db.execute("INSERT INTO requests (request_id, lambda_name, event_type, priority, request_data, response_data, status) VALUES ($1, $2, $3, $4, $5, $6, $7)", request_id, lambda_func_name, event_type, priority, request_data, response_data, status)
            log("ControlPlaneDB", "create_lambda_request", status="created")

    async def update_lambda_request(self, request_id, payload):
        log("ControlPlaneDB", "update_lambda_request", request_id=request_id, payload=payload)
        status = payload.get("status")
        response_data = payload.get("response_data")
        async with self.db_connection() as db:
            await db.execute("UPDATE requests SET status = $1, response_data = $2 WHERE request_id = $3", status, response_data, request_id)
            log("ControlPlaneDB", "update_lambda_request", status="updated")

    async def delete_stuck_requests(self):
        async with self.db_connection() as db:
            await db.execute("""
                UPDATE requests 
                SET status = 'failed'
                WHERE request_id IN (
                    SELECT request_id FROM requests 
                    WHERE 
                    (status IN ('pending', 'in_progress') AND event_type='RequestResponse' AND created_at < NOW() - INTERVAL '5 minutes')
                    OR (status IN ('pending', 'in_progress') AND event_type='Event' AND created_at < NOW() - INTERVAL '120 minutes')
                    
                )
            """)

    async def get_all_containers(self):
        log("ControlPlaneDB", "get_all_containers")
        async with self.db_connection() as db:
            res = await db.fetch("SELECT * FROM containers")
            log("ControlPlaneDB", "get_all_containers", result_count=len(res))
            return res
    
    async def get_lambda_container_by_ip(self, ip):
        log("ControlPlaneDB", "get_lambda_container_by_ip", ip=ip)
        async with self.db_connection() as db:
            return await db.fetchrow("SELECT lambda_name FROM containers WHERE ip_address = $1", ip)


    async def calculate_scaleup_requests(self):
        log("ControlPlaneDB", "calculate_scaleup_requests", status="starting")
        async with self.db_connection() as db:
            async with db.transaction():
                pending_requests = await db.fetch("""
                SELECT lambda_name, COUNT(*) as required_containers
                FROM requests
                WHERE 
                (status = 'pending' AND event_type='RequestResponse' AND created_at > NOW() - INTERVAL '5 minutes')
                OR (status = 'pending' AND event_type='Event' AND created_at > NOW() - INTERVAL '120 minutes')
                GROUP BY lambda_name
                ORDER BY MAX(priority) DESC, MIN(created_at) ASC;
                """)
                containers_counts = await db.fetch("SELECT lambda_name, status, COUNT(*) as container_count FROM containers WHERE status IN ('available','provisioning', 'busy') GROUP BY lambda_name, status")
                
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
            return await db.fetchrow("SELECT * FROM control_plane_health WHERE component_name = $1", component_name)
    
    async def get_enqueued_events(self):
        log("ControlPlaneDB", "get_enqueued_events")
        async with self.db_connection() as db:
            res = await db.fetch(f"SELECT * FROM requests WHERE status = 'pending' AND event_type = 'Event' LIMIT {self.individual_lambda_scale_limit}")
            log("ControlPlaneDB", "get_enqueued_events", result_count=len(res))
            return res

    async def mark_requests_as_processing(self, requests):
        log("ControlPlaneDB", "mark_requests_as_processing", count=len(requests))
        async with self.db_connection() as db:
            await db.execute("UPDATE requests SET status = 'processing' WHERE request_id = ANY($1::text[])", requests)
            log("ControlPlaneDB", "mark_requests_as_processing", status="updated")
    
    async def mark_requests_as_processed(self, requests):
        log("ControlPlaneDB", "mark_requests_as_processed", count=len(requests))
        async with self.db_connection() as db:
            await db.execute("UPDATE requests SET status = 'processed' WHERE request_id = ANY($1::text[])", requests)
            log("ControlPlaneDB", "mark_requests_as_processed", status="updated")

    async def clear_control_plane_health_stats(self):
        async with self.db_connection() as db:
            await db.execute("DELETE FROM control_plane_health;")

    async def get_all_health_stats(self):
        async with self.db_connection() as db:
            rows = await db.fetch("SELECT * FROM control_plane_health")
            health_stats = {row['component_name']: row for row in rows}
            return health_stats

if __name__=="__main__":
    test_db = ControlPlaneDB()

    asyncio.run(test_db.initialize_db())