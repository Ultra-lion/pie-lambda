import psycopg # Use psycopg v3
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from contextlib import asynccontextmanager
import asyncio
from logger_utils import log
import json
import os
from fastapi import HTTPException

DEFAULT_CONTAINERS_LIMIT = 1

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class ControlPlaneDB(metaclass=SingletonMeta):
    def __init__(self):
        self.pool = None
        config_path = os.getenv("CONFIG_PATH", "config.json")
        config = {}
        self.individual_lambda_scale_limit=5 
        self.rr_stuck_time=5
        self.event_stuck_time=15
        self.retry_request_count=3
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    self.individual_lambda_scale_limit = config.get("global_scale_limit", 5)
                    self.rr_stuck_time = config.get("rr_stuck_time", 5)
                    self.event_stuck_time = config.get("event_stuck_time", 15)
                    self.retry_event_count = config.get("retry_event_count", 3)
            except Exception:
                pass
        self.db_conn_timeout = config.get("db_conn_timeout", 30)
        self.available_container_scale_down_time = config.get("available_container_scale_down_time", 5)
        self.busy_container_scale_down_time = config.get("busy_container_scale_down_time", 30)
        self.provisioning_container_scale_down_time = config.get("provisioning_container_scale_down_time", 1)
        lambda_funcs_to_deploy = config.get("lambda_funcs_to_deploy",{})
        self.valid_lambda_names = [lambda_func["func_name"] for lambda_func in lambda_funcs_to_deploy.values() if lambda_func.get("func_name")]
    
    async def create_pool(self):
            # 1. Back to clean string
            conn_str = "host=127.0.0.1 port=6957 user=postgres dbname=postgres sslmode=disable"
            
            # 2. Add the configuration rule
            async def configure_conn(conn):
                conn.prepare_threshold = None
            
            # 3. Pass it to the pool
            self.pool = AsyncConnectionPool(
                conninfo=conn_str,
                kwargs={"row_factory": dict_row}, 
                configure=configure_conn, # Added this
                min_size=1,
                max_size=5,
                open=False,
                timeout=self.db_conn_timeout
            )
            await self.pool.open()
            await self.pool.wait()



    @asynccontextmanager
    async def db_connection(self):
        if not self.pool:
            await self.create_pool()
        try:
            async with self.pool.connection() as conn:
                yield conn
        except Exception as e:
            log("ControlPlaneDB", "db_connection", status="error", error=str(e))
            try:
                await self.create_pool()
            finally:
                raise e

    async def initialize_db(self):
        log("ControlPlaneDB", "initialize_db", status="starting")
        async with self.db_connection() as db:
            async with db.transaction():
                async with db.cursor() as cur:
                    await cur.execute("""
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

                    await cur.execute("""
                    CREATE TABLE IF NOT EXISTS requests (
                        request_id TEXT PRIMARY KEY,
                        lambda_name TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        retries INTEGER DEFAULT 0,
                        priority INTEGER ,
                        request_data TEXT,
                        response_data TEXT,
                        checked_out_at TIMESTAMP,
                        status TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """)

                    await cur.execute("""
                    CREATE TABLE IF NOT EXISTS control_plane_health (
                        component_name TEXT PRIMARY KEY,
                        pid INTEGER NOT NULL,
                        last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """)

                    await cur.execute("CREATE INDEX IF NOT EXISTS idx_containers_status ON containers(lambda_name, status);")
                    await cur.execute("CREATE INDEX IF NOT EXISTS idx_containers_last_used ON containers(last_used_at, status);")
                    await cur.execute("CREATE INDEX IF NOT EXISTS idx_requests ON requests(status, priority);")

        log("ControlPlaneDB", "initialize_db", status="completed")

    async def count_deployed_lambda_instances(self):
        log("ControlPlaneDB", "count_deployed_lambda_instances")
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT lambda_name, COUNT(*) as count FROM containers GROUP BY lambda_name")
                res = await cur.fetchall()
                log("ControlPlaneDB", "count_deployed_lambda_instances", result_count=len(res))
                return res

    async def count_deployed_lambda_instance(self, lambda_name):
        log("ControlPlaneDB", "count_deployed_lambda_instance", lambda_name=lambda_name)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT COUNT(*) as count FROM containers WHERE lambda_name = %s", (lambda_name,))
                row = await cur.fetchone()
                count = row['count'] if row else 0
                log("ControlPlaneDB", "count_deployed_lambda_instance", lambda_name=lambda_name, count=count)
                return count
    
    async def create_provisioning_container(self, lambda_name, container_id):
        log("ControlPlaneDB", "create_provisioning_container", lambda_name=lambda_name)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("INSERT INTO containers (lambda_name, container_id, ip_address, status) VALUES (%s, %s, %s, %s) RETURNING container_id", (lambda_name, container_id, None, "provisioning")) 
                result = await cur.fetchone()
                log("ControlPlaneDB", "create_provisioning_container", lambda_name=lambda_name, container_id=result['container_id'])
                return result['container_id']

    async def add_lambda_deployed_instances(self, lambda_name, container_id, ip_address, provisioning_row_id=None):
        log("ControlPlaneDB", "add_lambda_deployed_instances", lambda_name=lambda_name, container_id=container_id, ip=ip_address)
        status = "deployed"
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                if provisioning_row_id:
                    await cur.execute("UPDATE containers SET container_id = %s, ip_address = %s, status = %s WHERE container_id = %s", (container_id, ip_address, status, provisioning_row_id)) 
                    log("ControlPlaneDB", "add_lambda_deployed_instances", status="updated_existing", provisioning_row_id=provisioning_row_id)
                else:
                    await cur.execute("INSERT INTO containers (lambda_name, container_id, ip_address, port, status) VALUES (%s, %s, %s, %s, %s)", (lambda_name, container_id, ip_address, None, status)) 
                    log("ControlPlaneDB", "add_lambda_deployed_instances", status="inserted_new")

    async def remove_lambda_deployed_instances(self, container_ids):
        log("ControlPlaneDB", "remove_lambda_deployed_instances", container_ids=container_ids)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("DELETE FROM containers WHERE container_id = ANY(%s)", (container_ids,)) 
                log("ControlPlaneDB", "remove_lambda_deployed_instances", status="deleted")
    
    async def get_lambda_deployed_instances(self, lambda_func_name, status):
        log("ControlPlaneDB", "get_lambda_deployed_instances", lambda_name=lambda_func_name, status=status)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT * FROM containers WHERE lambda_name = %s AND status = %s", (lambda_func_name, status)) 
                res = await cur.fetchall()
                log("ControlPlaneDB", "get_lambda_deployed_instances", result_count=len(res))
                return res
    
    async def get_available_containers(self, lambda_name):
        log("ControlPlaneDB", "get_available_containers", lambda_name=lambda_name)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("UPDATE containers SET status = 'busy' WHERE ip_address = (select ip_address from containers where  lambda_name = %s AND status = 'available' order by created_at desc limit 1 ) RETURNING ip_address", (lambda_name,))
                res = await cur.fetchone()
                log("ControlPlaneDB", "get_available_containers", found=bool(res))
                return res


    async def mark_instance_as_busy(self, ip_address, request_id=None):
        log("ControlPlaneDB", "mark_instance_as_busy", ip_address=ip_address, request_id=request_id)
        async with self.db_connection() as db:
            async with db.transaction():
                async with db.cursor() as cur:
                    # Ensure we only pick up containers that aren't being reaped
                    await cur.execute("""
                        UPDATE containers SET status = 'busy', last_used_at = NOW() 
                        WHERE ip_address = %s AND status = 'available' 
                        RETURNING container_id""", (ip_address,))
                    result = await cur.fetchone()
                    if result is None:
                        log("ControlPlaneDB", "mark_instance_as_busy", ip_address=ip_address, status="failed_rowcount_0")
                        return False
                    if request_id:
                        await cur.execute("UPDATE requests SET status = 'in_progress', last_used_at = NOW() WHERE request_id = %s AND status = 'pending' RETURNING request_id", (request_id,))
                        result = await cur.fetchone()
                        if not result:
                            log("ControlPlaneDB", "mark_instance_as_busy", ip_address=ip_address, status="failed_rowcount_0")
                            return False
                    log("ControlPlaneDB", "mark_instance_as_busy", ip_address=ip_address, status="success")
                    return True

    async def mark_instance_as_available(self, ip_address):
        log("ControlPlaneDB", "mark_instance_as_available", ip_address=ip_address)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                cur = await cur.execute("UPDATE containers SET status = 'available', last_used_at = NOW() WHERE ip_address = %s RETURNING ip_address", (ip_address,))
                res = await cur.fetchone()
                log("ControlPlaneDB", "mark_instance_as_available", status="updated")
                return res.get("ip_address",None)

    async def mark_instance_as_failed(self, ip_address):
        log("ControlPlaneDB", "mark_instance_as_failed", ip_address=ip_address)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("UPDATE containers SET status = 'failed', last_used_at = NOW() WHERE ip_address = %s", (ip_address,))
                log("ControlPlaneDB", "mark_instance_as_failed", status="updated")


    async def get_containers_to_destroy(self):
        log("ControlPlaneDB", "get_containers_to_destroy", status="checking")
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("""
                UPDATE containers SET status = 'destroying'
                WHERE container_id IN 
                (
                SELECT container_id FROM containers 
                WHERE 
                    (status = 'available' AND COALESCE(last_used_at, created_at) < NOW() - INTERVAL '{} minutes')
                OR 
                    (status = 'busy' AND COALESCE(last_used_at, created_at) < NOW() - INTERVAL '{} minutes')
                OR 
                    (status in ('provisioning', 'deployed') AND COALESCE(last_used_at, created_at) < NOW() - INTERVAL '{} minutes')
                OR 
                    (status = 'failed')
                OR
                    (lambda_name NOT IN ({}))
                    
                )
                RETURNING *;
                """.format(self.available_container_scale_down_time, self.busy_container_scale_down_time, self.provisioning_container_scale_down_time, ','.join(f"'{name}'" for name in self.valid_lambda_names)))
                rows = await cur.fetchall()
                log("ControlPlaneDB", "get_containers_to_destroy", found_count=len(rows))
                return rows
            
    async def remove_destroyed_containers(self, container_ids):
        if not container_ids:
            return
        log("ControlPlaneDB", "remove_destroyed_containers", container_ids=container_ids)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("DELETE FROM containers WHERE container_id = ANY(%s)", (container_ids,)) 
                log("ControlPlaneDB", "remove_destroyed_containers", status="deleted")

    async def create_lambda_request(self, request_id, lambda_func_name, request, event_type, request_body):
        log("ControlPlaneDB", "create_lambda_request", request_id=request_id, lambda_name=lambda_func_name)
        if lambda_func_name not in self.valid_lambda_names:
            log("ControlPlaneDB", "create_lambda_request", status="invalid_lambda_name")
            raise HTTPException(status_code=400, detail="Invalid lambda function name")
        
        if hasattr(request, "get"):
            event_type = event_type
            priority = 1 if event_type == "RequestResponse" else 2
            request_data = request_body
            response_data = request.get("response_data", "")
            status = request.get("status", "pending")
        else:
            event_type = "RequestResponse"
            priority = 1
            request_data = ""
            response_data = ""
            status = "pending"

        if isinstance(request_data, dict):
            request_data = json.dumps(request_data)
        if isinstance(response_data, dict):
            response_data = json.dumps(response_data)

        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("INSERT INTO requests (request_id, lambda_name, event_type, priority, request_data, response_data, status) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                                 (request_id, lambda_func_name, event_type, priority, request_data, response_data, status))
                log("ControlPlaneDB", "create_lambda_request", status="created")

    async def update_lambda_request(self, request_id, payload):
        log("ControlPlaneDB", "update_lambda_request", request_id=request_id)
        status = payload.get("status")
        response_data = payload.get("response_data")
        increment_retry = payload.get("increment_retry", False)

        if isinstance(response_data, dict):
            response_data = json.dumps(response_data)

        async with self.db_connection() as db:
            async with db.cursor() as cur:
                if increment_retry:
                    await cur.execute("""
                        UPDATE requests 
                        SET status = CASE WHEN retries >= %s THEN 'failed' ELSE %s END,
                            retries = retries + 1,
                            response_data = %s,
                            created_at = NOW(),
                            checked_out_at = NULL
                        WHERE request_id = %s
                    """, (self.retry_event_count, status, response_data, request_id))
                elif status=="processed":
                    await cur.execute("UPDATE requests SET status = %s, response_data = %s WHERE request_id = %s AND status = 'in_progress'", (status, response_data, request_id))
                else:
                    await cur.execute("UPDATE requests SET status = %s, response_data = %s WHERE request_id = %s", (status, response_data, request_id))
                log("ControlPlaneDB", "update_lambda_request", status="updated")

    async def get_request_status(self, request_id):
        """Checks the status and response of a specific request."""
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT status, response_data FROM requests WHERE request_id = %s", (request_id,))
                return await cur.fetchone()

    async def delete_stuck_requests(self):
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                log("ControlPlaneDB", "delete_stuck_requests", rr_stuck_time=self.rr_stuck_time, event_stuck_time=self.event_stuck_time)
                await cur.execute("""
                    UPDATE requests 
                    SET 
                        status = CASE 
                            WHEN event_type = 'RequestResponse' OR retries >= %s THEN 'failed'
                            ELSE 'pending'
                        END,
                        retries = CASE 
                            WHEN event_type = 'Event' AND retries < %s THEN retries + 1
                            ELSE retries
                        END,
                        created_at = CASE 
                            WHEN event_type = 'Event' AND retries < %s THEN NOW()
                            ELSE created_at
                        END
                    WHERE 
                        status IN ('pending', 'in_progress', 'busy', 'processing')
                        AND (
                            (event_type = 'RequestResponse' AND created_at < NOW() - INTERVAL '{} minutes')
                            OR 
                            (event_type = 'Event' AND created_at < NOW() - INTERVAL '{} minutes')
                        )
                """.format(self.rr_stuck_time, self.event_stuck_time), [self.retry_request_count] * 3)

    async def get_all_containers(self):
        log("ControlPlaneDB", "get_all_containers")
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT * FROM containers")
                res = await cur.fetchall()
                log("ControlPlaneDB", "get_all_containers", result_count=len(res))
                return res
    
    async def get_lambda_container_by_ip(self, ip):
        log("ControlPlaneDB", "get_lambda_container_by_ip", ip=ip)
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT lambda_name FROM containers WHERE ip_address = %s", (ip,))
                return await cur.fetchone()

    async def calculate_scaleup_requests(self):
        log("ControlPlaneDB", "calculate_scaleup_requests", status="starting")
        async with self.db_connection() as db:
            async with db.transaction():
                async with db.cursor() as cur:
                    await cur.execute("""
                    SELECT lambda_name, COUNT(*) as required_containers
                    FROM requests
                    WHERE 
                    (
                    (status = 'pending' AND event_type='RequestResponse' AND created_at > NOW() - INTERVAL '{} minutes')
                    OR (status = 'pending' AND event_type='Event' AND created_at > NOW() - INTERVAL '{} minutes')
                    )
                    AND (lambda_name in ({}))
                    GROUP BY lambda_name
                    ORDER BY MIN(priority) ASC, MIN(created_at) ASC;
                    """.format(self.rr_stuck_time, self.event_stuck_time, ','.join(f"'{name}'" for name in self.valid_lambda_names)))
                    pending_requests = await cur.fetchall()
                    
                    await cur.execute("SELECT lambda_name, status, COUNT(*) as container_count FROM containers WHERE status IN ('available','provisioning', 'busy','deployed') GROUP BY lambda_name, status")
                    containers_counts = await cur.fetchall()
                    
                    stats = {}
                    for row in containers_counts:
                        ln = row['lambda_name']
                        st = row['status']
                        cnt = row['container_count']
                        if ln not in stats:
                            stats[ln] = {'available': 0, 'provisioning': 0, 'reserved': 0, 'busy': 0, 'deployed':0}
                        stats[ln][st] = cnt
                        
                    results = []
                    for req in pending_requests:
                        name = req['lambda_name']
                        pending_cnt = req['required_containers']
                        s = stats.get(name, {'available': 0, 'provisioning': 0, 'reserved': 0, 'busy': 0, 'deployed':0})
                        total_existing = sum(s.values())
                        needed = pending_cnt - (s['provisioning'] + s['available'] + s['deployed'])
                        log("ControlPlaneDB", "calculate_scaleup_requests", name=name, s=s, needed=needed, pending_cnt=pending_cnt)
                        if needed <= 0:
                            continue
                        allowed = max(0, self.individual_lambda_scale_limit - total_existing)
                        to_create = min(needed, allowed)
                        if to_create > 0:
                            results.append({"lambda_name": name, "required_containers": to_create})
                    log("ControlPlaneDB", "calculate_scaleup_requests", result_count=len(results))
                    log("ControlPlaneDB", "calculate_scaleup_requests", results=results)
                    return results

    async def get_component_health(self, component_name):
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT * FROM control_plane_health WHERE component_name = %s", (component_name,))
                return await cur.fetchone()
    
    async def get_enqueued_events(self):
        log("ControlPlaneDB", "get_enqueued_events")
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                # Correcting the argument mismatch: The interval is handled by .format(),
                # so we only pass the scale limit for the %s placeholder.
                # Also removing 'busy' from request status count as it's a container status.
                stuck_interval = max(self.rr_stuck_time//2, 1)
                await cur.execute("""
                    UPDATE requests
                    SET checked_out_at = NOW()
                    WHERE request_id IN (
                        WITH active_counts AS (
                            SELECT lambda_name, count(*) as active_count
                            FROM requests
                            WHERE status = 'in_progress' 
                               OR (status = 'pending' AND checked_out_at > NOW() - INTERVAL '1 minute')
                            GROUP BY lambda_name
                        ),
                        eligible_requests AS (
                            SELECT r.request_id, r.lambda_name,
                                   ROW_NUMBER() OVER (PARTITION BY r.lambda_name ORDER BY r.created_at) as rn,
                                   COALESCE(ac.active_count, 0) as current_active
                            FROM requests r
                            LEFT JOIN active_counts ac ON r.lambda_name = ac.lambda_name
                            WHERE r.status = 'pending' 
                              AND r.event_type = 'Event' 
                              AND (r.checked_out_at IS NULL OR r.checked_out_at < NOW() - INTERVAL '{} minutes')
                        )
                        SELECT request_id FROM eligible_requests
                        WHERE rn <= (%s - current_active)
                    )
                    RETURNING *
                """.format(stuck_interval), (self.individual_lambda_scale_limit,))
                res = await cur.fetchall()
                low_res = []
                for row in res:
                    low_res.append({
                        "request_id": row["request_id"],
                        "lambda_name": row["lambda_name"],
                        "event_type": row["event_type"],
                        "priority": row["priority"],
                        "status": row["status"],
                        "retries": row["retries"],
                        "created_at": row["created_at"],
                        "checked_out_at": row["checked_out_at"]
                    })
                log("ControlPlaneDB","enqueued_events", low_res=low_res)
                log("ControlPlaneDB", "get_enqueued_events", result_count=len(res))
                return res

    async def mark_requests_as_processing(self, requests):
        log("ControlPlaneDB", "mark_requests_as_processing", count=len(requests))
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("UPDATE requests SET status = 'in_progress' WHERE request_id = ANY(%s)", (requests,))
                log("ControlPlaneDB", "mark_requests_as_processing", status="updated")
    
    async def mark_requests_as_processed(self, requests):
        log("ControlPlaneDB", "mark_requests_as_processed", count=len(requests))
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("UPDATE requests SET status = 'processed' WHERE request_id = ANY(%s)", (requests,))
                log("ControlPlaneDB", "mark_requests_as_processed", status="updated")

    async def clear_control_plane_health_stats(self):
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("DELETE FROM control_plane_health;")

    async def get_all_health_stats(self):
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute("SELECT * FROM control_plane_health")
                rows = await cur.fetchall()
                health_stats = {row['component_name']: row for row in rows}
                return health_stats
        
    async def update_health_stats(self, component_name, pid):
        async with self.db_connection() as db:
            async with db.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO control_plane_health (component_name, pid, last_heartbeat) 
                    VALUES (%s, %s, NOW()) 
                    ON CONFLICT (component_name) 
                    DO UPDATE SET 
                        pid = EXCLUDED.pid, 
                        last_heartbeat = NOW()
                    """,
                    (component_name, pid)
                )

if __name__=="__main__":
    test_db = ControlPlaneDB()
    asyncio.run(test_db.initialize_db())
