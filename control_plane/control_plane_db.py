import aiosqlite
from contextlib import asynccontextmanager
import asyncio


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
        async with aiosqlite.connect(self.db) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA busy_timeout=10000;")
            yield db
            

    async def initialize_db(self):
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
    


    async def count_deployed_lambda_instances(self):
        async with self.db_connection() as db:
            await db.execute("SELECT COUNT(*) FROM containers group by lambda_name")
            return db.fetchall()


    async def count_deployed_lambda_instance(self, lambda_name):
        async with self.db_connection() as db:
            await db.execute("SELECT COUNT(*) FROM containers where lambda_name = ?", (lambda_name,))
            return db.fetchone()[0]
    
    async def create_provisioning_container(self, lambda_name, request_id):
        async with self.db_connection() as db:
            await db.execute("INSERT INTO containers (lambda_name, container_id, ip_address, port, status, reserved_for_request) VALUES (?, ?, ?, ?, ?, ?) RETURNING container_id", (lambda_name, "test", "test", "test", "test", None)) 
            result = await db.fetchone()
            await db.commit()
            return result[0]

    async def add_lambda_deployed_instances(self, provisioning_row_id, lambda_name, container_id, ip_address, port, reserved_for_request=None):
        status="available"
        if reserved_for_request:
            status="reserved"
        if provisioning_row_id:
            async with self.db_connection() as db:
                await db.execute("UPDATE containers SET container_id = ?, ip_address = ?, port = ?, status = ?, reserved_for_request = ? WHERE container_id = ?", (container_id, ip_address, port, status, reserved_for_request, provisioning_row_id)) 
                await db.commit()
        else:
            async with self.db_connection() as db:
                await db.execute("INSERT INTO containers (lambda_name, container_id, ip_address, port, status, reserved_for_request) VALUES (?, ?, ?, ?, ?, ?)", (lambda_name, container_id, ip_address, port, status, reserved_for_request)) 
                await db.commit()

    async def remove_lambda_deployed_instances(self, container_ids):
        async with self.db_connection() as db:
            await db.execute("DELETE FROM containers WHERE container_id in ?", (container_ids,)) 
            await db.commit()
    
    
    async def get_lambda_deployed_instances(self, lambda_func_name, status):
        async with self.db_connection() as db:
            await db.execute("SELECT * FROM containers WHERE lambda_name = ? and status = ?", (lambda_func_name, status)) 
            return db.fetchall()
    
    
    async def mark_instance_as_busy(self, instance_id, request_id):
        async with self.db_connection() as db:
            result = await db.execute("UPDATE containers SET status = 'busy', last_used_at = CURRENT_TIMESTAMP, request_id = ? WHERE container_id = ? and status = 'available'", (request_id, instance_id))
            if result.rowcount == 0:
                return False
            await db.execute("UPDATE requests SET status = 'busy', last_used_at = CURRENT_TIMESTAMP WHERE request_id = ?", (request_id,))
            await db.commit()
            return True

    async def mark_instance_as_available(self, instance_id):
        async with self.db_connection() as db:
            db.execute("UPDATE containers SET status = 'available', last_used_at = CURRENT_TIMESTAMP WHERE container_id = ?", (instance_id,))
            await db.commit()

    async def get_containers_to_destroy(self):
        async with self.db_connection() as db:
            res = await db.execute("""
            UPDATE containers SET status = 'destroying'
            where id in 
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
            return rows
            
    async def remove_destroyed_containers(self, container_ids):
        if not container_ids:
            return
        async with self.db_connection() as db:
            await db.execute(f"DELETE FROM containers WHERE container_id IN ({','.join(container_ids)})") 
            await db.commit()

    async def create_lambda_request(self, request_id, lambda_func_name, request):
        async with self.db_connection() as db:
            await db.execute("INSERT INTO requests (request_id, lambda_name, event_type, priority, request_data, response_data, status) VALUES (?, ?, ?, ?, ?, ?, ?)", (request_id, lambda_func_name, request.get("event_type"), request.get("priority"), request.get("request_data"), request.get("response_data"), request.get("status")))
            await db.commit()

    async def get_all_containers(self):
        async with self.db_connection() as db:
            res = await db.execute("SELECT * FROM containers")
            return res.fetchall()
    
    async def get_available_lambda_instance(self, request_id, lambda_func_name):
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
            return res.fetchone()
    
    async def release_stale_reservations(self, request_id):
        async with self.db_connection() as db:
            await db.execute("""
            UPDATE containers 
            SET status = 'available', reserved_for_request = NULL 
            WHERE status = 'reserved' and  reserved_for_request = ?;
            """, (request_id,))
            await db.commit()

    async def calculate_scaleup_requests(self):
        async with self.db_connection() as db:
            pending_requests = await db.execute("""
            SELECT lambda_name, COUNT(*) as required_containers
            FROM requests
            WHERE status = 'pending'
            GROUP BY lambda_name
            ORDER BY priority DESC, created_at ASC;
            """)
            pending_requests = await pending_requests.fetchall()
            containers_counts = await db.execute("SELECT lambda_name, status, COUNT(*) as container_count FROM containers WHERE status in ('available','provisioning', 'reserved', 'busy') GROUP BY lambda_name, status")
            containers_counts = await containers_counts.fetchall()
            
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
                # We ignore 'available' because if they were available, the request wouldn't be pending.
                # We only care about how many are already being built ('provisioning').
                needed = pending_cnt - s['provisioning']
                
                # How many MORE are we allowed to build?
                # (Assuming self.individual_lambda_scale_limit is available)
                allowed = max(0, self.individual_lambda_scale_limit - total_existing)
                
                to_create = min(needed, allowed)
                
                if to_create > 0:
                    results.append({
                        "lambda_name": name,
                        "required_containers": to_create
                    })
            return results
    
    async def get_component_health(self, component_name):
        async with self.db_connection() as db:
            result = await db.execute("SELECT * FROM control_plane_health WHERE component_name = ?", (component_name,))
            return await result.fetchone()

if __name__=="__main__":
    test_db = ControlPlaneDB()

    asyncio.run(test_db.initialize_db())