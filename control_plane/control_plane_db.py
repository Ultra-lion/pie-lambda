import aiosqlite
from contextlib import asynccontextmanager
import asyncio

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class ControlPlaneDB(metaclass=SingletonMeta):
    def __init__(self):
        self.db = "pie_lambda.db"


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
    

    async def add_lambda_deployed_instances(self, lambda_name, container_id, ip_address, port):
        status="available"
        async with self.db_connection() as db:
            await db.execute("INSERT INTO containers (lambda_name, container_id, ip_address, port, status) VALUES (?, ?, ?, ?, ?)", (lambda_name, container_id, ip_address, port, status)) 
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


    
if __name__=="__main__":
    test_db = ControlPlaneDB()

    asyncio.run(test_db.initialize_db())