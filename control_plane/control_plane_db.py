import aiosqlite
from contextlib import asynccontextmanager

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
            CREATE TABLE IF NOT EXISTS containers (
                container_id TEXT PRIMARY KEY,
                lambda_name TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                port INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
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
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            )
            """)

            await db.execute("""
            CREATE TABLE IF NOT EXISTS scaling_queue (
                request_id TEXT PRIMARY KEY,
                lambda_name TEXT NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            )                
            """)

            await db.commit()
    


    def add_lambda_built_docker_images(self, lambda_list):
        pass    

    def count_deployed_lambda_instances(self):
        pass

    def add_lambda_deployed_instances(self):
        pass

    def remove_lambda_deployed_instances(self):
        pass

    def get_lambda_deployed_instances(self, lambda_func_name):
        pass
    

    def create_scaleup_request(self, request_id, lambda_func_name):
        pass
    
    def get_available_lambda_instance(self, lambda_func_name):
        pass
    
    def mark_instance_as_busy(self, instance_id):
        pass

    def mark_instance_as_available(self, instance_id):
        pass
    
    def create_lambda_request(self, request_id, lambda_func_name, request):
        pass

    def update_lambda_request(self, request_id, lambda_func_name, updates):
        pass

    def get_lambda_last_request_time(self):
        pass

    def update_lambda_last_request_time(self):
        pass

    def get_total_combined_requests(self):
        pass

    def increment_total_combined_requests(self):
        pass



    
