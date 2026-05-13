import aiosqlite


class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class ControlPlaneDB(metaclass=SingletonMeta):
    def __init__(self):
        self.db = "pie_lambda.db"
        self.db_connection = None

    async def get_conn(self):

        if self.db_connection is None:
            self.db_connection = await aiosqlite.connect(self.db)
            self.db_connection.row_factory = aiosqlite.Row
            await self.db_connection.execute("PRAGMA journal_mode=WAL;")
            
        return self.db_connection
    

    async def build_db_tables(self):
        pass
    

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
    
    def get_available_lambda_instance(self, lambda_func_name):
        pass
    
    def create_lambda_request(self, lambda_func_name, request):
        pass

    def update_lambda_request(self, lambda_func_name, updates):
        pass

    def get_lambda_last_request_time(self):
        pass

    def update_lambda_last_request_time(self):
        pass

    def get_total_combined_requests(self):
        pass

    def increment_total_combined_requests(self):
        pass



    
