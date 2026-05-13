import threading


class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class ControlPlaneDB(metaclass=SingletonMeta):
    def __init__(self):
        self.db = "sqlite_connection"
    

    def build_db_tables(self):
        pass
    

    def add_lambda_built_docker_images(self, lambda_list):
        pass    

    def count_deployed_lambda_instances(self):
        pass

    def add_lambda_deployed_instances(self):
        pass

    def remove_lambda_deployed_instances(self):
        pass

    def get_lambda_deployed_instances(self):
        pass

    def get_lambda_last_request_time(self):
        pass

    def update_lambda_last_request_time(self):
        pass

    def get_total_combined_requests(self):
        pass

    def increment_total_combined_requests(self):
        pass



    
