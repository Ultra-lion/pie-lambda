
import docker



class LambdaScaler:
    def __init__(self):
        self.docker_client = docker.from_env()

    def get_lambda_image_name(self, lambda_func_name):
        pass
    
    def get_existing_lambda_containers(self, lambda_func_name):
        pass

    def scale_up_lambda(self, lambda_func_name):
        container = self.docker_client.containers.run(
            image= self.get_lambda_image_name(lambda_func_name),
            name= self.get_existing_lambda_containers(lambda_func_name),
            detach=True,
        )