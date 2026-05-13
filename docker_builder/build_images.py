import docker
from concurrent.futures import ThreadPoolExecutor
from .validators import LambdaImageConfig
from typing import List

client = docker.from_env()



def build_custom_user_docker_images(config:dict):

    custom_images = config.get("user_defined_docker_images")

    if not custom_images:
        return True
    
    for dockerfile_path in custom_images:
        try:
            image, build_logs = client.images.build(path=dockerfile_path, tag=f"user_defined_image_{dockerfile_path.split('/')[-1]}:latest", rm=True)

            for line in build_logs:
                if "stream" in line:
                    print(line["stream"].strip)
            print(f"Successfully built: {image.tags}")
        except Exception as e:
            print(f"Failed to build: {dockerfile_path}")
            print(e)
            

    return True

def pull_user_required_docker_images(config:dict):
    images_to_pull = config.get("docker_images_to_pull")

    if not images_to_pull:
        return True
    
    for image in images_to_pull:
        try:
            client.images.pull(image)
        except Exception as e:
            print(f"Failed to pull: {image}")
            print(e)
            
    return True

def build_lambda_dockers(lambda_funcs_to_deploy:List[LambdaImageConfig]):
    
    def build_docker_worker(lambda_config):
        # threadsafe client
        image, build_logs = client.images.build(
            path=lambda_config.func_code_path,
            tag=f"{lambda_config.func_name}:latest",
            rm=True,
            buildargs={
                "lambda_handler_func_name": lambda_config.lambda_handler_function_name,
                "lambda_func_code_dir": lambda_config.func_code_path,
                "main_handler_file_name": lambda_config.func_handler_file_name,
            }
        )

        for line in build_logs:
            if "stream" in line:
                print(line["strea,"].strip)
        print(f"Successfully built: {image.tags}")
        return image

    created_images = []

    with ThreadPoolExecutor as executor:
        created_images = executor.map(build_docker_worker, lambda_funcs_to_deploy)
        print(created_images)
    

def deploy_lambda_dockers(created_images:List[docker.models.images.Image]):
    with ThreadPoolExecutor as executor:
        executor.map(deploy_lambda_docker, created_images)


def deploy_lambda_docker(docker_image):
    client.containers.run(docker_image, detach=True, network="lambda_bridge")
    
def deploy_users_images(config:dict):
    user_defined_images = config.get("user_defined_docker_images")
    for image in user_defined_images:
        client.containers.run(f"user_defined_image_{image.split('/')[-1]}:latest", detach=True, network="lambda_bridge")
    
    docker_images_to_pull =     config.get("docker_images_to_pull")
    for image in docker_images_to_pull:
        client.containers.run(image, detach=True, network="lambda_bridge")


def build_lambda_functions(config:dict):
    lambda_funcs_to_deploy = config.get("lambda_funcs_to_deploy")
    if not lambda_funcs_to_deploy:
        raise Exception("No config found for lambda functions")
    
    for i in lambda_funcs_to_deploy:
        LambdaImageConfig.model_validate(i)
    
    build_lambda_dockers(lambda_funcs_to_deploy)
        

    

def build_docker_network():
    network = client.networks.create(
        name="lambda_bridge",
        driver="bridge",
        check_duplicate=True
    )
    return network

    

def setup_docker_network_bridge(config:dict):
    build_docker_network()

def setup_docker_network_transparent_dns(config:dict):
    def setup_ips_to_exclude():
        pass
    pass

def build_control_plane_docker(config:dict):
    pass

def deploy_control_plane_docker(config:dict):
    pass


def build(config:dict):
    pull_user_required_docker_images(config)
    build_custom_user_docker_images(config)
    build_lambda_dockers(config)
    build_control_plane_docker(config)

def deploy(config:dict):
    deploy_users_images(config)
    deploy_lambda_docker(config)


def teardown(config:dict):
    pass

def shutdown(config:dict):
    pass

def run_existing(config:dict):
    pass