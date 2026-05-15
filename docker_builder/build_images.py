import os 
import docker
from concurrent.futures import ThreadPoolExecutor
from .validators import LambdaImageConfig
from typing import List
import sys


client = docker.from_env()

BASE_SUBSTR = "pie-lambda"
BASE_NETWORK_BRIDGE = "lambda_bridge"

def build_lambda_dockers(lambda_funcs_to_deploy:List[LambdaImageConfig]):

    os.makedirs(".build",exist_ok=True)

    def build_docker_worker(lambda_config):
        # threadsafe client
        if os.path.exists(f".build/{lambda_config['func_name']}"):
            os.system(f"rm -rf .build/{lambda_config['func_name']}")
        os.system(f"cp -r {lambda_config['func_code_path']} .build/{lambda_config['func_name']}")
        os.system(f"cp docker_builder/python/Dockerfile .build/{lambda_config['func_name']}/Dockerfile")
        os.system(f"cp docker_builder/python/bootstrap.sh .build/{lambda_config['func_name']}/bootstrap.sh")
        
        image, build_logs = client.images.build(
            path=f".build/{lambda_config['func_name']}",
            tag=f"{BASE_SUBSTR}-{lambda_config['func_name']}:latest",
            rm=True,
            buildargs={
                "lambda_handler_func_name": lambda_config["lambda_handler_function_name"],
                "lambda_func_code_dir": f".",
                "main_handler_file_name": lambda_config["func_handler_file_name"],
            }
        )

        for line in build_logs:
            if "stream" in line:
                print(line["stream"].strip())
        print(f"Successfully built: {image.tags}")
        return image

    created_images = []

    # with ThreadPoolExecutor() as executor:
    #     created_images = executor.map(build_docker_worker, lambda_funcs_to_deploy.values())
    #     print([image for image in created_images])
    
    # print([image for image in created_images])
    
    for func in lambda_funcs_to_deploy.values():
        build_docker_worker(func)


def build_lambda_functions(config:dict):
    lambda_funcs_to_deploy = config.get("lambda_funcs_to_deploy")
    if not lambda_funcs_to_deploy:
        raise Exception("No config found for lambda functions")
    
    for i, func_config in lambda_funcs_to_deploy.items():
        LambdaImageConfig.model_validate(func_config)
    
    build_lambda_dockers(lambda_funcs_to_deploy)

    print("printing image tags")

    for img in client.images.list():
        print(img.tags)
        

    

def build_docker_network():
    network = client.networks.create(
        name=BASE_NETWORK_BRIDGE,
        driver="bridge",
        check_duplicate=True
    )
    return network

    

def setup_docker_network_bridge():
    build_docker_network()

def build_control_plane_docker():
    image, build_logs = client.images.build(
        path="control_plane",
        tag=f"{BASE_SUBSTR}-control-plane:latest",
        rm=True
    )

    for line in build_logs:
        if "stream" in line:
            print(line["stream"].strip())
    print(f"Successfully built: {image.tags}")
    return image

def get_host_docker_socket():
    if sys.platform == "win32":
        return "//./pipe/docker_engine"
    # This works for both Linux AND macOS!
    return "/var/run/docker.sock"


def deploy_control_plane_docker(config:dict):

    try:
        container = client.containers.get("pie-lambda-control-plane")
        container.stop()
        container.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        print(f"Could Not remove container pie-lambda-control-plane Error: {e}")

    control_plane_docker_image = client.images.get(f"{BASE_SUBSTR}-control-plane:latest")
    
    host_socket = get_host_docker_socket()

    volumes = {
        host_socket:{
            'bind':'/var/run/docker.sock',
            'mode':'rw'
        }
    }
    
    client.containers.run(
        image=control_plane_docker_image,
        name="pie-lambda-control-plane",
        network=BASE_NETWORK_BRIDGE,
        volumes=volumes,
        detach=True,
        extra_hosts={"host.docker.internal":"host-gateway"}
    )


def build(config:dict):
    setup_docker_network_bridge()
    build_control_plane_docker()
    # build_lambda_functions(config)
    
def deploy(config:dict):
    deploy_control_plane_docker(config)


def teardown(config:dict):

    all_images = client.images.list()
    matching_images = []
    for image in all_images:
        for tag in image.tags:
            if BASE_SUBSTR.lower() in tag.lower():
                matching_images.append(image)
                break

    matching_containers = []
    all_containers_list = client.containers.list(all=True)
    for image in matching_images:
        for container in all_containers_list:
            if image.id == container.image.id:
                matching_containers.append(container)
                break
    
    for container in matching_containers:
        container.stop()
        container.remove()
    
    for image in matching_images:
        image.remove()
    
    try:

        network = client.networks.get(BASE_NETWORK_BRIDGE)
        network.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        print(f"Could Not remove Network {BASE_NETWORK_BRIDGE} Error: {e}")
    
    

def shutdown(config:dict):
    all_images = client.images.list()
    matching_images = []
    for image in all_images:
        for tag in image.tags:
            if BASE_SUBSTR.lower() in tag.lower():
                matching_images.append(image)
                break
    
    matching_containers = []
    all_containers_list = client.containers.list()
    for image in matching_images:
        for container in all_containers_list:
            if image.id == container.image.id:
                matching_containers.append(container)
                break
    
    for container in matching_containers:
        container.stop()

def run_existing(config:dict):
    all_images = client.images.list()
    matching_images = []
    for image in all_images:
        for tag in image.tags:
            if BASE_SUBSTR.lower() in tag.lower():
                matching_images.append(image)
                break
    
    matching_containers = []
    all_containers_list = client.containers.list()
    for image in matching_images:
        for container in all_containers_list:
            if image.id == container.image.id:
                matching_containers.append(container)
                break
    
    for container in matching_containers:
        container.start()