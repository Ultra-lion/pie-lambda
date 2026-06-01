import os 
import docker
from concurrent.futures import ThreadPoolExecutor
from .validators import LambdaImageConfig
from typing import List
import sys
import io, tarfile

from control_plane.utils import BASE_SUBSTR, BASE_NETWORK_BRIDGE

client = docker.from_env()



def build_lambda_dockers(lambda_funcs_to_deploy:List[LambdaImageConfig]):

    os.makedirs(".build",exist_ok=True)

    def build_docker_worker(lambda_config):
        # threadsafe client
        if os.path.exists(f".build/{lambda_config['func_name']}"):
            os.system(f"rm -rf .build/{lambda_config['func_name']}")
        os.system(f"cp -r {lambda_config['func_code_path']} .build/{lambda_config['func_name']}")
        os.system(f"cp docker_builder/python/Dockerfile .build/{lambda_config['func_name']}/Dockerfile")
        os.system(f"cp docker_builder/python/bootstrap.sh .build/{lambda_config['func_name']}/bootstrap.sh")

        if not os.path.exists(f"{lambda_config['func_code_path']}/requirements.txt"):
            print(f"⚠️ Warning: No requirements.txt found for {lambda_config['func_name']}.")
            print(f"💡 Tip: If your Lambda has external dependencies like 'openai', please create a requirements.txt at {lambda_config['func_code_path']}/requirements.txt to ensure they are installed correctly.")
        
        try:
            image, build_logs = client.images.build(
                path=f".build/{lambda_config['func_name']}",
                tag=f"{BASE_SUBSTR}-{lambda_config['func_name']}:latest",
                rm=True,
                buildargs={
                    "lambda_handler_func_name": lambda_config["lambda_handler_function_name"],
                    "lambda_func_code_dir": f".",
                    "main_handler_file_name": lambda_config["func_handler_file_name"],
                    "lambda_func_name": lambda_config["func_name"],
                }
            )

            for line in build_logs:
                if "stream" in line:
                    print(line["stream"].strip())
            print(f"Successfully built: {image.tags}")
            return image
        except docker.errors.BuildError as e:
            print(f"\n❌ Docker build failed for {lambda_config['func_name']}. Detailed logs below:")
            for line in e.build_log:
                if "stream" in line:
                    print(line["stream"].strip())
            # Re-raise so the process still exits with error
            raise e

    created_images = []

    with ThreadPoolExecutor() as executor:
        images = list(executor.map(build_docker_worker, lambda_funcs_to_deploy.values()))
        created_images.extend(images)



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
    docker_networks = client.networks.list()
    for network in docker_networks:
        if network.name == BASE_NETWORK_BRIDGE:
            return
    build_docker_network()

def build_control_plane_docker():
    
    existing_control_plane_container = client.containers.get("pie-lambda-control-plane")
    if existing_control_plane_container:
        existing_control_plane_container.stop()
        existing_control_plane_container.remove()

    existing_control_plane_image = client.images.get(f"{BASE_SUBSTR}-control-plane:latest")
    if existing_control_plane_image:
        existing_control_plane_image.remove()


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
    
    ca_path = os.path.abspath("certs")
    control_plane_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "control_plane"))
    
    ca_cert_file = os.path.join(ca_path, "ca.crt")
    server_cert_file = os.path.join(ca_path, "server.crt")
    server_key_file = os.path.join(ca_path, "server.key")

    volumes = {
        host_socket:{
            'bind':'/var/run/docker.sock',
            'mode':'rw'
        },
        control_plane_path:{
            'bind':'/app/control_plane',
            'mode':'rw'
        },
        ca_cert_file:{
            'bind':'/etc/ssl/certs/ca.crt',
            'mode':'ro'
        },
        server_cert_file:{
            'bind':'/app/control_plane/server.crt',
            'mode':'ro'
        },
        server_key_file:{
            'bind':'/app/control_plane/server.key',
            'mode':'ro'
        }
    }
    
    # client.containers.run(
    #     image=control_plane_docker_image,
    #     name="pie-lambda-control-plane",
    #     network=BASE_NETWORK_BRIDGE,
    #     volumes=volumes,
    #     detach=True,
    #     extra_hosts={"host.docker.internal":"host-gateway"}
    # )
    control_plane_container = client.containers.create(
        image=control_plane_docker_image,
        name="pie-lambda-control-plane",
        network=BASE_NETWORK_BRIDGE,
        volumes=volumes,
        detach=True,
        extra_hosts={"host.docker.internal":"host-gateway"}
    )

    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w') as tar:
        with open('ca.crt', 'rb') as f:
            info = tarfile.TarInfo(name='ca.crt')
            info.size = os.path.getsize('ca.crt')
            tar.addfile(info, f)
    
    control_plane_container.put_archive('/etc/ssl/certs/', stream.getvalue())
    
    # NOW start it
    control_plane_container.start()


def teardown_lambda_functions(config:dict):
    all_images = client.images.list()
    matching_images = []
    lambda_funcs_to_deploy = config.get("lambda_funcs_to_deploy")
    lambda_names = [lambda_config["func_name"] for lambda_config in lambda_funcs_to_deploy.values()]
    
    for lambda_name in lambda_names:
        for image in all_images:
            for tag in image.tags:
                if lambda_name.lower() in tag.lower():
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
        try:
            container.reload()
            if container.status == "running":
                container.stop(timeout=2)
            
        except Exception as e:
            print(f"Could Not stop container {container.name} Error: {e}")

        try:
            container.remove(force=True)
        except Exception as e:
            print(f"Could Not remove container {container.name} Error: {e}")
    
    for image in matching_images:
        try:
            image.remove(force=True)
        except Exception as e:
            print(f"Could Not remove image {image.tags} Error: {e}")
    
    try:

        network = client.networks.get(BASE_NETWORK_BRIDGE)
        network.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        print(f"Could Not remove Network {BASE_NETWORK_BRIDGE} Error: {e}")
    

def rebuildlambdas(config):
    teardown_lambda_functions(config)
    build_lambda_functions(config)

def build(config:dict):
    setup_docker_network_bridge()
    build_control_plane_docker()
    build_lambda_functions(config)
    
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
        try:
            container.reload()
            if container.status == "running":
                container.stop(timeout=2)
            
        except Exception as e:
            print(f"Could Not stop container {container.name} Error: {e}")

        try:
            container.remove(force=True)
        except Exception as e:
            print(f"Could Not remove container {container.name} Error: {e}")
    
    for image in matching_images:
        try:
            image.remove(force=True)
        except Exception as e:
            print(f"Could Not remove image {image.tags} Error: {e}")
    
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
    print(matching_images)
    matching_containers = []
    all_containers_list = client.containers.list(all=True)
    for image in matching_images:
        for container in all_containers_list:
            print(image.id, container.image.id)
            if image.id == container.image.id:
                matching_containers.append(container)
                break
    print(matching_containers)
    for container in matching_containers:
        container.start()