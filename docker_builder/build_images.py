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
        

    

def build_docker_network():
    network = client.networks.create(
        name=BASE_NETWORK_BRIDGE,
        driver="bridge",
        check_duplicate=True
    )
    return network

    
def check_if_network_exists():
    exists = False
    docker_networks = client.networks.list()
    for network in docker_networks:
        if network.name == BASE_NETWORK_BRIDGE:
            exists=True
            break
    if not exists:
        build_docker_network()

def setup_docker_network_bridge():
    docker_networks = client.networks.list()
    for network in docker_networks:
        if network.name == BASE_NETWORK_BRIDGE:
            return
    build_docker_network()

def build_control_plane_docker():
    
    try:
        existing_control_plane_container = client.containers.get("pie-lambda-control-plane")
        if existing_control_plane_container:
            existing_control_plane_container.stop()
            existing_control_plane_container.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        raise e
        
    try:
        existing_control_plane_image = client.images.get(f"{BASE_SUBSTR}-control-plane:latest")
        if existing_control_plane_image:
            existing_control_plane_image.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        raise e


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
    ca_cert_file = os.path.join(ca_path, "ca.crt")
    
    volumes = {
        host_socket:{
            'bind':'/var/run/docker.sock',
            'mode':'rw'
        },
        "/home/rohan/Desktop/FUN-Projects/pie-lambda/control_plane":{
            'bind':'/app/control_plane',
            'mode':'rw'
        }
    }
    
    control_plane_container = client.containers.create(
        image=control_plane_docker_image,
        # entrypoint="tail -f /dev/null",
        name="pie-lambda-control-plane",
        network=BASE_NETWORK_BRIDGE,
        volumes=volumes,
        detach=True,
        extra_hosts={"host.docker.internal":"host-gateway"}
    )
    ca_stream = io.BytesIO()
    with tarfile.open(fileobj=ca_stream, mode='w') as tar:
        with open(ca_cert_file, 'rb') as f:
            info = tarfile.TarInfo(name='ca.crt')
            info.size = os.path.getsize(ca_cert_file)
            tar.addfile(info, f)
    control_plane_container.put_archive('/etc/ssl/certs/', ca_stream.getvalue())
    
    # 2. Prepare and push ALL CP certs to the app directory
    certs_stream = io.BytesIO()
    with tarfile.open(fileobj=certs_stream, mode='w') as tar:
        for filename in ['ca.crt', 'server.crt', 'server.key', 'ca.key']:
            file_path = os.path.join(ca_path, filename)
            if os.path.exists(file_path):
                info = tarfile.TarInfo(name=filename)
                info.size = os.path.getsize(file_path)
                with open(file_path, 'rb') as f:
                    tar.addfile(info, f)
    
    control_plane_container.put_archive('/app/control_plane/', certs_stream.getvalue())

    config_file_path = config.get("config_file_path")
    config_stream = io.BytesIO()
    with tarfile.open(fileobj=config_stream, mode='w') as tar:
        with open(config_file_path, 'rb') as f:
            info = tarfile.TarInfo(name='config.json')
            info.size = os.path.getsize(config_file_path)
            tar.addfile(info, f)
    control_plane_container.put_archive('/app/control_plane/', config_stream.getvalue())
    
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
    

def rebuildlambdas(config):
    teardown_lambda_functions(config)
    build_lambda_functions(config)

def build(config:dict):
    setup_docker_network_bridge()
    build_control_plane_docker()
    build_lambda_functions(config)
    
def deploy(config:dict):
    check_if_network_exists()
    deploy_control_plane_docker(config)


def teardownall(config:dict):

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
    
def teardowncontainers(config:dict):
    all_images = client.images.list(all=True)
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
                
    print("len of containers: ", len(matching_containers))
    for container in matching_containers:
        try:
            container.reload()
            if container.status == "running":
                container.stop(timeout=2)
            
        except Exception as e:
            print(f"Could Not stop container {container.name} Error: {e}")

        try:
            container.remove(force=True)
            print(f"Removed container {container.name}")
        except Exception as e:
            print(f"Could Not remove container {container.name} Error: {e}")
    
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
        try:
            container.stop()
        except Exception as e:
            print(f"Could Not stop container {container.name} Error: {e}")

def run_existing(config:dict):
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
            container.start()
        except Exception as e:
            print(f"Could Not start container {container.name} Error: {e}")