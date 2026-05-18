import docker
import os
import argparse 

client = docker.from_env()

# Paths relative to the project root or absolute
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CA_PATH = os.path.abspath(os.path.join(BASE_DIR, "../../certs/ca.crt")) # Adjust path to your generated CA


def cleanup_preexisting_containers(image_tag):
    print(f"Cleaning up preexisting containers for {image_tag}...")
    containers = client.containers.list(all=True, filters={"ancestor": image_tag})
    for container in containers:
        print(f"Removing container {container.short_id}...")
        container.remove(force=True)



def build_and_run_test(build_image):
    image_tag = "pie-lambda-boto3-test"
    cleanup_preexisting_containers(image_tag)
    try:
        if build_image:
            existing_image = client.images.get(image_tag)
            if existing_image:
                existing_image.remove()
            client.images.build(path=BASE_DIR, tag=image_tag)
        else:
            client.images.get(image_tag)
    except docker.errors.ImageNotFound:
        print(f"Image {image_tag} not found, building...")
        client.images.build(path=BASE_DIR, tag=image_tag)

    print("Running test container...",CA_PATH) 
    
    try:
        # Note: 'lambda_bridge' must exist. 
        # The CA is mounted and the env var points to it.
        container = client.containers.run(
            image=image_tag,
            network="lambda_bridge",
            command="tail -f /dev/null",
            environment={
                "AWS_CA_BUNDLE": "/etc/ssl/certs/ca.crt"
            },
            volumes={
                CA_PATH: {'bind': '/etc/ssl/certs/ca.crt', 'mode': 'ro'},
                BASE_DIR: {'bind': '/app', 'mode': 'rw'}
            },
            dns=["172.18.0.2"],
            detach=True,
        )
    except Exception as e:
        print(f"Error during deployment: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pie Lambda Local AWS Test")
    
    parser.add_argument("--build_image", 
                        dest="build_image", 
                        default=True, 
                        help="Build the image")
    args = parser.parse_args()
    
    build_and_run_test(args.build_image)
