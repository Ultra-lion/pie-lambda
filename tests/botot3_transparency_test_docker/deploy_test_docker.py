import docker
import os

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



def build_and_run_test():
    image_tag = "pie-lambda-boto3-test"
    cleanup_preexisting_containers(image_tag)
    try:
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
                CA_PATH: {'bind': '/etc/ssl/certs/ca.crt', 'mode': 'ro'}
            },
            dns=["172.19.0.3"],
            detach=True,
        )
    except Exception as e:
        print(f"Error during deployment: {e}")

if __name__ == "__main__":
    build_and_run_test()
