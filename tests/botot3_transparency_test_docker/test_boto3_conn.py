import boto3
import os
import json

def test_lambda_connectivity():
    print(f"Testing connectivity to AWS Lambda...")
    print(f"AWS_CA_BUNDLE is set to: {os.getenv('AWS_CA_BUNDLE')}")
    

    def log_request(request, **kwargs):
        print(f"\n[DEBUG] Boto3 is actualy hitting: {request.url}")
        print(f"[DEBUG] Method: {request.method}")
        # print(f"[DEBUG] Headers: {request.headers}")
    

    # We use a dummy region and credentials since we're hitting our local proxy
    client = boto3.client(
        'lambda', 
        region_name='us-east-1',
        aws_access_key_id='testing',
        aws_secret_access_key='testing'
    )
    
    # Register the hook
    client.meta.events.register('before-send.lambda.*', log_request)

    try:
        # A simple call to check if the handshake and routing work
        print("Attempting list_functions...")
        response = client.list_functions()
        print("Success! Response received from local Control Plane:")
        print(json.dumps(response, indent=2, default=str))
    except Exception as e:
        print(f"Failed to connect: {e}")

if __name__ == "__main__":
    test_lambda_connectivity()
