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

    # try:
    #     # A simple call to check if the handshake and routing work
    #     print("Attempting list_functions...")
    #     response = client.list_functions()
    #     print("Success! Response received from local Control Plane:")
    #     print(json.dumps(response, indent=2, default=str))
    # except Exception as e:
    #     print(f"Failed to connect: {e}")
    

    try:
        print("\nAttempting to invoke a function...")
        # The FunctionName triggers the path interception in your control plane.
        # Payload must be bytes or a file-like object.
        invoke_response = client.invoke(
            FunctionName='test-function',
            InvocationType='RequestResponse', # Sync call
            Payload=json.dumps({"message": "Hello from Boto3"}).encode('utf-8')
        )
        
        print("Success! Response metadata:")
        print(f"Status Code: {invoke_response['ResponseMetadata']['HTTPStatusCode']}")
        
        # Read the StreamingBody response payload
        payload = invoke_response['Payload'].read().decode('utf-8')
        print(f"Payload: {payload}")
        
    except Exception as e:
        print(f"Failed to invoke: {e}")


if __name__ == "__main__":
    test_lambda_connectivity()