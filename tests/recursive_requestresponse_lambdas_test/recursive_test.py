import boto3
import json
import urllib3

# Bypass SSL verification for local testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

control_plane_ip = '172.18.0.2'

def run_recursive_test():
    """
    Triggers Lambda 1, which in turn triggers Lambda 2.
    """
    # Pointing to the local Control Plane
    client = boto3.client(
        'lambda',
        endpoint_url=f'https://{control_plane_ip}', 
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        verify=False
    )

    print("🚀 Triggering Recursive Lambda Chain (L1 -> L2)...")
    try:
        # We start by calling Lambda 1
        resp = client.invoke(
            FunctionName='recursive_lambda_1',
            InvocationType='RequestResponse',
            Payload=json.dumps({"msg": "Start recursion"})
        )
        
        result_bytes = resp['Payload'].read()
        result = json.loads(result_bytes)
        
        print("\n" + "="*50)
        print("   RECURSION TEST COMPLETE")
        print("="*50)
        print(json.dumps(result, indent=4))
        print("="*50)
        
    except Exception as e:
        print(f"\n❌ Test Execution Failed: {e}")

if __name__ == "__main__":
    run_recursive_test()
