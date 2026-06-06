import json
import boto3

def lambda_handler(event, context):
    print("DEBUG: Lambda 1 initiating recursive call to Lambda 2")
    
    # Pie-Lambda DNS hijacking will route this to the Control Plane
    client = boto3.client('lambda')
    
    try:
        response = client.invoke(
            FunctionName='recursive_lambda_2',
            InvocationType='RequestResponse',
            Payload=json.dumps({"data": "Passed from Lambda 1"})
        )
        
        l2_payload = json.loads(response['Payload'].read().decode())
        
        return {
            "status": "success",
            "origin": "recursive_lambda_1",
            "child_response": l2_payload,
            "request_id": context.aws_request_id
        }
    except Exception as e:
        print(f"ERROR in Lambda 1: {e}")
        return {
            "status": "error",
            "message": str(e),
            "origin": "recursive_lambda_1"
        }
