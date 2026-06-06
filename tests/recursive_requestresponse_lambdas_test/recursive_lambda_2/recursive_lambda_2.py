import json

def lambda_handler(event, context):
    print(f"DEBUG: Lambda 2 received event: {json.dumps(event)}")
    return {
        "status": "success",
        "origin": "recursive_lambda_2",
        "echo": event.get("data", "nothing"),
        "request_id": context.aws_request_id
    }
