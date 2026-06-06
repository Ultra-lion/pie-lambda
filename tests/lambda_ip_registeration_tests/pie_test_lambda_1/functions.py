
from datetime import datetime



def handler(event , context):
    
    request_payload = event["payload"]
    # Record start time for runtime limit tracking
    lambda_start_time = str(datetime.now())
    
    return {"reuqest_id":context.aws_request_id, "payload":request_payload, "lambda_start_time":lambda_start_time}
