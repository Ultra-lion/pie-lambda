
from pie_lambda_module_levels_test.mod1.file1 import func1
from pie_lambda_module_levels_test.mod1.mod2.file2 import func2
from datetime import datetime


def handlerino(event , context):
    lambda_start_time = str(datetime.now())
    
    return {"reuqest_id":context.aws_request_id, "func1":func1(), "func2":func2(), "lambda_start_time":lambda_start_time}
