from pydantic import BaseModel



class LambdaImageConfig(BaseModel):
    func_name:str
    func_code_path:str
    func_handler_file_name:str
    lambda_handler_function_name:str
    lambda_language:str
    
