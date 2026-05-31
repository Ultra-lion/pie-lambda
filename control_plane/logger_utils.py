import logging
import time
import os

# Custom formatter to match the requested format
class PieLambdaFormatter(logging.Formatter):
    def format(self, record):
        return record.getMessage()

# Create logger
logger = logging.getLogger("pie-lambda")
logger.setLevel(logging.INFO)

# Console handler
ch = logging.StreamHandler()
ch.setFormatter(PieLambdaFormatter())
logger.addHandler(ch)

funcs_to_print_data_for = [
    # "ipc_server.handle_poke", 
    # "ipc_server.handle_poke_back",
    # "proxy_api_call",
    # "scaler_main_process",
    # "scale_up_lambda",
    "proxy_request",
]

print_all = False

def log(service, function, **datapoints):
    if print_all or function in funcs_to_print_data_for:
        datapoints_str = " ".join([f"{k}: {v}" for k, v in datapoints.items()])
        logger.info(f"[{service}] [{function}] {datapoints_str}")

