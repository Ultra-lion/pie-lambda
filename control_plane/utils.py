import socket 
import datetime


BASE_SUBSTR = "pie-lambda"
BASE_NETWORK_BRIDGE = "lambda_bridge"

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def parse_timestamp(ts_string):
    # SQLite default format is YYYY-MM-DD HH:MM:SS
    return datetime.datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")
