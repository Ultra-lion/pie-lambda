import socket
import datetime

BASE_SUBSTR = "pie-lambda"
BASE_NETWORK_BRIDGE = "lambda_bridge"

def get_local_ip():
    """
    Attempts to find the local IP address of the machine running the control plane.
    Used for DNS records and as the API endpoint for worker containers.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def parse_timestamp(ts_str):
    """
    Parses SQLite timestamp strings into datetime objects for health checks.
    """
    if isinstance(ts_str, datetime.datetime):
        return ts_str
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{ts_str}' does not match any known format")
