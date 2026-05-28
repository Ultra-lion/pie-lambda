from dnslib import DNSRecord, QTYPE, RR, A
from dnslib.server import DNSServer, BaseResolver
import socketserver
import re
import socket 
import struct
import os
import asyncio


from control_plane_db import ControlPlaneDB

intercepted_domain = '.*lambda.*\.amazonaws\.com.*'




def get_docker_dns():
    return "127.0.0.11"

def get_host_ip():
    try:
        with open("/proc/net/route") as fh:
            for line in fh:
                fields = line.strip().split()
                
                if fields[1] == '00000000':
                    return socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
    except Exception as e:
        print("Could not detect Host IP")
        return "172.17.0.1"

class HybridResolver(BaseResolver):
    def __init__(self, config):
        self.docker_dns = get_docker_dns()
        self.host_dns = get_host_ip()
        self.user_config = config
        self.control_plane_ip = config.get('control_plane_ip')
        print(f"Docker Dns Route {self.docker_dns}")
        print(f"Host Dns Route {self.host_dns}")
    
    def is_intercepted_domain(self, domain):
        return re.match(intercepted_domain, domain)
    
    def forward_query(self, request, upstream_ip):
        try:
            reply_data = request.send(upstream_ip, port=53, timeout=2)
            return DNSRecord.parse(reply_data)
        except Exception as e:
            print(f"Error forwarding query to {upstream_ip}: {e}")
            return None


    def resolve(self, request, handler):
        qname = str(request.q.qname)
        qtype = QTYPE[request.q.qtype]
        
        # 0. Host Gateway Resolution
        # Automatically resolve special hostnames to the gateway IP (detected in __init__).
        # This allows users to reach host services (DBs, etc) without knowing the IP.
        if qtype == 'A' and qname.strip('.') in ['host.docker.internal', 'pie-lambda.local', 'host.pie-lambda.local']:
            print(f"!!! RESOLVING HOST GATEWAY: {qname} -> {self.host_dns}")
            reply = request.reply()
            reply.add_answer(RR(qname, QTYPE.A, rdata=A(self.host_dns)))
            return reply

        # 1. Local Interception: AWS Lambda API calls
        # If the domain matches our interception pattern, point it to our Control Plane IP.
        if qtype in ['A', 'AAAA'] and self.is_intercepted_domain(qname):
            if qtype == 'AAAA':
                return request.reply()
            print(f"!!! INTERCEPTING A: {qname}")
            reply = request.reply()
            reply.add_answer(RR(qname, QTYPE.A, rdata=A(self.control_plane_ip)))
            return reply

        # 2. Primary Resolver: Docker DNS (127.0.0.11)
        # This handles container names and automatically recurses to the host's DNS.
        reply = self.forward_query(request, self.docker_dns)
        if reply and reply.rr:
            print(f"[DNS] {qname} resolved via Docker DNS")
            return reply
        
        # 3. Fallback: Host DNS
        # Only used if Docker DNS fails to provide an answer.
        print(f"[DNS] Falling back to Host Machine DNS for {qname}")
        reply = self.forward_query(request, self.host_dns)
        if reply:
            return reply

        return request.reply()
    


async def start_heartbeat(component_name):
    db = ControlPlaneDB()
    pid = os.getpid()
    
    while True:
        async with db.db_connection() as conn:
            await conn.execute(
                "REPLACE INTO control_plane_health (component_name, pid, last_heartbeat) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (component_name, pid)
            )
            await conn.commit()
        await asyncio.sleep(5)




async def run_server(config:dict):
    asyncio.create_task(start_heartbeat("DNS_SERVER"))
    resolver = HybridResolver(config)
    # Using ThreadingUDPServer allows the DNS server to handle multiple 
    # requests concurrently. Each request will run in its own thread, 
    # so a blocking 'forward_query' won't freeze the whole server.
    server = DNSServer(resolver, port=53, address="0.0.0.0", server=socketserver.ThreadingUDPServer)
    
    await asyncio.to_thread(server.start)
    print("oi")



if __name__=="__main__":
    asyncio.run(run_server({
        "control_plane_ip": os.getenv("CONTROL_PLANE_IP", "0.0.0.0")
    }))

    print("oooga")
