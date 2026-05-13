from dnslib import DNSRecord, QTYPE, RR, A
from dnslib.server import DNSServer, BaseResolver
import re

intercepted_domain = '*lambda*'




def get_docker_dns():
    pass

def get_host_ip():
    pass

class HybridResolver(BaseResolver):
    def __init__(self, config):
        self.docker_dns = get_docker_dns()
        self.host_dns = get_host_ip()
        self.user_config = config
        self.control_plane_ip = config.control_plane_ip
        print(f"Docker Dns Route {self.docker_dns}")
        print(f"Host Dns Route {self.host_dns}")
    
    def is_intercepted_domain(self, domain):
        return re.match(intercepted_domain, domain)
    
    def forward_query(self, request, upstream_ip):
        try:
            reply_data = request.send(address=upstream_ip, port=53, timeout=2)
            return DNSRecord.parse(reply_data)
        except Exception as e:
            print(f"Error forwarding query to {upstream_ip}: {e}")
            return None


    def resolve(self, request, handler):
        qname = str(request.q.qname)
        qtype = QTYPE[request.q.qtype]



        if (
            (
            qname not in self.user_config['docker_bridge_network_exclude_ips']
            or qname in self.user_config['docker_bridge_network_include_ips']
            or self.is_intercepted_domain(qname)
            ) and qtype=='A'
            ):
            reply = request.reply()
            if qname == self.is_intercepted_domain(qname):
                return reply.add_answer(RR(qname, QTYPE.A, rdata=A(self.control_plane_ip)))
            else:
                return reply.add_answer(RR(qname, QTYPE.A, rdata=A(self.docker_dns)))


        if qname.count('.')<=1 or '.internal' in qname or '.local' in qname:
            reply = self.forward_query(request,self.docker_dns)
            if reply and reply.rr:
                print(f"[DOCKER NETWORK] Resolved {qname} via embedded DNS")
                return reply
        
        print(f"[Outbound Route] Forwarding {qname} to Host Machine")
        reply = self.forward_query(request, self.host_dns)
        if reply:
            return reply

        return request.reply()




def run_server(config:dict):
    resolver = HybridResolver(config)
    server = DNSServer(resolver, port=53, address="0.0.0.0")
    server.start()
