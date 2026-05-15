
from websockets import client
import argparse
from docker_builder.build_images import build, deploy, shutdown, teardown, run_existing
import argparse 
import json
import os
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta
import docker

from control_plane.control_plane_db import ControlPlaneDB


def generate_master_ca():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
    x509.NameAttribute(NameOID.COMMON_NAME, u"Pie-Lambda Local CA"),
    x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Pie-Lambda"),
    ])

    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now() - timedelta(hours=1))
        .not_valid_after(datetime.now() + timedelta(days=365))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .sign(private_key, hashes.SHA256())
    )


    with open("certs/ca.crt", "wb") as f:
        f.write(ca_cert.public_bytes(serialization.Encoding.PEM))
    
    with open("certs/ca.key", "wb") as f:
        f.write(private_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))

def generate_aws_impersonator_cert():
    with open("certs/ca.key","rb") as f:
        ca_private_key = serialization.load_pem_private_key(f.read(), password=None)
    
    with open("certs/ca.crt","rb") as f:
        ca_cert = x509.load_pem_x509_certificate(f.read())
    
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, u"lambda.amazonaws.com"),
    ])


    sans = x509.SubjectAlternativeName([
    x509.DNSName(u"lambda.us-east-1.amazonaws.com"),
    x509.DNSName(u"lambda.us-west-2.amazonaws.com"),
    x509.DNSName(u"s3.amazonaws.com"),
    x509.DNSName(u"pie-lambda.local"),
    ])

    server_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now() - timedelta(hours=1))
        .not_valid_after(datetime.now() + timedelta(days=365))
        .add_extension(sans, critical=False)
        .sign(private_key=ca_private_key, algorithm=hashes.SHA256())
    )

    with open("certs/server.crt", "wb") as f:
        f.write(server_cert.public_bytes(serialization.Encoding.PEM))
    
    with open("certs/server.key", "wb") as f:
        f.write(server_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))

def generate_certs():
    if not os.path.exists("certs"):
        os.makedirs("certs", exist_ok=True)
        generate_master_ca()
        generate_aws_impersonator_cert()

def check_if_docker_running():
    try:
        client = docker.from_env()
        if client.ping():
            print("Docker Engine Running")
    except docker.errors.APIError as e1:
        raise Exception("Docker Engine Not Running")
    except Exception as e2:
        raise e2

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Pie Lambda Local AWS Toolkit")
    
    parser.add_argument("--config", 
                        dest="config_file_path", 
                        default="config.json", 
                        help="Path to config.json (default: config.json in root)")
    
    parser.add_argument("--command", 
                        dest="command", 
                        choices=["build", "deploy", "teardown", "shutdown", "RunExisting"],
                        default="build",
                        help="Action to perform")
    args = parser.parse_args()
    # Accessing the values
    config_file_path = args.config_file_path
    command = args.command
    # Checking if the default (or provided) config actually exists
    if not os.path.exists(config_file_path):
        print(f"🐶 Oops! Config file not found at: {os.path.abspath(config_file_path)}")
        print("Tip: Make sure you have a 'config.json' in your root directory.")
        exit(1)
    with open(config_file_path, "r") as f:
        config = json.load(f)

    if not config:
        raise Exception("Need config file")

    if not command:
        raise Exception("Need a command")
    
    control_plane_db = ControlPlaneDB()

    check_if_docker_running()

    generate_certs()
    
    client = docker.from_env()

    for img in client.images.list():
        print(img.tags)
        print(img.id)

    # exit(1)
    match command:
        case "build":
            build(config)
        case "deploy":
            deploy(config)
        case "teardown":
            teardown(config)
        case "shutdown":
            shutdown(config)
        case "RunExisting":
            run_existing(config)
