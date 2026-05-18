FROM python:3.11.4-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY test_boto3_conn.py .

# We expect the CA to be mounted at /etc/ssl/certs/ca.crt
# The deployment script will set AWS_CA_BUNDLE to this path
CMD ["python", "test_boto3_conn.py"]
