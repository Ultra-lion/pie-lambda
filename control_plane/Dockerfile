# Use the specific version requested
FROM python:3.11.4-slim

USER root

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies (minimal)
# Install system dependencies (including Node 20)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    python3-dev \
    curl \
    ca-certificates \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*


RUN npm install @electric-sql/pglite @electric-sql/pglite-socket

# Copy the entire project context
# This ensures that internal imports like 'from control_plane.control_plane_db import ...' work correctly
COPY . /app/control_plane/

WORKDIR /app/control_plane/

RUN pip install --no-cache-dir -r requirements.txt


# Expose standard AWS interception ports
# 443: The Load Balancer (mimicking AWS HTTPS endpoints)
# 53: The DNS server (UDP is critical for DNS)
EXPOSE 443 53/udp 53/tcp

STOPSIGNAL SIGINT

ENTRYPOINT ["python", "run_control_plane.py"]

