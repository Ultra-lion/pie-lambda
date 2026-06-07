# 🥧 Pie-Lambda

**The Snappy, Transparent, and Recursive AWS Lambda Emulator.**

Pie-Lambda is a high-performance local control plane that allows you to run complex AWS Lambda architectures on your local machine with **zero code changes**. Unlike standard emulators that require you to override `endpoint_url` in every Boto3 client, Pie-Lambda uses **Transparent DNS Hijacking** to intercept and route AWS traffic automatically.

> [!NOTE]
> Currently, Pie-Lambda supports **Python** runtimes and intercepts any traffic matching the pattern: `lambda.*amazonaws.*com.*`


## 🚀 The "Killer Feature": Recursive Execution
In most local environments, if `Lambda A` tries to invoke `Lambda B` via the AWS SDK, the call fails or requires a complex networking setup. 

**Pie-Lambda solves this.** Because it intercepts traffic at the DNS level, your Boto3/SDK calls within a container are automatically routed back to the control plane, allowing for seamless, infinite recursion of Lambda calls—just like in the real cloud.

## ✨ Key Features
- 🧠 **Smart Signaling:** Uses **Unix Domain Sockets (IPC)** for near-zero latency scaling pokes and an event-driven architecture.
- 🗄️ **Serialized State:** A dedicated **PGlite Server** process handles connection multiplexing, serializing queries to a single-writer DB to ensure absolute data integrity across multiple processes.
- 🛡️ **Self-Healing:** A dedicated supervisor monitors and restarts control plane components (Load Balancer, DNS, Scaler) if they crash.
- ⚡ **Connection-Aware:** Real-time monitoring of client connections ensures compute is never wasted on stale requests.
- 🐳 **Atomic Container Management:** Rapid scale-up and scale-down logic with **Ghost Container sync**—automatically reclaims leaked resources from previous crashes.
- 🔐 **Built-in Security:** Automatically generates and injects local CA certificates for seamless SSL/TLS interception.

## 🛠️ Installation & Setup

### 1. Prerequisites
- **Operating System:** Linux or macOS (Linux highly recommended for networking features).
- **Python 3.10+:** Required for structural pattern matching (`match` statements).
- **Docker Engine:** Must be installed and running.
- **Sudo / Root Privileges:** 
  - Required to bind to privileged ports (53, 443).
  - Required if your user is not in the `docker` group.

### 2. System-Level Dependencies
If you are installing dependencies manually (via `pip`), some packages like `cryptography` may require C-headers. Ensure your system has the following:
- **Debian/Ubuntu:** `sudo apt-get install libssl-dev libffi-dev python3-dev`
- **RHEL/CentOS:** `sudo yum install openssl-devel libffi-devel python3-devel`
- **Alpine:** `apk add gcc musl-dev python3-dev libffi-dev openssl-dev`

### 3. Execution Context
> [!IMPORTANT]
> **Run from Root:** You MUST execute `main.py` from the root of the `pie-lambda` repository. Relative paths for configuration and certificate generation are resolved based on the current working directory.

> [!TIP]
> **AWS Credentials:** While Pie-Lambda doesn't verify signatures, AWS SDKs still require credentials in the environment. You can use dummy values:
> `AWS_ACCESS_KEY_ID=testing`
> `AWS_SECRET_ACCESS_KEY=testing`
> `AWS_DEFAULT_REGION=us-east-1`

### 2. Configure your Functions
Create a `config.json` in the root directory:
```json
{
  "lambda_funcs_to_deploy": {
    "my-service": {
        "func_name": "my-service",
        "func_code_path": "./path/to/code",
        "func_handler_file_name": "handler.py",
        "lambda_handler_function_name": "lambda_handler"
    }
  }
}
```
> [!IMPORTANT]
> **Naming & Module Imports**
> If your Lambda code uses internal module imports (e.g., `from my_service import ...`), the **`func_name`** in your `config.json` must match the internal Python module structure. **The Nexus** uses this to set up the container environment.
> 
> **Avoid Naming Conflicts:** Do not name your handler file (`func_handler_file_name`) the same as your `func_name` (e.g., avoid `my-service.py` if the function is named `my-service`). Pie-Lambda creates a virtual package named after the function to support these absolute imports; naming your file the same can cause a recursive import loop.
> 
> *Note: Pie-Lambda includes a safety fallback that detects these collisions and switches to a standard import structure, but advanced virtual package features will be disabled for that specific function.*


### 3. CLI Commands
Use `main.py` to manage the project lifecycle. All commands require `sudo` if the control plane needs to bind to privileged ports (53, 443).

| Command | Description |
| :--- | :--- |
| `build` | **Initial Setup.** Creates the bridge network, builds the Control Plane base image, and builds all Lambda images defined in `config.json`. |
| `deploy` | **Start Service.** Stops any existing Control Plane container and starts a fresh one, mounting certs and config. |
| `rebuildlambdas` | **Fast Iteration.** Tears down and rebuilds ONLY the Lambda function images (useful when you change function code). |
| `runexisting` | **Quick Restart.** Restarts existing (stopped) containers without rebuilding images. |
| `shutdown` | **Stop.** Stops all running Pie-Lambda containers (non-destructive). |
| `teardowncontainers` | **Cleanup.** Stops and completely removes all active containers. |
| `teardownall` | **Factory Reset.** Removes all containers, Pie-Lambda images, and the custom Docker network. |

---

## 🔐 SSL & DNS: The "Magic" Explained

To achieve zero-code changes in your Lambda functions, Pie-Lambda performs two types of "impersonation":

### 1. DNS Hijacking
The internal DNS server (running on port 53) intercepts queries for `*.amazonaws.com`. Instead of going to the real AWS, these requests are routed to the **Control Plane Load Balancer**.
*   **Default:** Pie-Lambda creates a Docker bridge network and sets the Control Plane as the primary DNS for all worker containers.

### 2. SSL Interception (MITM)
Because the SDKs use HTTPS, Pie-Lambda generates a local **Certificate Authority (CA)** on every launch (or reuse).
- It signs a certificate for `lambda.amazonaws.com`.
- It bundles this local CA with the system's standard CA bundle (`certifi`).
- This bundle is mounted into every Lambda container and set via the `AWS_CA_BUNDLE` environment variable.
- Result: Your `boto3` client "trusts" the Control Plane as if it were the real AWS.

#### 🗝️ Certificate Management
On the first run, Pie-Lambda generates a `certs/` directory containing the keys and certificates required for MITM interception:
- `ca.crt` / `ca.key`: Your local Certificate Authority.
- `server.crt` / `server.key`: Certificates for the Load Balancer.

**Lifecycle & Rotation:** Generated certificates carry a 1-year validity period. **Note: Rotation is not automatic.** If you need to refresh the environment or handle credential mismatches, removing the `certs/` directory forces the control plane to re-initialize its local CA and server certificates during startup.

To enable SSL verification in your own external containers, you must mount the CA certificate and set the `AWS_CA_BUNDLE` environment variable:

```bash
docker run -d \
  -v $(pwd)/certs:/etc/ssl/certs \
  -e AWS_CA_BUNDLE=/etc/ssl/certs/ca.crt \
  --dns <CONTROL_PLANE_IP> \
  --network lambda_bridge \
  my-app
```

> [!TIP]
> If you set `do_ssl: false` in `config.json`, the Load Balancer will bypass SSL interception and listen on port **444** (plain HTTP).


---

## 🌐 Advanced Networking Configuration

### Transparent DNS (Default)
In this mode, you don't even need to tell your code it's running locally.
```python
import boto3
client = boto3.client('lambda') # Works automatically!
```
The Worker containers are started with `--dns` pointing to the Control Plane.

### Direct IP / Load Balancer Mode (No DNS Hijacking)
If you prefer not to use DNS hijacking (or for testing from the host machine), you can point your clients directly to the Control Plane's IP.

1.  **Get the Control Plane IP:** Check it via `docker inspect pie-lambda-control-plane`.
2.  **Configure Boto3:**
    ```python
    import boto3
    # Use the Control Plane IP directly
    client = boto3.client(
        'lambda', 
        endpoint_url='https://<CONTROL_PLANE_IP>', 
        verify='/path/to/pie-lambda/certs/ca.crt' # Must trust the local CA
    )
    ```

### 🐳 Integrating Your Containers (Transparent DNS)

To make an external container (like your main app or a sidecar) use Pie-Lambda's transparent interception, follow these steps:

1.  **Retrieve Control Plane IP:**
    ```bash
    CONTROL_PLANE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' pie-lambda-control-plane)
    ```

2.  **Launch Your Container:**
    Run your container with the `--dns` flag pointing to the Control Plane and attach it to the `lambda_bridge` network.
    ```bash
    docker run -d \
      --name my-app \
      --network lambda_bridge \
      --dns $CONTROL_PLANE_IP \
      my-app-image
    ```


### 📦 Dependencies & Environment
Pie-Lambda automatically handles dependencies and environment variables for your functions during the `build` phase:

- **`.env`**: Place a `.env` file in your `func_code_path` to inject environment variables into the Lambda container.
- **`requirements.txt`**: Standard Python requirements file in your `func_code_path` will be automatically installed during the Docker build.
- **Lambda Layers**:native Layer support can be easily workaround by including any "Layer" dependencies directly in your `requirements.txt`.


> [!TIP]
> If you update your `requirements.txt` or `.env`, remember to run `python3 main.py rebuildlambdas` to apply the changes to your container images.


#### 🛠️ Accessing Localhost Services
If your application needs to talk back to a service running on your **host machine** (outside Docker), use the magic domain:
- **Host:** `pie-lambda.local`
- **Port:** Use the same port as your host service.

The Control Plane's DNS automatically resolves `pie-lambda.local` to the host's bridge gateway.

#### 🌐 External Traffic
Don't worry about standard internet traffic; Pie-Lambda's DNS is recursive. Requests to `google.com` or other external domains are automatically forwarded to the outside internet.

---

## ⚙️ Configuration Reference (`config.json`)
The `config.json` file is the central source of truth for your local Lambda environment. By default, `main.py` looks for this file in the project root. Refer to `config.json.template` for a complete list of all default values.

> [!NOTE]
> You can specify a custom configuration path using the `--config` flag.
> 
> **Important:** Any changes made to `config.json` require a control plane redeployment (`python3 main.py deploy`) to take effect.


| Field | Type | Description |
| :--- | :--- | :--- |
| `lambda_funcs_to_deploy` | Object | Map of lambda function configurations. |
| `enable_internal_logging` | Boolean | Enables verbose debug logging for all Control Plane components. |
| `global_scale_limit` | Integer | Max concurrent containers **per lambda type**. |
| `lambda_timeout_mins` | Integer | Max execution time for `RequestResponse` invocations. |
| `db_type` | String | Storage backend: `disk` (persistent) or `memory` (ephemeral). Default is `memory`|
| `db_path` | String | Path to the database file (required if `db_type` is `disk`). |
| `do_ssl` | Boolean | Toggles SSL interception. `true` = Port 443 (HTTPS), `false` = Port 444 (HTTP). |
| `lambda_default_region` | String | Default AWS region injected into worker containers. |

### Scaling Settings
- `available_container_scale_down_time`: Seconds an idle container stays alive before scaling down.
- `busy_container_scale_down_time`: Seconds a container stays reserved for a request before returning to the pool.
- `provisioning_container_scale_down_time`: Seconds to wait before cleaning up failed provisioning attempts.
- `docker_sdk_check_interval_mins`: Interval for the scaler to sync with the Docker daemon.
- `created_container_stuck_time_mins`: Threshold to detect containers that failed to reach "Running" state.

### Reliability & Watchdog
- `retry_event_count`: Max retries for `InvocationType: Event` requests.
- `rr_stuck_time`: Minutes after which a `RequestResponse` call is marked as abandoned.
- `event_stuck_time`: Minutes after which an `Event` type request is marked as abandoned.
- `watchdog_loop_time`: Interval (seconds) for health checks.
- `process_kill_time`: Seconds to wait for a component to stop before forcing a kill.
- `watchdog_failure_limit`: Number of consecutive failures before the watchdog gives up on a component.


---

### AWS Lambda Parallels:

| Pie-Lambda Component | AWS Lambda Equivalent | Description |
| :--- | :--- | :--- |

| **Control Plane** | **AWS Lambda Service** | Manages the lifecycle of all Lambda functions. |
| **Worker Container** | **Lambda Execution Environment** | The isolated environment where your function code runs. |
| **Scaler** | **AWS Auto Scaling** | Automatically adjusts the number of running containers based on traffic. |
| **Load Balancer** | **AWS Application Load Balancer (ALB)** | Distributes incoming requests across healthy worker containers. |
| **DNS Server** | **AWS Route 53** | Resolves domain names to IP addresses. |
| **Supervisor** | **AWS Health Dashboard** | Monitors the health of all components and restarts them if they fail. |


### 🧠 Expected Behavior

#### 🔄 Cloud Parity
- **At-Least-Once Execution:** Like real AWS Lambda, there is a small chance of duplicate executions. We recommend using **idempotent functions**.
- **Execution Order:** The order of `Event` type (asynchronous) invocations is not guaranteed.

### 🏗️ Internal Architecture & Communication

Pie-Lambda is designed as a distributed system of micro-services running on your local machine.

#### 🔌 Logic Flow & Protocols
1.  **Transparent DNS**: Intercepts `lambda.*amazonaws.*com.*` and redirects traffic to the **Load Balancer**.
2.  **Load Balancer (The Dispatcher)**: 
    - **Synchronous (`RequestResponse`)**: Calls **The Nexus** API directly for immediate execution.
    - **Asynchronous (`Event`)**: Commits the request to the **PGlite DB** (acting as a local SQS) and returns an immediate `202 Accepted`.
3.  **Event Queue Worker (The Consumer)**: 
    - Continually polls the PGlite DB for pending events.
    - Proxies the execution to **The Nexus's** internal API.
4.  **The Nexus (The Core)**: The central hub of the control plane. It implements the **Lambda Runtime Interface API** (RIE), manages worker long-polling (`/next`), and pokes the Scaler over high-speed **Unix Domain Sockets (IPC)**.
5.  **The Muscle (Scaler)**: Responds to IPC pokes to manage the Docker lifecycle of worker containers.

#### 🗄️ The Data Backbone: PGlite Multiplexer
To prevent lock contention and race conditions in a multi-process environment, Pie-Lambda does not access the database directly from every component. 
- A standalone **PGlite Server** process owns the database.
- All components connect to this server, which **multiplexes and serializes** queries.
- This ensures a **Single-Writer** pattern for the PGlite DB, guaranteeing atomic state changes across DNS, LB, and **The Nexus** components.

#### 🛰️ Port Mapping
| Port | Purpose | Protocol |
| :--- | :--- | :--- |
| **53** | **DNS Server** | UDP/TCP (Hijacking) |
| **443** | **Control Plane (SSL)** | HTTPS (Public Service API) |
| **444** | **Control Plane (Non-SSL)** | HTTP (Debug API) |
| **80** | **Runtime API** | HTTP (Internal RIE via **The Nexus**) |
| **UDS** | **Unix Domain Sockets** | IPC (**The Nexus** -> Scaler) |



## 🛡️ Excellence, Not Perfection
Pie-Lambda was built for developers who need a **snappy**, **reliable** environment for local cloud development without the overhead of mocking the entire AWS ecosystem.

*Chasing excellence, one microsecond at a time.*
