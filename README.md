# 🥧 Pie-Lambda

**The Snappy, Transparent, and Recursive AWS Lambda Emulator.**

Pie-Lambda is a high-performance local control plane that allows you to run complex AWS Lambda architectures on your local machine with **zero code changes**. Unlike standard emulators that require you to override `endpoint_url` in every Boto3 client, Pie-Lambda uses **Transparent DNS Hijacking** to intercept and route AWS traffic automatically.

> [!NOTE]
> Currently, Pie-Lambda supports **Python** runtimes and intercepts any traffic matching the pattern: `lambda.*amazonaws.*com.*`


## 🚀 The "Killer Feature": Recursive Execution
In most local environments, if `Lambda A` tries to invoke `Lambda B` via the AWS SDK, the call fails or requires a complex networking setup. 

**Pie-Lambda solves this.** Because it intercepts traffic at the DNS level, your Boto3/SDK calls within a container are automatically routed back to the control plane, allowing for seamless, infinite recursion of Lambda calls—just like in the real cloud.

## ✨ Key Features
- 🧠 **Smart Signaling:** Uses `asyncio` event-driven IPC for near-zero latency (no sluggish DB polling).
- 🛡️ **Self-Healing:** A dedicated supervisor monitors and restarts control plane components (Load Balancer, DNS, Scaler) if they crash.
- ⚡ **Connection-Aware:** Real-time monitoring of client connections ensures compute is never wasted on stale requests.
- 🐳 **Atomic Container Management:** Rapid scale-up and scale-down logic with **Ghost Container sync**—automatically reclaims leaked resources from previous crashes.
- 🔐 **Built-in Security:** Automatically generates and injects local CA certificates for seamless SSL/TLS interception.

## 🛠️ Installation & Setup

### 1. Prerequisites
- **Linux/macOS** (Linux recommended for the best Docker bridge experience).
- **Python 3.11+**
- **Docker** installed and running.
- **Sudo Access:** Required to bind to privileged ports (53, 80, 443).

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
> If your Lambda code uses internal module imports (e.g., `from my_service import ...`), the `func_name` defined in `config.json` **must exactly match** the module name used in your code. Pie-Lambda uses this name to structure the container's environment; a mismatch will cause a `ModuleNotFoundError` at runtime.


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
| `db_type` | String | Storage backend: `disk` (persistent) or `memory` (ephemeral). |
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

### 🏗️ Under the Hood
Pie-Lambda uses high-fidelity emulation by leveraging official AWS Lambda base images. It implements the **Lambda Runtime API** directly, allowing it to interface with existing Lambda Runtimes without any modifications to your code.

#### 🔌 Internal Routing & Ports
Pie-Lambda orchestrates several layers of communication to achieve zero-latency transparency:

| Port | Purpose | Scope |
| :--- | :--- | :--- |
| **53** | **DNS Server** | Hijacks `*.amazonaws.com` requests. |
| **443** | **Control Plane (SSL)** | Emulates the public AWS Lambda Service API (Port 443). |
| **444** | **Control Plane (Non-SSL)** | Used when `do_ssl: false` for easier local debugging. |
| **80** | **Runtime API** | Internal endpoint for the Lambda Runtime Interface (RIE). |
| **UDS** | **Unix Domain Sockets** | High-speed IPC for signaling between Control Plane sub-processes. |



## 🛡️ Excellence, Not Perfection
Pie-Lambda was built for developers who need a **snappy**, **reliable** environment for local cloud development without the overhead of mocking the entire AWS ecosystem.

*Chasing excellence, one microsecond at a time.*
