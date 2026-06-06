# 🥧 Pie-Lambda

**The Snappy, Transparent, and Recursive AWS Lambda Emulator.**

Pie-Lambda is a high-performance local control plane that allows you to run complex AWS Lambda architectures on your local machine with **zero code changes**. Unlike standard emulators that require you to override `endpoint_url` in every Boto3 client, Pie-Lambda uses **Transparent DNS Hijacking** to intercept and route AWS traffic automatically.

## 🚀 The "Killer Feature": Recursive Execution
In most local environments, if `Lambda A` tries to invoke `Lambda B` via the AWS SDK, the call fails or requires a complex networking setup. 

**Pie-Lambda solves this.** Because it intercepts traffic at the DNS level, your Boto3/SDK calls within a container are automatically routed back to the control plane, allowing for seamless, infinite recursion of Lambda calls—just like in the real cloud.

## ✨ Key Features
- 🧠 **Smart Signaling:** Uses `asyncio` event-driven IPC for near-zero latency (no sluggish DB polling).
- 🛡️ **Self-Healing:** A dedicated supervisor monitors and restarts control plane components (Load Balancer, DNS, Scaler) if they crash.
- ⚡ **Connection-Aware:** Real-time monitoring of client connections ensures compute is never wasted on stale requests.
- 🐳 **Atomic Container Management:** Rapid scale-up and scale-down logic with "Ghost Container" detection to keep your Docker environment clean.
- 🔐 **Built-in Security:** Automatically generates and injects local CA certificates for seamless SSL/TLS interception.

## 🛠️ Installation & Setup

### 1. Prerequisites
- **Linux/macOS** (Linux recommended for the best Docker bridge experience).
- **Python 3.11+**
- **Docker** installed and running.
- **Sudo Access:** Required to bind to privileged ports (53, 80, 443).

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

#### 🛠️ Accessing Localhost Services
If your application needs to talk back to a service running on your **host machine** (outside Docker), use the magic domain:
- **Host:** `pie-lambda.local`
- **Port:** Use the same port as your host service.

The Control Plane's DNS automatically resolves `pie-lambda.local` to the host's bridge gateway.

#### 🌐 External Traffic
Don't worry about standard internet traffic; Pie-Lambda's DNS is recursive. Requests to `google.com` or other external domains are automatically forwarded to the outside internet.

---

## ⚙️ Configuration Reference (`config.json`)

The `config.json` file is the central source of truth for your local Lambda environment. By default, `main.py` looks for this file in the project root.

> [!NOTE]
> You can specify a custom configuration path using the `--config` flag.
> 
> **Important:** Any changes made to `config.json` require a control plane redeployment (`python3 main.py deploy`) to take effect.


| Field | Type | Description |
| :--- | :--- | :--- |
| `lambda_funcs_to_deploy` | Object | Map of lambda function configurations. |
| `enable_internal_logging` | Boolean | Enables verbose debug logging for all CP components. |
| `global_scale_limit` | Integer | Maximum number of concurrent lambda containers (all types). Note: for each type of lambda, there will be a separate pool of containers with global_scale_limit applied separately to each of them. for example you can have 3 lambdas with global_scale_limit number of containers deployed for each of them. |

| `lambda_timeout_mins` | Integer | Max execution time for `RequestResponse` invocations. |
| `db_type` | String | Storage type: `disk` (persistent) or `memory` (ephemeral). |

### Scaling Settings
- `available_container_scale_down_time`: Seconds an idle container stays alive before scaling down.
- `busy_container_scale_down_time`: Seconds a container stays reserved for a request before returning to the pool.
- `provisioning_container_scale_down_time`: Seconds to wait before cleaning up failed provisioning attempts.
- `created_container_stuck_time_mins`: Threshold to detect containers that failed to reach "Running" state.

### Reliability & Watchdog
- `retry_event_count`: Max retries for `InvocationType: Event` requests.
- `rr_stuck_time`: Minutes after which a `RequestResponse` call is marked as abandoned.
- `watchdog_loop_time`: Interval (seconds) for health checks.
- `watchdog_failure_limit`: Number of consecutive failures before the watchdog gives up on a component.

---

## 🛡️ Excellence, Not Perfection
Pie-Lambda was built for developers who need a **snappy**, **reliable** environment for local cloud development without the overhead of mocking the entire AWS ecosystem.

*Chasing excellence, one microsecond at a time.*
