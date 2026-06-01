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

### 3. Launch
```bash
# 1. Build the images
python main.py --command build

# 2. Deploy and start the Control Plane
python main.py --command deploy
```

## 📖 How it Works: The Magic Under the Hood

Pie-Lambda orchestrates five specialized components:
1. **DNS Server (Port 53):** Redirects `*.amazonaws.com` to your local IP.
2. **Load Balancer (Port 443):** Authenticates and queues incoming HTTPS requests.
3. **Scaler:** Manages the Docker lifecycle, spawning new workers as demand increases.
4. **Worker Manager (Port 80):** Bridges the AWS Runtime Interface (RIE) to your queued requests using high-speed async signaling.
5. **Event Handler:** Manages asynchronous background tasks (`InvocationType: Event`) with persistent retry logic.

---

## 🛡️ Excellence, Not Perfection
Pie-Lambda was built for developers who need a **snappy**, **reliable** environment for local cloud development without the overhead of mocking the entire AWS ecosystem.

*Chasing excellence, one microsecond at a time.*
