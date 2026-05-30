import os
import json
import time
from pathlib import Path
from py_pglite import PGliteManager, PGliteConfig

def run_db():
    work_dir = Path("./pgdata")
    work_dir.mkdir(parents=True, exist_ok=True)
    
    # --- WORKAROUND START: Pin versions to support require() ---
    package_json = work_dir / "package.json"
    if not package_json.exists():
        with open(package_json, "w") as f:
            json.dump({
                "name": "py-pglite-env",
                "dependencies": {
                    "@electric-sql/pglite": "0.1.5",
                    "@electric-sql/pglite-socket": "0.0.5"
                }
            }, f, indent=2)
    # --- WORKAROUND END ---

    config = PGliteConfig(
        use_tcp=True, 
        tcp_port=6957, 
        work_dir=work_dir
    )
    manager = PGliteManager(config)
    
    print("Starting PGlite server with pinned CJS-compatible versions...")
    manager.start()
    
    try:
        print(f"Success! Connection string: {manager.get_connection_string()}")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()

if __name__ == "__main__":
    run_db()
