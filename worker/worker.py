import os, time, json, requests, socket, random

MASTER_URL = os.environ["MASTER_URL"].rstrip("/")
WORKER_ID = os.environ["WORKER_ID"]
PORT = int(os.environ.get("WORKER_PORT", "9000"))
CAPACITY = int(os.environ.get("WORKER_CAPACITY", "1"))
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))

def ip_guess():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "0.0.0.0"

def register():
    payload = {
        "worker_id": WORKER_ID,
        "ip": os.environ.get("WORKER_IP", ip_guess()),
        "port": PORT,
        "capacity": CAPACITY
    }
    r = requests.post(f"{MASTER_URL}/register", json=payload, timeout=5)
    r.raise_for_status()
    print("REGISTER:", r.json(), flush=True)

def heartbeat_loop():
    while True:
        payload = {
            "worker_id": WORKER_ID,
            "running_tasks": 0,
            "cpu": round(random.uniform(0.05, 0.35), 2),
            "mem": round(random.uniform(0.05, 0.45), 2),
        }
        try:
            r = requests.post(f"{MASTER_URL}/heartbeat", json=payload, timeout=5)
            print("HEARTBEAT:", r.status_code, r.text, flush=True)
        except Exception as e:
            print("HEARTBEAT ERROR:", e, flush=True)
        time.sleep(HEARTBEAT_INTERVAL)

if __name__ == "__main__":
    # Espera breve por si el master aún no arrancó
    time.sleep(2)
    register()
    heartbeat_loop()