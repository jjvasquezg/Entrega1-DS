from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict
import time

app = FastAPI()
workers: Dict[str, dict] = {}

class WorkerRegister(BaseModel):
    worker_id: str
    ip: str
    port: int
    capacity: int

class Heartbeat(BaseModel):
    worker_id: str
    running_tasks: int
    cpu: float
    mem: float

@app.get("/health")
def health():
    return {"status": "ok", "node": "master"}

@app.post("/register")
def register(worker: WorkerRegister):
    workers[worker.worker_id] = {
        "ip": worker.ip,
        "port": worker.port,
        "capacity": worker.capacity,
        "last_seen": time.time(),
        "status": "ACTIVE",
        "running_tasks": 0,
        "cpu": 0.0,
        "mem": 0.0,
    }
    return {"status": "registered", "worker_id": worker.worker_id}

@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    if hb.worker_id not in workers:
        return {"status": "error", "msg": "Worker not registered"}
    w = workers[hb.worker_id]
    w["last_seen"] = time.time()
    w["running_tasks"] = hb.running_tasks
    w["cpu"] = hb.cpu
    w["mem"] = hb.mem
    w["status"] = "ACTIVE"
    return {"status": "ok"}

@app.get("/workers")
def list_workers():
    now = time.time()
    for wid, data in workers.items():
        if now - data["last_seen"] > 15:
            workers[wid]["status"] = "SUSPECT"
    return workers