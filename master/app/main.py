import uuid
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Optional, List
from .settings import settings

app = FastAPI(title="GridMR Master")

class JobRequest(BaseModel):
    script_url: str  # p.ej. http(s)://, file:// o nfs:// (en MVP usaremos http/file)
    input_url: str
    partitions: int = 3

class JobStatus(BaseModel):
    job_id: str
    status: str
    message: Optional[str] = None
    outputs: Optional[List[str]] = None

# Estado mínimo en memoria para el MVP
JOBS: Dict[str, JobStatus] = {}
WORKERS: Dict[str, dict] = {}  # worker_id -> {address, last_seen}

@app.get("/health")
def health():
    return {"ok": True, "shared_dir": settings.SHARED_DIR}

@app.post("/jobs", response_model=JobStatus)
def submit_job(req: JobRequest):
    job_id = str(uuid.uuid4())
    # En el siguiente paso descargaremos a /data/shared/jobs/<job_id>/
    status = JobStatus(job_id=job_id, status="QUEUED")
    JOBS[job_id] = status
    return status

@app.get("/jobs/{job_id}", response_model=JobStatus)
def get_job(job_id: str):
    return JOBS.get(job_id, JobStatus(job_id=job_id, status="UNKNOWN", message="not found"))

@app.post("/workers/register")
def register_worker(worker_id: str, address: str):
    WORKERS[worker_id] = {"address": address}
    return {"ok": True, "count": len(WORKERS)}
