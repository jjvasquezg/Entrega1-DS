import os
import time
import uuid
import asyncio
import threading
from collections import deque, defaultdict
from enum import Enum
from typing import Dict, Optional, List

import grpc
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from .settings import settings
from .storage import (
    job_paths, ensure_dir, download_to_file,
    file_size, partition_input_by_lines, concat_files
)

# Stubs gRPC
from master.app import mapreduce_pb2 as pb2
from master.app import mapreduce_pb2_grpc as pb2_grpc

app = FastAPI(title="GridMR Master")

# ---------- Modelos ----------
class JobRequest(BaseModel):
    script_url: str
    input_url: str
    partitions: int = 6  # usado solo si entra en modo DISTRIBUTED

class JobMode(str, Enum):
    SINGLE = "SINGLE"
    DISTRIBUTED = "DISTRIBUTED"

class JobState(str, Enum):
    QUEUED      = "QUEUED"
    PREPARING   = "PREPARING"
    PREPARED    = "PREPARED"
    MAPPING     = "MAPPING"
    REDUCING    = "REDUCING"
    RUNNING     = "RUNNING"      # SINGLE
    SUCCEEDED   = "SUCCEEDED"
    FAILED      = "FAILED"
    UNKNOWN     = "UNKNOWN"

class JobStatus(BaseModel):
    job_id: str
    status: str
    mode: JobMode | None = None
    message: Optional[str] = None
    outputs: Optional[List[str]] = None
    script_path: Optional[str] = None
    input_path: Optional[str] = None
    chunks_dir: Optional[str] = None
    shuffle_dir: Optional[str] = None
    partitions: Optional[int] = None

# ---------- Estado ----------
JOBS: Dict[str, JobStatus] = {}
WORKER_INFO: Dict[str, dict] = {}   # worker_id -> {address, last_seen}
WORKER_RING = deque()               # addresses RR
LOCK = threading.Lock()

# Colas
JOB_QUEUE_SINGLE = deque()          # job_ids en modo SINGLE
MAP_QUEUE = deque()                 # (job_id, chunk_path, chunk_id)
REDUCE_QUEUE = deque()              # (job_id, partition_id, files)

# Progreso de distributed
MAP_RESULTS: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
# MAP_RESULTS[job_id][chunk_id] -> list(paths por partición generados por ese chunk)
PENDING_MAPS: Dict[str, int] = {}
PENDING_REDUCES: Dict[str, int] = {}

# ---------- API ----------
@app.get("/health")
def health():
    return {
        "ok": True, "shared_dir": settings.SHARED_DIR,
        "threshold": settings.FILE_PARTITION_THRESHOLD_BYTES
    }

@app.post("/jobs", response_model=JobStatus)
def submit_job(req: JobRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    st = JobStatus(job_id=job_id, status=JobState.QUEUED)
    JOBS[job_id] = st
    background_tasks.add_task(prepare_job, job_id, req)
    return st

@app.get("/jobs/{job_id}", response_model=JobStatus)
def get_job(job_id: str):
    return JOBS.get(job_id, JobStatus(job_id=job_id, status=JobState.UNKNOWN, message="not found"))

@app.get("/jobs/{job_id}/ls")
def list_job_files(job_id: str):
    st = JOBS.get(job_id)
    if not st:
        raise HTTPException(status_code=404, detail="job not found")
    paths = job_paths(settings.SHARED_DIR, job_id)
    def safe_ls(p):
        try: return sorted(os.listdir(p))
        except Exception: return []
    return {
        "base": paths["base"],
        "script": st.script_path,
        "input": st.input_path,
        "chunks": safe_ls(paths["chunks"]),
        "shuffle": safe_ls(paths["shuffle"]),
        "out": safe_ls(paths["out"]),
        "status": st.status,
        "mode": st.mode,
        "message": st.message,
        "partitions": st.partitions,
    }

@app.delete("/jobs/{job_id}")
def delete_job(job_id: str):
    if job_id in JOBS:
        del JOBS[job_id]
        # Limpieza de estructuras opcional
        MAP_RESULTS.pop(job_id, None)
        PENDING_MAPS.pop(job_id, None)
        PENDING_REDUCES.pop(job_id, None)
        return {"job_id": job_id, "deleted": True}
    return {"job_id": job_id, "deleted": False, "reason": "not found"}

@app.post("/workers/register")
def register_worker(worker_id: str, address: str):
    with LOCK:
        WORKER_INFO[worker_id] = {"address": address, "last_seen": time.time()}
        if address not in WORKER_RING:
            WORKER_RING.append(address)
    return {"ok": True, "count": len(WORKER_INFO), "ring": list(WORKER_RING)}

@app.get("/workers")
def list_workers():
    return {"workers": WORKER_INFO, "ring": list(WORKER_RING)}

# ---------- Preparación ----------
async def _prepare_async(job_id: str, req: JobRequest):
    st = JOBS[job_id]
    st.status = JobState.PREPARING

    paths = job_paths(settings.SHARED_DIR, job_id)
    ensure_dir(paths["base"]); ensure_dir(paths["input_dir"])
    ensure_dir(paths["tmp"]);  ensure_dir(paths["out"])
    ensure_dir(paths["shuffle"]); ensure_dir(paths["chunks"])

    await download_to_file(req.script_url, paths["script"], settings.SHARED_DIR)
    await download_to_file(req.input_url,  paths["input_file"], settings.SHARED_DIR)

    size = file_size(paths["input_file"])
    partitions = max(1, req.partitions or settings.DEFAULT_PARTITIONS)

    # Decidir modo
    if size >= settings.FILE_PARTITION_THRESHOLD_BYTES and partitions > 1:
        # DISTRIBUTED
        st.mode = JobMode.DISTRIBUTED
        st.partitions = partitions
        chunk_files = partition_input_by_lines(paths["input_file"], paths["chunks"], partitions)
        # Encolar maps por chunk
        with LOCK:
            for idx, chunk in enumerate(sorted(chunk_files)):
                MAP_QUEUE.append((job_id, chunk, f"{idx:04d}"))
            PENDING_MAPS[job_id] = len(chunk_files)
        st.status = JobState.PREPARED
    else:
        # SINGLE
        st.mode = JobMode.SINGLE
        st.partitions = 1
        with LOCK:
            JOB_QUEUE_SINGLE.append(job_id)
        st.status = JobState.PREPARED

    st.script_path = paths["script"]
    st.input_path = paths["input_file"]
    st.chunks_dir = (paths["chunks"] if st.mode == JobMode.DISTRIBUTED else None)
    st.shuffle_dir = (paths["shuffle"] if st.mode == JobMode.DISTRIBUTED else None)

def prepare_job(job_id: str, req: JobRequest):
    try:
        asyncio.run(_prepare_async(job_id, req))
    except Exception as e:
        st = JOBS.get(job_id)
        if st:
            st.status = JobState.FAILED
            st.message = str(e)

# ---------- Dispatch helpers ----------
def _take_worker_rr() -> Optional[str]:
    with LOCK:
        if not WORKER_RING:
            return None
        addr = WORKER_RING[0]
        WORKER_RING.rotate(-1)
        return addr

def _dispatch_single(job_id: str, worker_addr: str):
    st = JOBS[job_id]
    paths = job_paths(settings.SHARED_DIR, job_id)
    st.status = JobState.RUNNING
    with grpc.insecure_channel(worker_addr) as channel:
        stub = pb2_grpc.WorkerStub(channel)
        req = pb2.JobTask(
            job_id=job_id,
            script_path=paths["script"],
            input_path=paths["input_file"],
            output_dir=paths["out"],
        )
        resp: pb2.JobResult = stub.ExecuteJob(req, timeout=settings.GRPC_TIMEOUT_S)
    if resp.success:
        st.status = JobState.SUCCEEDED
        st.outputs = [resp.output_path]
        st.message = resp.log or "ok"
    else:
        st.status = JobState.FAILED
        st.message = resp.error or "worker failed"

def _dispatch_map(job_id: str, chunk_path: str, chunk_id: str, worker_addr: str):
    st = JOBS[job_id]
    st.status = JobState.MAPPING
    paths = job_paths(settings.SHARED_DIR, job_id)
    with grpc.insecure_channel(worker_addr) as channel:
        stub = pb2_grpc.WorkerStub(channel)
        req = pb2.MapTask(
            job_id=job_id,
            script_path=paths["script"],
            input_chunk=chunk_path,
            shuffle_dir=paths["shuffle"],
            reduce_partitions=st.partitions or settings.DEFAULT_PARTITIONS,
            chunk_id=chunk_id,
        )
        resp: pb2.MapResult = stub.ExecuteMap(req, timeout=settings.GRPC_TIMEOUT_S)
    if not resp.success:
        st.status = JobState.FAILED
        st.message = resp.error or f"map failed on chunk {chunk_id}"
        return False
    # Guardar los archivos intermedios que reportó el worker
    MAP_RESULTS[job_id][resp.chunk_id] = list(resp.partition_files)
    # Decrementar pendientes y, si llega a cero, encolar reduce por partición
    done = False
    with LOCK:
        PENDING_MAPS[job_id] -= 1
        if PENDING_MAPS[job_id] == 0:
            # Construir inputs por partición
            parts = st.partitions or settings.DEFAULT_PARTITIONS
            by_partition: Dict[int, List[str]] = {i: [] for i in range(parts)}
            # Cada MapResult devuelve N archivos (uno por partición) -> los agrupamos
            for chunk, part_files in MAP_RESULTS[job_id].items():
                for p, file in enumerate(part_files):
                    by_partition[p].append(file)
            for p in range(parts):
                REDUCE_QUEUE.append((job_id, p, by_partition[p]))
            PENDING_REDUCES[job_id] = parts
            done = True
    return True

def _dispatch_reduce(job_id: str, partition_id: int, files: List[str], worker_addr: str):
    st = JOBS[job_id]
    st.status = JobState.REDUCING
    paths = job_paths(settings.SHARED_DIR, job_id)
    output_path = os.path.join(paths["out"], f"part-{partition_id:04d}")
    with grpc.insecure_channel(worker_addr) as channel:
        stub = pb2_grpc.WorkerStub(channel)
        req = pb2.ReduceTask(
            job_id=job_id,
            script_path=paths["script"],
            partition_id=partition_id,
            shuffle_inputs=files,
            output_path=output_path,
        )
        resp: pb2.ReduceResult = stub.ExecuteReduce(req, timeout=settings.GRPC_TIMEOUT_S)
    if not resp.success:
        st.status = JobState.FAILED
        st.message = resp.error or f"reduce failed on partition {partition_id}"
        return False
    # Contar reduces restantes; si 0 -> consolidar result.txt
    finished = False
    with LOCK:
        PENDING_REDUCES[job_id] -= 1
        if PENDING_REDUCES[job_id] == 0:
            parts = sorted([os.path.join(paths["out"], f) for f in os.listdir(paths["out"]) if f.startswith("part-")])
            concat_files(parts, paths["result"])
            st.status = JobState.SUCCEEDED
            st.outputs = [paths["result"]]
            st.message = "ok"
            finished = True
    return finished

# ---------- Scheduler principal ----------
def scheduler_loop():
    while True:
        try:
            # SINGLE first
            job_id = None
            with LOCK:
                if JOB_QUEUE_SINGLE:
                    job_id = JOB_QUEUE_SINGLE.popleft()
            if job_id:
                addr = _take_worker_rr()
                if addr: _dispatch_single(job_id, addr)
                else:
                    # no workers, re-enqueue
                    with LOCK: JOB_QUEUE_SINGLE.appendleft(job_id)
                    time.sleep(0.5)
                continue

            # MAP queue
            task = None
            with LOCK:
                if MAP_QUEUE:
                    task = MAP_QUEUE.popleft()
            if task:
                job_id, chunk_path, chunk_id = task
                addr = _take_worker_rr()
                if addr:
                    _dispatch_map(job_id, chunk_path, chunk_id, addr)
                else:
                    with LOCK: MAP_QUEUE.appendleft(task)
                    time.sleep(0.5)
                continue

            # REDUCE queue
            rtask = None
            with LOCK:
                if REDUCE_QUEUE:
                    rtask = REDUCE_QUEUE.popleft()
            if rtask:
                job_id, pid, files = rtask
                addr = _take_worker_rr()
                if addr:
                    _dispatch_reduce(job_id, pid, files, addr)
                else:
                    with LOCK: REDUCE_QUEUE.appendleft(rtask)
                    time.sleep(0.5)
                continue

            time.sleep(0.2)
        except Exception:
            time.sleep(0.5)

@app.on_event("startup")
def start_scheduler():
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
