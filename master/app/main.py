import os
import time
import logging
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

jobid = 10000
i = 1

# --- Logging a stdout ---
LOG_LEVEL = os.getenv("MASTER_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [master] %(message)s"
)
log = logging.getLogger("gridmr-master")

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
    log.debug("health check")
    return {
        "ok": True, "shared_dir": settings.SHARED_DIR,
        "threshold": settings.FILE_PARTITION_THRESHOLD_BYTES
    }

@app.post("/jobs", response_model=JobStatus)
def submit_job(req: JobRequest, background_tasks: BackgroundTasks):
    job_id = "job_"+str(jobid+i)
    i+=1
    log.info("job submitted id=%s script=%s input=%s partitions=%s",
             job_id, req.script_url, req.input_url, req.partitions)
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
    log.info("worker registered id=%s addr=%s ring=%s", worker_id, address, list(WORKER_RING))
    return {"ok": True, "count": len(WORKER_INFO), "ring": list(WORKER_RING)}

@app.get("/workers")
def list_workers():
    return {"workers": WORKER_INFO, "ring": list(WORKER_RING)}

# ---------- Preparación ----------
async def _prepare_async(job_id: str, req: JobRequest):
    st = JOBS[job_id]
    st.status = JobState.PREPARING
    log.info("job preparing id=%s", job_id)

    paths = job_paths(settings.SHARED_DIR, job_id)
    ensure_dir(paths["base"]); ensure_dir(paths["input_dir"])
    ensure_dir(paths["tmp"]);  ensure_dir(paths["out"])
    ensure_dir(paths["shuffle"]); ensure_dir(paths["chunks"])

    log.debug("downloading script to %s", paths["script"])
    await download_to_file(req.script_url, paths["script"], settings.SHARED_DIR)
    log.debug("downloading input to %s", paths["input_file"])
    await download_to_file(req.input_url,  paths["input_file"], settings.SHARED_DIR)

    size = file_size(paths["input_file"])
    partitions = max(1, req.partitions or settings.DEFAULT_PARTITIONS)
    log.info("prepared files id=%s size=%d partitions=%d", job_id, size, partitions)

    if size >= settings.FILE_PARTITION_THRESHOLD_BYTES and partitions > 1:
        st.mode = JobMode.DISTRIBUTED
        st.partitions = partitions
        chunk_files = partition_input_by_lines(paths["input_file"], paths["chunks"], partitions)
        with LOCK:
            for idx, chunk in enumerate(sorted(chunk_files)):
                MAP_QUEUE.append((job_id, chunk, f"{idx:04d}"))
            PENDING_MAPS[job_id] = len(chunk_files)
        st.status = JobState.PREPARED
        log.info("job queued for MAP id=%s chunks=%d", job_id, len(chunk_files))
    else:
        st.mode = JobMode.SINGLE
        st.partitions = 1
        with LOCK:
            JOB_QUEUE_SINGLE.append(job_id)
        st.status = JobState.PREPARED
        log.info("job queued SINGLE id=%s", job_id)

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
    log.info("SINGLE start id=%s worker=%s", job_id, worker_addr)
    log.debug("paths script=%s input=%s out=%s", paths["script"], paths["input_file"], paths["out"])

    channel_opts = [
        ('grpc.keepalive_time_ms', 20000),
        ('grpc.keepalive_timeout_ms', 10000),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.keepalive_permit_without_calls', 1),
        ('grpc.max_send_message_length', 50 * 1024 * 1024),
        ('grpc.max_receive_message_length', 50 * 1024 * 1024),
    ]

    try:
        with grpc.insecure_channel(worker_addr, options=channel_opts) as channel:
            try:
                grpc.channel_ready_future(channel).result(timeout=3)
            except grpc.FutureTimeoutError:
                raise RuntimeError(f"channel not ready to {worker_addr}")

            stub = pb2_grpc.WorkerStub(channel)
            req = pb2.JobTask(
                job_id=job_id,
                script_path=paths["script"],
                input_path=paths["input_file"],
                output_dir=paths["out"],
            )
            log.debug("RPC ExecuteJob(timeout=%ss)", settings.GRPC_TIMEOUT_S)
            resp: pb2.JobResult = stub.ExecuteJob(req, timeout=settings.GRPC_TIMEOUT_S)

        log.info("SINGLE end id=%s ok=%s out=%s err=%s", job_id, resp.success, resp.output_path, resp.error)
        if resp.success:
            st.status = JobState.SUCCEEDED
            st.outputs = [resp.output_path]
            st.message = resp.log or "ok"
        else:
            st.status = JobState.FAILED
            st.message = resp.error or "worker failed"

    except grpc.RpcError as e:
        st.status = JobState.FAILED
        st.message = f"grpc {e.code().name}: {e.details()}"
        log.error("SINGLE grpc error id=%s code=%s details=%s", job_id, e.code().name, e.details())
    except Exception as e:
        st.status = JobState.FAILED
        st.message = f"dispatch error: {e}"
        log.error("SINGLE dispatch error id=%s err=%s", job_id, e)

def _dispatch_map(job_id: str, chunk_path: str, chunk_id: str, worker_addr: str):
    st = JOBS[job_id]
    st.status = JobState.MAPPING
    paths = job_paths(settings.SHARED_DIR, job_id)
    log.info("MAP start id=%s chunk=%s worker=%s", job_id, chunk_id, worker_addr)

    try:
        with grpc.insecure_channel(worker_addr) as channel:
            grpc.channel_ready_future(channel).result(timeout=3)
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
            st.message = resp.error or f"map failed chunk {chunk_id}"
            log.error("MAP fail id=%s chunk=%s err=%s", job_id, chunk_id, st.message)
            return False

        MAP_RESULTS[job_id][resp.chunk_id] = list(resp.partition_files)
        log.info("MAP end id=%s chunk=%s files=%d", job_id, chunk_id, len(resp.partition_files))

        with LOCK:
            PENDING_MAPS[job_id] -= 1
            if PENDING_MAPS[job_id] == 0:
                parts = st.partitions or settings.DEFAULT_PARTITIONS
                by_partition: Dict[int, List[str]] = {i: [] for i in range(parts)}
                for _, part_files in MAP_RESULTS[job_id].items():
                    for p, f in enumerate(part_files):
                        by_partition[p].append(f)
                for p in range(parts):
                    REDUCE_QUEUE.append((job_id, p, by_partition[p]))
                PENDING_REDUCES[job_id] = parts
                log.info("MAP all done id=%s -> queued %d reduces", job_id, parts)
        return True

    except Exception as e:
        st.status = JobState.FAILED
        st.message = f"map dispatch error: {e}"
        log.error("MAP dispatch error id=%s chunk=%s err=%s", job_id, chunk_id, e)
        return False


def _dispatch_reduce(job_id: str, partition_id: int, files: List[str], worker_addr: str):
    st = JOBS[job_id]
    st.status = JobState.REDUCING
    paths = job_paths(settings.SHARED_DIR, job_id)
    output_path = os.path.join(paths["out"], f"part-{partition_id:04d}")
    log.info("REDUCE start id=%s part=%s inputs=%d worker=%s", job_id, partition_id, len(files), worker_addr)

    try:
        with grpc.insecure_channel(worker_addr) as channel:
            grpc.channel_ready_future(channel).result(timeout=3)
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
            st.message = resp.error or f"reduce failed part {partition_id}"
            log.error("REDUCE fail id=%s part=%s err=%s", job_id, partition_id, st.message)
            return False

        log.info("REDUCE end id=%s part=%s out=%s", job_id, partition_id, resp.output_path)

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
                log.info("JOB SUCCEEDED id=%s result=%s", job_id, paths["result"])
        return finished

    except Exception as e:
        st.status = JobState.FAILED
        st.message = f"reduce dispatch error: {e}"
        log.error("REDUCE dispatch error id=%s part=%s err=%s", job_id, partition_id, e)
        return False


# ---------- Scheduler principal ----------
def scheduler_loop():
    last_tick = 0
    while True:
        try:
            # SINGLE
            job_id = None
            with LOCK:
                if JOB_QUEUE_SINGLE:
                    job_id = JOB_QUEUE_SINGLE.popleft()
            if job_id:
                addr = _take_worker_rr()
                if addr:
                    log.info("dispatch SINGLE id=%s -> %s", job_id, addr)
                    _dispatch_single(job_id, addr)
                else:
                    log.warning("no workers available; requeue SINGLE id=%s", job_id)
                    with LOCK: JOB_QUEUE_SINGLE.appendleft(job_id)
                    time.sleep(0.5)
                continue

            # MAP
            task = None
            with LOCK:
                if MAP_QUEUE:
                    task = MAP_QUEUE.popleft()
            if task:
                job_id, chunk_path, chunk_id = task
                addr = _take_worker_rr()
                if addr:
                    log.info("dispatch MAP id=%s chunk=%s -> %s", job_id, chunk_id, addr)
                    _dispatch_map(job_id, chunk_path, chunk_id, addr)
                else:
                    log.warning("no workers available; requeue MAP id=%s chunk=%s", job_id, chunk_id)
                    with LOCK: MAP_QUEUE.appendleft(task)
                    time.sleep(0.5)
                continue

            # REDUCE
            rtask = None
            with LOCK:
                if REDUCE_QUEUE:
                    rtask = REDUCE_QUEUE.popleft()
            if rtask:
                job_id, pid, files = rtask
                addr = _take_worker_rr()
                if addr:
                    log.info("dispatch REDUCE id=%s part=%s files=%d -> %s", job_id, pid, len(files), addr)
                    _dispatch_reduce(job_id, pid, files, addr)
                else:
                    log.warning("no workers available; requeue REDUCE id=%s part=%s", job_id, pid)
                    with LOCK: REDUCE_QUEUE.appendleft(rtask)
                    time.sleep(0.5)
                continue

            # Pulso cada 10s con tamaños de cola (debug)
            now = time.time()
            if now - last_tick > 10:
                with LOCK:
                    log.debug("queues single=%d map=%d reduce=%d ring=%d",
                              len(JOB_QUEUE_SINGLE), len(MAP_QUEUE), len(REDUCE_QUEUE), len(WORKER_RING))
                last_tick = now

            time.sleep(0.2)
        except Exception as e:
            log.exception("scheduler loop error: %s", e)
            time.sleep(0.5)


@app.on_event("startup")
def start_scheduler():
    log.info("starting scheduler loop")
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
