# master/app/main.py

import os
import time
import uuid
import asyncio
import threading
import logging
from collections import deque, defaultdict
from enum import Enum
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, asdict
from time import time as now
from concurrent.futures import ThreadPoolExecutor

import grpc
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from .settings import settings
from .storage import (
    job_paths, ensure_dir, download_to_file,
    file_size, partition_input_by_lines, concat_files
)

# Stubs gRPC (generados por grpc_tools.protoc durante el build)
from master.app import mapreduce_pb2 as pb2
from master.app import mapreduce_pb2_grpc as pb2_grpc

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("MASTER_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [master] %(message)s"
)
log = logging.getLogger("gridmr-master")

# ---------------------------------------------------------------------------
# Config concurrencia (ajustables por ENV)
# ---------------------------------------------------------------------------
EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv("EXECUTOR_MAX_WORKERS", "32")))
MAX_INFLIGHT_MAPS = int(os.getenv("MAX_INFLIGHT_MAPS", "16"))
MAX_INFLIGHT_REDUCES = int(os.getenv("MAX_INFLIGHT_REDUCES", "8"))
MAX_INFLIGHT_SINGLES = int(os.getenv("MAX_INFLIGHT_SINGLES", "4"))

INFLIGHT_MAPS = 0
INFLIGHT_REDUCES = 0
INFLIGHT_SINGLES = 0

# ---------------------------------------------------------------------------
# Modelos / Estados
# ---------------------------------------------------------------------------
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

class JobRequest(BaseModel):
    script_url: str
    input_url: str
    # control explícito:
    map_splits: int = 3
    reduce_partitions: int = 3
    # Forzar distributed (por defecto True). Si False, decide por tamaño:
    force_distributed: bool = True
    # Compat: si el cliente manda "partitions", lo usamos para ambos
    partitions: Optional[int] = None

class JobStatus(BaseModel):
    job_id: str
    status: str
    mode: Optional[JobMode] = None
    message: Optional[str] = None
    outputs: Optional[List[str]] = None
    script_path: Optional[str] = None
    input_path: Optional[str] = None
    chunks_dir: Optional[str] = None
    shuffle_dir: Optional[str] = None
    partitions: Optional[int] = None
    map_splits: Optional[int] = None
    reduce_partitions: Optional[int] = None

@dataclass
class JobTimeline:
    job_id: str
    t_submit: float
    t_prepare_start: float | None = None
    t_prepare_end: float | None = None
    # distributed
    t_map_start: float | None = None
    t_map_end: float | None = None
    t_reduce_start: float | None = None
    t_reduce_end: float | None = None
    # single
    t_single_start: float | None = None
    t_single_end: float | None = None
    # final
    t_finish: float | None = None
    status: str = "QUEUED"
    mode: str | None = None
    map_splits: int | None = None
    reduce_partitions: int | None = None
    input_size_bytes: int | None = None
    message: str | None = None

# ---------------------------------------------------------------------------
# Estado global
# ---------------------------------------------------------------------------
app = FastAPI(title="GridMR Master")

JOBS: Dict[str, JobStatus] = {}
JOB_TIMELINES: Dict[str, JobTimeline] = {}

WORKER_INFO: Dict[str, dict] = {}   # worker_id -> {address, last_seen}
WORKER_RING = deque()               # direcciones gRPC (ip:port), RR
LOCK = threading.Lock()

# Colas de trabajo
JOB_QUEUE_SINGLE = deque()          # job_ids en modo SINGLE
MAP_QUEUE = deque()                 # (job_id, chunk_path, chunk_id)
REDUCE_QUEUE = deque()              # (job_id, partition_id, files)

# Progreso distributed
MAP_RESULTS: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
PENDING_MAPS: Dict[str, int] = {}
PENDING_REDUCES: Dict[str, int] = {}

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    log.debug("health check")
    return {
        "ok": True,
        "shared_dir": settings.SHARED_DIR,
        "threshold": settings.FILE_PARTITION_THRESHOLD_BYTES
    }

@app.get("/workers")
def list_workers():
    return {"workers": WORKER_INFO, "ring": list(WORKER_RING)}

@app.post("/workers/register")
def register_worker(worker_id: str, address: str):
    with LOCK:
        WORKER_INFO[worker_id] = {"address": address, "last_seen": time.time()}
        if address not in WORKER_RING:
            WORKER_RING.append(address)
    log.info("worker registered id=%s addr=%s ring=%s", worker_id, address, list(WORKER_RING))
    return {"ok": True, "count": len(WORKER_INFO), "ring": list(WORKER_RING)}

@app.get("/jobs")
def list_jobs():
    jobs = []
    for job_id, st in JOBS.items():
        jobs.append({
            "job_id": job_id,
            "status": st.status,
            "mode": st.mode,
            "message": st.message,
            "outputs": st.outputs
        })
    return {"jobs": jobs}

@app.get("/jobs/latest")
def latest_job():
    if not JOBS:
        return {"error": "no jobs yet"}
    job_id = list(JOBS.keys())[-1]
    st = JOBS[job_id]
    return {
        "job_id": job_id,
        "status": st.status,
        "mode": st.mode,
        "message": st.message,
        "outputs": st.outputs
    }

@app.post("/jobs")
def submit_job(req: JobRequest, background_tasks: BackgroundTasks):
    # ID corto y legible
    job_id = uuid.uuid4().hex[:8]
    log.info("job submitted id=%s script=%s input=%s map_splits=%s reduce_partitions=%s force_distributed=%s",
             job_id, req.script_url, req.input_url, req.map_splits, req.reduce_partitions, req.force_distributed)
    st = JobStatus(job_id=job_id, status=JobState.QUEUED)
    JOBS[job_id] = st
    JOB_TIMELINES[job_id] = JobTimeline(job_id=job_id, t_submit=now(), status="QUEUED")
    background_tasks.add_task(prepare_job, job_id, req)
    return {
        "job_id": job_id,
        "status": st.status,
        "message": "job submitted successfully"
    }

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    st = JOBS.get(job_id)
    if not st:
        return JobStatus(job_id=job_id, status=JobState.UNKNOWN, message="not found")
    return st

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
        "map_splits": st.map_splits,
        "reduce_partitions": st.reduce_partitions,
    }

@app.delete("/jobs/{job_id}")
def delete_job(job_id: str):
    existed = job_id in JOBS
    if existed:
        del JOBS[job_id]
    # limpieza de estructuras auxiliares
    MAP_RESULTS.pop(job_id, None)
    PENDING_MAPS.pop(job_id, None)
    PENDING_REDUCES.pop(job_id, None)
    JOB_TIMELINES.pop(job_id, None)
    return {"job_id": job_id, "deleted": existed}

@app.get("/jobs/{job_id}/timeline")
def job_timeline(job_id: str):
    tl = JOB_TIMELINES.get(job_id)
    if not tl:
        raise HTTPException(404, "job not found")
    return asdict(tl)

@app.get("/metrics/jobs")
def metrics_jobs():
    out = []
    for job_id, tl in JOB_TIMELINES.items():
        def d(a, b): return (a - b) if (a and b) else None
        total = d(tl.t_finish, tl.t_submit)
        prepare = d(tl.t_prepare_end, tl.t_prepare_start)
        map_d   = d(tl.t_map_end,     tl.t_map_start)
        reduce_d= d(tl.t_reduce_end,  tl.t_reduce_start)
        single  = d(tl.t_single_end,  tl.t_single_start)
        parts = [x for x in [prepare, map_d, reduce_d, single] if x is not None]
        overhead = (max(total - sum(parts), 0.0) if (total is not None and parts) else None)
        out.append({
            "job_id": job_id,
            "status": tl.status,
            "mode": tl.mode,
            "input_size_bytes": tl.input_size_bytes,
            "map_splits": tl.map_splits,
            "reduce_partitions": tl.reduce_partitions,
            "t_total_s": total,
            "t_prepare_s": prepare,
            "t_single_s": single,
            "t_map_s": map_d,
            "t_reduce_s": reduce_d,
            "t_overhead_s": overhead
        })
    return {"jobs": out}

# ---------------------------------------------------------------------------
# Preparación de jobs
# ---------------------------------------------------------------------------
def prepare_job(job_id: str, req: JobRequest):
    try:
        asyncio.run(_prepare_async(job_id, req))
    except Exception as e:
        st = JOBS.get(job_id)
        if st:
            st.status = JobState.FAILED
            st.message = str(e)
        tl = JOB_TIMELINES.get(job_id)
        if tl:
            tl.t_finish = now()
            tl.status = "FAILED"
            tl.message = str(e)
        log.exception("prepare job failed id=%s err=%s", job_id, e)

async def _prepare_async(job_id: str, req: JobRequest):
    tl = JOB_TIMELINES[job_id]
    tl.t_prepare_start = now()
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
    tl.input_size_bytes = size

    # normaliza compat
    if req.partitions and not req.map_splits:
        req.map_splits = max(1, req.partitions)
    if req.partitions and not req.reduce_partitions:
        req.reduce_partitions = max(1, req.partitions)

    # Decidir modo
    use_distributed = req.force_distributed or (size >= settings.FILE_PARTITION_THRESHOLD_BYTES)

    if use_distributed:
        st.mode = JobMode.DISTRIBUTED
        # fija splits/partitions
        map_splits = max(1, req.map_splits or 1)
        reduce_parts = max(1, req.reduce_partitions or 1)
        st.map_splits = map_splits
        st.reduce_partitions = reduce_parts
        st.partitions = reduce_parts

        chunk_files = partition_input_by_lines(paths["input_file"], paths["chunks"], map_splits)
        with LOCK:
            for idx, chunk in enumerate(sorted(chunk_files)):
                MAP_QUEUE.append((job_id, chunk, f"{idx:04d}"))
            PENDING_MAPS[job_id] = len(chunk_files)
        log.info("job queued for MAP id=%s chunks=%d", job_id, len(chunk_files))
        tl.mode = "DISTRIBUTED"
        tl.map_splits = len(chunk_files)
        tl.reduce_partitions = reduce_parts
    else:
        st.mode = JobMode.SINGLE
        st.map_splits = 1
        st.reduce_partitions = 1
        with LOCK:
            JOB_QUEUE_SINGLE.append(job_id)
        log.info("job queued SINGLE id=%s", job_id)
        tl.mode = "SINGLE"

    st.status = JobState.PREPARED
    st.script_path = paths["script"]
    st.input_path = paths["input_file"]
    st.chunks_dir = (paths["chunks"] if st.mode == JobMode.DISTRIBUTED else None)
    st.shuffle_dir = (paths["shuffle"] if st.mode == JobMode.DISTRIBUTED else None)

    tl.t_prepare_end = now()
    tl.status = "PREPARED"

# ---------------------------------------------------------------------------
# Utilidades scheduler
# ---------------------------------------------------------------------------
def _take_worker_rr() -> Optional[str]:
    with LOCK:
        if not WORKER_RING:
            return None
        addr = WORKER_RING[0]
        WORKER_RING.rotate(-1)
        return addr

# ---------------------------------------------------------------------------
# Despachos (SINGLE / MAP / REDUCE)
# ---------------------------------------------------------------------------
def _dispatch_single(job_id: str, worker_addr: str):
    st = JOBS[job_id]
    tl = JOB_TIMELINES.get(job_id)
    paths = job_paths(settings.SHARED_DIR, job_id)
    st.status = JobState.RUNNING
    log.info("SINGLE start id=%s worker=%s", job_id, worker_addr)
    log.debug("paths script=%s input=%s out=%s", paths["script"], paths["input_file"], paths["out"])

    if tl:
        tl.mode = tl.mode or "SINGLE"
        tl.t_single_start = tl.t_single_start or now()
        tl.status = "RUNNING"

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
    finally:
        if tl:
            tl.t_single_end = tl.t_single_end or now()
            tl.t_finish = tl.t_finish or tl.t_single_end
            tl.status = st.status
            if st.message:
                tl.message = st.message

def _dispatch_map(job_id: str, chunk_path: str, chunk_id: str, worker_addr: str):
    st = JOBS[job_id]
    tl = JOB_TIMELINES.get(job_id)
    st.status = JobState.MAPPING
    paths = job_paths(settings.SHARED_DIR, job_id)
    if tl and tl.t_map_start is None:
        tl.t_map_start = now()
        tl.status = "MAPPING"
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
                reduce_partitions=st.reduce_partitions or settings.DEFAULT_PARTITIONS,
                chunk_id=chunk_id,
            )
            resp: pb2.MapResult = stub.ExecuteMap(req, timeout=settings.GRPC_TIMEOUT_S)

        if not resp.success:
            st.status = JobState.FAILED
            st.message = resp.error or f"map failed chunk {chunk_id}"
            log.error("MAP fail id=%s chunk=%s err=%s", job_id, chunk_id, st.message)
            if tl:
                tl.t_finish = now(); tl.status = "FAILED"; tl.message = st.message
            return False

        MAP_RESULTS[job_id][resp.chunk_id] = list(resp.partition_files)
        log.info("MAP end id=%s chunk=%s files=%d", job_id, chunk_id, len(resp.partition_files))

        finished_maps = False
        with LOCK:
            PENDING_MAPS[job_id] -= 1
            if PENDING_MAPS[job_id] == 0:
                tl and setattr(tl, "t_map_end", now())
                # agrupar por partición
                parts = st.reduce_partitions or settings.DEFAULT_PARTITIONS
                by_partition: Dict[int, List[str]] = {i: [] for i in range(parts)}
                for _, part_files in MAP_RESULTS[job_id].items():
                    for p, f in enumerate(part_files):
                        by_partition[p].append(f)
                for p in range(parts):
                    REDUCE_QUEUE.append((job_id, p, by_partition[p]))
                PENDING_REDUCES[job_id] = parts
                log.info("MAP all done id=%s -> queued %d reduces", job_id, parts)
                finished_maps = True
        return finished_maps

    except Exception as e:
        st.status = JobState.FAILED
        st.message = f"map dispatch error: {e}"
        log.error("MAP dispatch error id=%s chunk=%s err=%s", job_id, chunk_id, e)
        if tl:
            tl.t_finish = now(); tl.status = "FAILED"; tl.message = st.message
        return False

def _dispatch_reduce(job_id: str, partition_id: int, files: List[str], worker_addr: str):
    st = JOBS[job_id]
    tl = JOB_TIMELINES.get(job_id)
    st.status = JobState.REDUCING
    paths = job_paths(settings.SHARED_DIR, job_id)
    output_path = os.path.join(paths["out"], f"part-{partition_id:04d}")
    if tl and tl.t_reduce_start is None:
        tl.t_reduce_start = now()
        tl.status = "REDUCING"
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
            if tl:
                tl.t_finish = now(); tl.status = "FAILED"; tl.message = st.message
            return False

        log.info("REDUCE end id=%s part=%s out=%s", job_id, partition_id, resp.output_path)

        finished = False
        with LOCK:
            PENDING_REDUCES[job_id] -= 1
            if PENDING_REDUCES[job_id] == 0:
                tl and setattr(tl, "t_reduce_end", now())
                # concatenar parts -> result.txt
                parts = sorted([os.path.join(paths["out"], f)
                                for f in os.listdir(paths["out"]) if f.startswith("part-")])
                concat_files(parts, paths["result"])
                st.status = JobState.SUCCEEDED
                st.outputs = [paths["result"]]
                st.message = "ok"
                finished = True
                if tl:
                    tl.t_finish = now()
                    tl.status = "SUCCEEDED"
        return finished

    except Exception as e:
        st.status = JobState.FAILED
        st.message = f"reduce dispatch error: {e}"
        log.error("REDUCE dispatch error id=%s part=%s err=%s", job_id, partition_id, e)
        if tl:
            tl.t_finish = now(); tl.status = "FAILED"; tl.message = st.message
        return False

# ---------------------------------------------------------------------------
# Scheduler (concurrency-aware para MAP/REDUCE/SINGLE)
# ---------------------------------------------------------------------------
def scheduler_loop():
    global INFLIGHT_MAPS, INFLIGHT_REDUCES, INFLIGHT_SINGLES
    log.info("starting scheduler loop")
    last_tick = 0.0

    while True:
        try:
            dispatched_any = False

            # SINGLE en paralelo
            while INFLIGHT_SINGLES < MAX_INFLIGHT_SINGLES:
                job_id = None
                with LOCK:
                    if JOB_QUEUE_SINGLE:
                        job_id = JOB_QUEUE_SINGLE.popleft()
                if not job_id:
                    break
                addr = _take_worker_rr()
                if not addr:
                    with LOCK:
                        JOB_QUEUE_SINGLE.appendleft(job_id)
                    break
                INFLIGHT_SINGLES += 1
                log.info("dispatch SINGLE id=%s -> %s (inflight_single=%d)", job_id, addr, INFLIGHT_SINGLES)

                def _run_single(jid: str, a: str):
                    global INFLIGHT_SINGLES
                    try:
                        _dispatch_single(jid, a)
                    finally:
                        INFLIGHT_SINGLES -= 1

                EXECUTOR.submit(_run_single, job_id, addr)
                dispatched_any = True

            # MAP en paralelo
            while INFLIGHT_MAPS < MAX_INFLIGHT_MAPS:
                task = None
                with LOCK:
                    if MAP_QUEUE:
                        task = MAP_QUEUE.popLeft() if hasattr(MAP_QUEUE, "popLeft") else MAP_QUEUE.popleft()
                if not task:
                    break
                job_id, chunk_path, chunk_id = task
                addr = _take_worker_rr()
                if not addr:
                    with LOCK:
                        MAP_QUEUE.appendleft(task)
                    break
                INFLIGHT_MAPS += 1
                log.info("dispatch MAP id=%s chunk=%s -> %s (inflight_map=%d)", job_id, chunk_id, addr, INFLIGHT_MAPS)

                def _run_map(jid: str, cpath: str, cid: str, a: str):
                    global INFLIGHT_MAPS
                    try:
                        _dispatch_map(jid, cpath, cid, a)
                    finally:
                        INFLIGHT_MAPS -= 1

                EXECUTOR.submit(_run_map, job_id, chunk_path, chunk_id, addr)
                dispatched_any = True

            # REDUCE en paralelo
            while INFLIGHT_REDUCES < MAX_INFLIGHT_REDUCES:
                rtask = None
                with LOCK:
                    if REDUCE_QUEUE:
                        rtask = REDUCE_QUEUE.popleft()
                if not rtask:
                    break
                job_id, pid, files = rtask
                addr = _take_worker_rr()
                if not addr:
                    with LOCK:
                        REDUCE_QUEUE.appendleft(rtask)
                    break
                INFLIGHT_REDUCES += 1
                log.info("dispatch REDUCE id=%s part=%s -> %s (inflight_reduce=%d)", job_id, pid, addr, INFLIGHT_REDUCES)

                def _run_reduce(jid: str, part_id: int, fpaths: List[str], a: str):
                    global INFLIGHT_REDUCES
                    try:
                        _dispatch_reduce(jid, part_id, fpaths, a)
                    finally:
                        INFLIGHT_REDUCES -= 1

                EXECUTOR.submit(_run_reduce, job_id, pid, files, addr)
                dispatched_any = True

            # Pulso de estado
            nowt = time.time()
            if nowt - last_tick > 10:
                with LOCK:
                    log.debug(
                        "queues single=%d map=%d reduce=%d | inflight S/M/R = %d/%d/%d | ring=%d",
                        len(JOB_QUEUE_SINGLE), len(MAP_QUEUE), len(REDUCE_QUEUE),
                        INFLIGHT_SINGLES, INFLIGHT_MAPS, INFLIGHT_REDUCES,
                        len(WORKER_RING)
                    )
                last_tick = nowt

            if not dispatched_any:
                time.sleep(0.1)

        except Exception as e:
            log.exception("scheduler loop error: %s", e)
            time.sleep(0.5)

@app.on_event("startup")
def start_scheduler():
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
