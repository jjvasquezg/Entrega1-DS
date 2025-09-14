import os
import time
import logging
import grpc
import hashlib
import importlib.util
from typing import Tuple, List, Dict
from concurrent import futures

from worker.app.settings import settings
from worker.app import mapreduce_pb2 as pb2
from worker.app import mapreduce_pb2_grpc as pb2_grpc

# --- logging básico a stdout ---
LOG_LEVEL = os.getenv("WORKER_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s"
)
log = logging.getLogger("gridmr-worker")

def load_user_module(script_path: str):
    spec = importlib.util.spec_from_file_location("user_script", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module

def run_job_single(script_path: str, input_path: str, output_dir: str) -> Tuple[bool, str, str]:
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "result.txt")
    try:
        mod = load_user_module(script_path)
        if hasattr(mod, "process"):
            mod.process(input_path, output_path)  # type: ignore
            return True, output_path, "process() done"
        if not (hasattr(mod, "map_func") and hasattr(mod, "reduce_func")):
            return False, "", "script must define process() or map_func/reduce_func"
        interm: Dict[str, list] = {}
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                pairs = mod.map_func(line)  # type: ignore
                for k, v in pairs:
                    interm.setdefault(str(k), []).append(v)
        with open(output_path, "w", encoding="utf-8") as out:
            for key, values in interm.items():
                k, res = mod.reduce_func(key, values)  # type: ignore
                out.write(f"{k}\t{res}\n")
        return True, output_path, "map/reduce (single) done"
    except Exception as e:
        return False, "", f"error: {e}"

def partitioner(key: str, reduce_partitions: int) -> int:
    """Hash estable basado en la key, para decidir la partición."""
    # Usamos SHA1 para consistencia, luego módulo
    h = int(hashlib.sha1(key.encode("utf-8")).hexdigest(), 16)
    return h % reduce_partitions

def run_map(script_path, input_chunk, shuffle_dir, reduce_partitions, chunk_id):
    try:
        spec = importlib.util.spec_from_file_location("user_script", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        map_func = getattr(module, "map_func", None)
        if not map_func:
            return False, [], "map_func not defined"

        outputs: Dict[int, str] = {}
        for pid in range(reduce_partitions):
            out_dir = os.path.join(shuffle_dir, f"part-{pid:04d}")
            os.makedirs(out_dir, exist_ok=True)
            outputs[pid] = os.path.join(out_dir, f"chunk-{chunk_id}")

        writers: Dict[int, any] = {pid: open(path, "w") for pid, path in outputs.items()}
        try:
            with open(input_chunk) as fin:
                for line in fin:
                    for key, value in map_func(line):
                        pid = partitioner(str(key), reduce_partitions)
                        writers[pid].write(f"{key}\t{value}\n")
        finally:
            for f in writers.values():
                f.close()

        return True, list(outputs.values()), "ok"

    except Exception as e:
        return False, [], str(e)

def run_reduce(script_path: str, partition_id: int, inputs: List[str], output_path: str) -> Tuple[bool, str]:
    """
    Lee todos los archivos de la partición (inputs) y aplica reduce_func agrupando en memoria.
    """
    try:
        mod = load_user_module(script_path)
        if not hasattr(mod, "reduce_func"):
            return False, "script must define reduce_func for distributed mode"

        groups: Dict[str, list] = {}
        for fp in inputs:
            if not os.path.exists(fp):  # puede que algún map no haya producido esa clave
                continue
            with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if "\t" not in line: 
                        # tolerar líneas raras
                        key = line.strip()
                        val = 1
                    else:
                        key, val = line.rstrip("\n").split("\t", 1)
                        try:
                            # intentar parsear numérico si aplica
                            val = int(val)
                        except Exception:
                            pass
                    groups.setdefault(key, []).append(val)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as out:
            for key, vals in groups.items():
                k, res = mod.reduce_func(key, vals)  # type: ignore
                out.write(f"{k}\t{res}\n")

        return True, "reduce done"
    except Exception as e:
        return False, f"error: {e}"

class WorkerService(pb2_grpc.WorkerServicer):
    def Heartbeat(self, request, context):
        return pb2.HeartbeatResp(ok=True, message=f"{settings.WORKER_ID} alive")

    def ExecuteJob(self, request, context):
        log.info(f"[{settings.WORKER_ID}] ExecuteJob start job=%s", request.job_id)
        log.info(" script=%s input=%s outdir=%s", request.script_path, request.input_path, request.output_dir)
        ok, out_path, log_or_err = run_job_single(request.script_path, request.input_path, request.output_dir)
        if ok:
            log.info(f"[{settings.WORKER_ID}] ExecuteJob end ok=True out=%s", out_path)
        else:
            log.error(f"[{settings.WORKER_ID}] ExecuteJob end ok=False err=%s", log_or_err)
        return pb2.JobResult(
            job_id=request.job_id,
            worker_id=settings.WORKER_ID,
            output_path=out_path,
            log=(log_or_err if ok else ""),
            success=ok,
            error=("" if ok else log_or_err),
        )

    def ExecuteMap(self, request, context):
        log.info(f"[{settings.WORKER_ID}] ExecuteMap start job=%s chunk=%s parts=%s", request.job_id, request.chunk_id, request.reduce_partitions)
        ok, files, log_or_err = run_map(
            request.script_path, request.input_chunk, request.shuffle_dir,
            request.reduce_partitions, request.chunk_id
        )
        if ok:
            log.info(f"[{settings.WORKER_ID}] ExecuteMap end ok=True files=%s", files)
        else:
            log.error(f"[{settings.WORKER_ID}] ExecuteMap end ok=False err=%s", log_or_err)
        return pb2.MapResult(
            job_id=request.job_id,
            worker_id=settings.WORKER_ID,
            chunk_id=request.chunk_id,
            partition_files=files,
            success=ok,
            error=("" if ok else log_or_err),
        )

    def ExecuteReduce(self, request, context):
        log.info(f"[{settings.WORKER_ID}] ExecuteReduce start job=%s part=%s inputs=%d", request.job_id, request.partition_id, len(request.shuffle_inputs))
        ok, log_or_err = run_reduce(
            request.script_path, request.partition_id,
            list(request.shuffle_inputs), request.output_path
        )
        if ok:
            log.info(f"[{settings.WORKER_ID}] ExecuteReduce end ok=True out=%s", request.output_path)
        else:
            log.error(f"[{settings.WORKER_ID}] ExecuteReduce end ok=False err=%s", log_or_err)
        return pb2.ReduceResult(
            job_id=request.job_id,
            worker_id=settings.WORKER_ID,
            partition_id=request.partition_id,
            output_path=request.output_path,
            success=ok,
            error=("" if ok else log_or_err),
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb2_grpc.add_WorkerServicer_to_server(WorkerService(), server)
    server.add_insecure_port(f"{settings.GRPC_HOST}:{settings.GRPC_PORT}")

    # Autoregistro (best-effort)
    try:
        import httpx, os
        if settings.MASTER_HTTP:
            host_to_advertise = settings.ADVERTISE_HOST or os.getenv("HOST_IP", "") or settings.GRPC_HOST
            with httpx.Client(timeout=5) as client:
                client.post(
                    f"{settings.MASTER_HTTP}/workers/register",
                    params={"worker_id": settings.WORKER_ID,
                            "address": f"{host_to_advertise}:{settings.GRPC_PORT}"}
                )
    except Exception:
        pass


    server.start()
    print(f"Worker {settings.WORKER_ID} listening on {settings.ADVERTISE_HOST}:{settings.GRPC_PORT}, shared={settings.SHARED_DIR}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
