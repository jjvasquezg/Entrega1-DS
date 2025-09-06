import time
import asyncio
import grpc
from concurrent import futures
from .settings import settings

# Stubs generados en la imagen por el Dockerfile del master, copiamos aquí también:
from master.app import mapreduce_pb2 as pb2
from master.app import mapreduce_pb2_grpc as pb2_grpc

class WorkerService(pb2_grpc.WorkerServicer):
    def Heartbeat(self, request, context):
        return pb2.HeartbeatResp(ok=True, message=f"{settings.WORKER_ID} alive")

    def ExecuteMap(self, request, context):
        # Implementación real vendrá en el siguiente paso
        return pb2.MapResult(job_id=request.job_id, worker_id=settings.WORKER_ID, output_index="")

    def ExecuteReduce(self, request, context):
        return pb2.ReduceResult(job_id=request.job_id, worker_id=settings.WORKER_ID, output_path="")

async def register_to_master():
    import httpx
    if not settings.MASTER_HTTP:
        return
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            await client.post(f"{settings.MASTER_HTTP}/workers/register",
                              params={"worker_id": settings.WORKER_ID,
                                      "address": f"{settings.GRPC_HOST}:{settings.GRPC_PORT}"})
        except Exception:
            pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb2_grpc.add_WorkerServicer_to_server(WorkerService(), server)
    server.add_insecure_port(f"{settings.GRPC_HOST}:{settings.GRPC_PORT}")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(register_to_master())
    server.start()
    print(f"Worker {settings.WORKER_ID} listening on {settings.GRPC_HOST}:{settings.GRPC_PORT}, shared={settings.SHARED_DIR}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
