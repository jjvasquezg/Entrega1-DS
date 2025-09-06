from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    GRPC_HOST: str = "0.0.0.0"
    GRPC_PORT: int = 50051
    SHARED_DIR: str = "/data/shared"
    WORKER_ID: str = "worker-unknown"
    MASTER_HTTP: str = ""  # http://<master-ip>:8080 para autoinregistro

settings = Settings()
