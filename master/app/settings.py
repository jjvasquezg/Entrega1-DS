from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    REST_HOST: str = "0.0.0.0"
    REST_PORT: int = 8080
    GRPC_TIMEOUT_S: int = 60
    SHARED_DIR: str = "/data/shared"   # bind a /mnt/gridmr-files
    # Dirección(es) de workers se registrarán dinámicamente,
    # pero dejamos un var por si queremos precargar
    KNOWN_WORKERS: str = ""  # "10.0.1.10:50051,10.0.1.11:50051"

settings = Settings()
