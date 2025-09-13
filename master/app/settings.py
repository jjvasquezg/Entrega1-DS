from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    REST_HOST: str = "0.0.0.0"
    REST_PORT: int = 8080
    GRPC_TIMEOUT_S: int = 180
    SHARED_DIR: str = "/data/shared"
    FILE_PARTITION_THRESHOLD_BYTES: int = 50_000_000  # ~50MB
    DEFAULT_PARTITIONS: int = 6

settings = Settings()

