FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev && rm -rf /var/lib/apt/lists/*

# Copiamos proto y código del worker
COPY proto/ /app/proto/
COPY worker/app /app/worker/app

# Dependencias
RUN pip install --upgrade pip && \
    pip install grpcio grpcio-tools protobuf pydantic pydantic_settings httpx

# Generar stubs gRPC para el worker
RUN python -m grpc_tools.protoc -I./proto \
    --python_out=./worker/app \
    --grpc_python_out=./worker/app \
    proto/mapreduce.proto

ENV PYTHONPATH=/app

EXPOSE 50051
VOLUME ["/data/shared"]

CMD ["python", "-m" , "worker.app.server"]
