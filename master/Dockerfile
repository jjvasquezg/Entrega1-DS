FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Dependencias básicas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev && rm -rf /var/lib/apt/lists/*

COPY proto/ /app/proto/
COPY master/app /app/master/app
COPY common /app/common

RUN pip install --upgrade pip && \
    pip install fastapi uvicorn[standard] pydantic grpcio grpcio-tools protobuf httpx

# Generar stubs gRPC
RUN python -m grpc_tools.protoc -I./proto \
    --python_out=./master/app \
    --grpc_python_out=./master/app \
    proto/mapreduce.proto

EXPOSE 8080
VOLUME ["/data/shared"]

CMD ["uvicorn", "master.app.main:app", "--host", "0.0.0.0", "--port", "8080"]
