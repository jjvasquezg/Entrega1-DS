FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app:/app/master/app \
    MASTER_LOG_LEVEL=${MASTER_LOG_LEVEL} 

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev && rm -rf /var/lib/apt/lists/*

COPY proto/ /app/proto/
COPY master/app /app/master/app

RUN pip install --upgrade pip && \
    pip install fastapi uvicorn[standard] \
               grpcio grpcio-tools protobuf httpx \
               pydantic pydantic-settings

RUN python -m grpc_tools.protoc -I./proto \
    --python_out=./master/app \
    --grpc_python_out=./master/app \
    proto/mapreduce.proto

EXPOSE 8080
VOLUME ["/data/shared"]

CMD ["uvicorn", "master.app.main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
