FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev && rm -rf /var/lib/apt/lists/*

# Copiamos proto y stubs igual que en master para mantener versiones
COPY proto/ /app/proto/
COPY worker/app /app/worker/app
COPY master/app/mapreduce_pb2.py /app/master/app/mapreduce_pb2.py
COPY master/app/mapreduce_pb2_grpc.py /app/master/app/mapreduce_pb2_grpc.py

RUN pip install --upgrade pip && \
    pip install grpcio grpcio-tools protobuf pydantic httpx

EXPOSE 50051
VOLUME ["/data/shared"]

CMD ["python", "worker/app/server.py"]
