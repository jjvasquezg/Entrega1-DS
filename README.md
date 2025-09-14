# Info de la materia: ST0263 Sistemas Distribuidos

# Estudiante(s): 
 Santiago Alvarez Peña, salvarezp4@eafit.edu.co  
 Juan José Vasquez Gomez, jjvasquezg@eafit.edu.co  
 Sebastián Giraldo Alvarez, sgiraldoa7@eafit.edu.co

## Profesor: Edwin Nelson Montoya Munera, emontoya@eafit.edu.co

# Proyecto 1 GridMR

## 1. Breve descripción de la actividad

**GridMR** es un sistema de procesamiento distribuido basado en el paradigma **MapReduce** que opera sobre un **grid** de nodos (EC2) dockerizados. El **cliente** envía trabajos (script + datos) al **master** vía **REST**; el master prepara los insumos en un NFS compartido y planifica la ejecución: llama a los **workers** vía **gRPC** para ejecutar **Map** y **Reduce**. Los resultados se guardan en el NFS y se exponen por API.

Arquitectura **Master–Workers**, comunicación **REST** (cliente→master) y **gRPC** (master↔workers), almacenamiento común por **NFS** y planificación **Round Robin** con despachos **concurrentes** (ThreadPool). Soporta:
- **SINGLE**: trabajo completo en un worker (archivos pequeños).
- **DISTRIBUTED**: partición en *map_splits* y agregación en *reduce_partitions*, con *partitioner(key)* determinístico para que cada clave sea reducida en una sola partición.

> Inspirado en MapReduce (Dean & Ghemawat) y Spark (Zaharia et al.), siguiendo el enunciado del curso.

### 1.1. Qué aspectos **cumplió** o desarrolló (funcionales y no funcionales)

- API REST del master: crear/listar/estado/borrar jobs, listar artefactos y descargar resultado.
- gRPC de workers: `ExecuteMap`, `ExecuteReduce`, `ExecuteJob` (SINGLE).
- **NFS** compartido entre master y workers (`/srv/gridmr-files` en el server, montado como `/mnt/gridmr-files` en nodos; contenedores lo ven como `/data/shared`).
- **Planificador Round Robin** con **ejecución concurrente** (pool de threads) y límites configurables para **SINGLE/MAP/REDUCE**.
- **Partitioner por key** (SHA1 mod *reduce_partitions*) → cada palabra cae en una sola partición (evita duplicados).
- **Métricas por job** (tiempos *prepare*, *single*, *map*, *reduce*, *total* y *overhead*).
- **Dockerización** completa (master y workers).  
- Registro de workers vía REST con `address=IP_PRIVADA:50051` (alcanzable por el master).
- Modo **forzado DISTRIBUTED** (flag `force_distributed`), independientemente del tamaño.

### 1.2. Qué aspectos **no** se cumplieron (o quedan como trabajo futuro)

- Autodiscovery de IP y *health* continuo sin intervención (hoy se registra por REST o env var).  
- Tolerancia a fallos avanzada (reintentos por task, *speculative execution* de *stragglers*).  
- Seguridad de capa de transporte (HTTPS/MTLS), autenticación/autorización de API.  
- Métricas Prometheus/Grafana; hoy solo endpoint JSON.  
- “Data locality” real (trabajamos con NFS centralizado).  
- Balanceo *least-loaded*; hoy Round Robin puro.

---

## 2. Diseño de alto nivel, arquitectura, patrones y buenas prácticas

**Tipo**: Maestro–Trabajador (C/S).  
**Patrones**:
- *Master orchestrator* + *workers stateless*.
- *Map* → *Shuffle (en NFS)* → *Reduce*; *partitioner(key)* determinístico.
- *Queues* (deques) por fase + **Round Robin** + **concurrencia** con `ThreadPoolExecutor`.
- Endpoints REST explícitos y simples; stubs gRPC autogenerados.

**Buenas prácticas**:
- Contenedores aislados con **PYTHONPATH** corregido para stubs gRPC.  
- **Logging** con niveles configurables (`MASTER_LOG_LEVEL`, `WORKER_LOG_LEVEL`).  
- Variables de entorno para IP/puertos/rutas; no *hardcodear* addresses.  
- Separación de responsabilidades: API REST, scheduler, despacho gRPC, funciones de almacenamiento.

**Componentes**:
- **Cliente externo** → REST → **Master** → gRPC → **Workers** → NFS.  
- **NFS**: `/srv/gridmr-files` (server) ↔ `/mnt/gridmr-files` (nodos) ↔ contenedor: `/data/shared`.  
- **Proto gRPC**: `mapreduce.proto` (`ExecuteJob`, `ExecuteMap`, `ExecuteReduce`, `Heartbeat`).

---

## 3. Ambiente de desarrollo/técnico (versiones y dependencias)

- **Lenguaje**: Python 3.11
- **Framework REST**: FastAPI 0.110+; **ASGI**: Uvicorn (con uvloop)
- **RPC**: grpcio / grpcio-tools / protobuf
- **Config**: pydantic v2 + pydantic-settings
- **HTTP client** (worker autoinfo): httpx
- **Docker**: imágenes *slim* basadas en `python:3.11-slim`

### Cómo compilar y ejecutar

**Estructura (resumen):**
```
proto/
  mapreduce.proto
master/
  app/
    main.py, settings.py, storage.py, mapreduce_pb2*.py (generados)
worker/
  app/
    server.py, settings.py, mapreduce_pb2*.py (generados)
deploy/
  master.env
  worker.env
samples/
  wordcount.py
  input.txt
```

**Build (desde raíz):**
```bash
# Master
docker build -t gridmr-master -f master/Dockerfile .

# Worker (repetir para cada nodo)
docker build -t gridmr-worker -f worker/Dockerfile .
```

**Run (ejemplo):**
```bash
# Master (EC2 con NFS montado)
docker run -d --name gridmr-master   -p 8080:8080   -v /mnt/gridmr-files:/data/shared:rw   --env-file deploy/master.env   gridmr-master

# Worker (cada EC2 worker)
docker run -d --name gridmr-worker1   -p 50051:50051   -v /mnt/gridmr-files:/data/shared:rw   --env-file deploy/worker.env   gridmr-worker
```

### Detalles técnicos claves

- **Generación de stubs gRPC** en build (`grpc_tools.protoc`) y ajuste de `PYTHONPATH=/app:/app/master/app` para imports.
- **Partitioner** en `worker/app/server.py`:
  ```python
  import hashlib
  pid = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % reduce_partitions
  ```
- **Scheduler concurrente** (master): `ThreadPoolExecutor` y límites en vuelo:
  - `MAX_INFLIGHT_SINGLES`, `MAX_INFLIGHT_MAPS`, `MAX_INFLIGHT_REDUCES`.
- **Forzar distributed**: `force_distributed=true` en el request (independiente del umbral).
- **Métricas**: `/metrics/jobs` (tiempos por fase, total y overhead).

### Parámetros del proyecto (env)

**`deploy/master.env` (ejemplo):**
```
MASTER_LOG_LEVEL=INFO
GRPC_TIMEOUT_S=120
FILE_PARTITION_THRESHOLD_BYTES=5242880   # 5MB
EXECUTOR_MAX_WORKERS=32
MAX_INFLIGHT_SINGLES=4
MAX_INFLIGHT_MAPS=16
MAX_INFLIGHT_REDUCES=8
SHARED_DIR=/data/shared
```

**`deploy/worker.env` (ejemplo por nodo):**
```
GRPC_HOST=0.0.0.0      # bind interno gRPC
GRPC_PORT=50051
SHARED_DIR=/data/shared
WORKER_ID=worker-1
MASTER_HTTP=http://<IP_PRIVADA_MASTER>:8080
ADVERTISE_HOST=<IP_PRIVADA_WORKER1>      # IP enrutable por el master
WORKER_LOG_LEVEL=INFO
```

**Puertos y rutas**:
- Master REST: `:8080`
- Worker gRPC: `:50051`
- NFS host path: `/srv/gridmr-files` (server) → `/mnt/gridmr-files` (nodos) → contenedor: `/data/shared`

### Organización de carpetas (sugerencia de `tree`)

```
.
├── proto/
├── master/
│   └── app/
│       ├── main.py
│       ├── settings.py
│       ├── storage.py
│       └── mapreduce_pb2*.py
├── worker/
│   └── app/
│       ├── server.py
│       ├── settings.py
│       └── mapreduce_pb2*.py
├── deploy/
│   ├── master.env
│   └── worker.env
└── samples/
    ├── wordcount.py
    └── input.txt
```

---

## 4. Ambiente de **EJECUCIÓN** (producción)

- **AWS EC2** para master y workers; **NFS** server (EC2 o EFS).
- **Docker** en todos los nodos.
- **Security Groups**:
  - Master: abrir `8080/tcp` (REST) desde IPs clientes/ALB.
  - Workers: abrir `50051/tcp` desde el SG del master.
  - NFS: `2049/tcp` entre NFS server y nodos.
- **DNS/IP**: el cliente usa la **IP/DNS pública** del master.

### Configuración de parámetros (prod)

- Definir `ADVERTISE_HOST` en cada worker con la **IP privada** enrutable.
- Asegurar permisos de escritura en NFS para el usuario del contenedor (para pruebas, `chmod -R 0777 /srv/gridmr-files`).
- (Opcional) Ajustar timeouts y concurrencia según CPU/IO real.

### Lanzar el servidor (ejemplo ya arriba)

- Master: contenedor con `deploy/master.env`.
- Workers: tantos como nodos; cada uno con su `worker.env`.  
- Registrar workers (si no se auto-registran) vía REST:
  ```bash
  curl -X POST "http://<MASTER_PRIV>:8080/workers/register"     -d worker_id=worker-1 -d address=<IP_PRIVADA_WORKER1>:50051
  ```

### Mini guía de uso (cliente externo)

**Crear job (DISTRIBUTED forzado, 3×3):**
```bash
export MASTER_URL="http://3.228.131.251:8080"

curl -s -X POST "$MASTER_URL/jobs"   -H "Content-Type: application/json"   --data-binary '{
    "script_url":"nfs:///data/shared/samples/wordcount.py",
    "input_url":"nfs:///data/shared/samples/input.txt",
    "partitions": 3,
  }'
# → {"job_id":"job-1", "status":"QUEUED", ...}
```

**Consultar estado / artefactos / resultado:**
```bash
curl -s "$MASTER_URL/jobs/job-1"
curl -s "$MASTER_URL/jobs/job-1/ls"
curl -s "$MASTER_URL/jobs/job-1/result"
```

**Listar jobs y métricas:**
```bash
curl -s "$MASTER_URL/jobs" | jq .
curl -s "$MASTER_URL/metrics/jobs" | jq .
```

**Eliminar job (estado en memoria):**
```bash
curl -X DELETE "$MASTER_URL/jobs/job-1"
```

> Nota: los archivos en NFS (`/data/shared/jobs/<job_id>`) no se borran por defecto al `DELETE` (se puede habilitar si se requiere).

---

## 5. Otra información relevante

- **Concurrencia real**: el scheduler despacha múltiples MAP/REDUCE en paralelo (configurable).
- **Partitioner estable por key**: evita duplicados en `result.txt`; si aparecen, revisar que todos los workers usen la versión correcta del `run_map`.
- **SINGLE vs DISTRIBUTED**: hoy puede forzarse `force_distributed=true` para que todo pase por `Map/Reduce`, incluso en archivos pequeños.
- **Diagnóstico rápido**:
  - `GET /workers` → ring y direcciones registradas (evitar `0.0.0.0`).
  - `docker logs -f gridmr-master` / `gridmr-workerX` → traza completa.
  - Conectividad gRPC desde master: `grpcurl -plaintext <IP_WORKER>:50051 gridmr.Worker.Heartbeat`.

---

## referencias

- Enunciado y lineamientos del proyecto GridMR (ST0263 / SI3007), incluyendo objetivos, arquitectura Master–Workers, protocolos y criterios de evaluación.
- Dean, J., & Ghemawat, S. *MapReduce: Simplified Data Processing on Large Clusters.*
- Zaharia, M., et al. *Spark: Cluster Computing with Working Sets.*
- Sitios:  
  - Hadoop: https://hadoop.apache.org  
  - Spark: https://spark.apache.org
