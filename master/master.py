import requests

# Datos de prueba
task = {
    "task_id": "001",
    "operation": "sum",
    "data": [1, 2, 3, 4, 5]
}

# URL del worker (ajusta IP si está en otra máquina)
WORKER_URL = "http://localhost:8001/execute"

print(f"Enviando tarea al worker: {task}")
response = requests.post(WORKER_URL, json=task)
print(f"Respuesta del worker: {response.json()}")