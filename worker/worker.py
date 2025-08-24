from fastapi import FastAPI, Request
from pydantic import BaseModel
import uvicorn

app = FastAPI()

class Task(BaseModel):
    task_id: str
    operation: str
    data: list

@app.post("/execute")
async def execute_task(task: Task):
    print(f"Received task {task.task_id}")
    # Procesar la tarea: ejemplo con suma
    if task.operation == "sum":
        result = sum(task.data)
    else:
        result = "Operation not supported"
    return {"task_id": task.task_id, "result": result}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)