FROM python:3.10-slim
WORKDIR /app
COPY worker.py .
RUN pip install fastapi uvicorn pydantic
CMD ["uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "8001"]