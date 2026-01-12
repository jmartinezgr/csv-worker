# Worker Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dependencias mínimas del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instala requirements (buildando desde dentro de /worker)
COPY requirements-worker.txt /app/requirements-worker.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements-worker.txt

# Copia todo el código del worker (build context es ./worker)
COPY . /app

# Variables con defaults (puedes sobreescribir al correr)
ENV BATCH_SIZE=5000 \
    BLOCK_SIZE_BYTES=16777216

ENTRYPOINT ["python", "-m", "main"]