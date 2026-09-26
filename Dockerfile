FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (build-essential needed for some C extensions if compiling zfec or similar)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY apps ./apps
COPY vault_core ./vault_core
COPY config ./config
RUN pip install --no-cache-dir -e . && \
    python -c "import apps.gateway.main; import apps.storage_node.main; import apps.metadata_node.main"

COPY scripts ./scripts
COPY docs ./docs

ENV PYTHONPATH=/app
EXPOSE 8000 8001 9001 9002
