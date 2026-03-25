# Dockerfile — llm_service (LLM Incident Intelligence)
#
# This container ONLY runs llm_service.py.
# Ollama runs as a SEPARATE container (see docker-compose.yml).
# This keeps the image small — no model weights baked in.
#
# What this container does NOT do:
#   ✗ Does NOT download or run the LLM model
#   ✗ Does NOT include Ollama runtime
#   ✗ Does NOT bake in model weights
#
# What this container DOES do:
#   ✓ Runs llm_service_v2.py (MQTT subscriber + ChromaDB + RAG pipeline)
#   ✓ Uses sentence-transformers all-MiniLM-L6-v2 for embeddings (offline, no API key)
#   ✓ torch pinned to 2.6.0+cpu — prevents version drift inflating the image
#   ✓ Pre-downloads all-MiniLM-L6-v2 at build time for fast startup
#   ✓ Waits for Ollama container to be ready before starting (entrypoint.sh)
#   ✓ Connects to Ollama via HTTP (OLLAMA_HOST env var)
#   ✓ Persists ChromaDB vector store to a Docker volume
#
# Image size: ~2.5GB (torch pinned to 2.6.0+cpu — matches original :1.0 baseline)
#
# Build:
#   docker build -t commonpoc.azurecr.io/llm_service:2.1 .
#
# Run (recommended — via Helm):
#   helm upgrade cep-edge ./edge-cep-demo -n cep-edge

# ── Stage 1: Build dependencies ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# gcc needed to compile onnxruntime / numpy C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime image ────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

COPY llm_service_v2.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Pre-download the sentence-transformers embedding model at build time — avoids first-run delay.
# all-MiniLM-L6-v2 is ~90MB, acceptable to bake in. torch is pinned to 2.6.0+cpu
# in requirements.txt to prevent version drift inflating the image size.
RUN python -c "\
from sentence_transformers import SentenceTransformer; \
print('Downloading all-MiniLM-L6-v2 embedding model...'); \
SentenceTransformer('all-MiniLM-L6-v2'); \
print('Embedding model ready.')"

# ChromaDB vector store — mount a volume so data persists across restarts
RUN mkdir -p /app/incident_store
VOLUME ["/app/incident_store"]

# ── Environment defaults ───────────────────────────────────────────────────────
ENV MQTT_HOST=mqtt-broker
ENV MQTT_PORT=1883
ENV OLLAMA_HOST=http://ollama:11434
ENV OLLAMA_MODEL=phi3
ENV CHROMA_PATH=/app/incident_store
ENV SITE_ID=SUBSTATION-ABU-01

# ── Startup ────────────────────────────────────────────────────────────────────
ENTRYPOINT ["./entrypoint.sh"]
