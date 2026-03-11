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
#   ✓ Runs llm_service.py (MQTT subscriber + ChromaDB + RAG pipeline)
#   ✓ Downloads all-MiniLM-L6-v2 (~90MB) at build time for fast startup
#   ✓ Waits for Ollama container to be ready before starting (entrypoint.sh)
#   ✓ Connects to Ollama via HTTP (OLLAMA_HOST env var)
#   ✓ Persists ChromaDB vector store to a Docker volume
#
# Build:
#   docker build -f Dockerfile.llm-service -t llm-service:1.0.0 .
#
# Run (recommended — via docker-compose):
#   docker-compose up
#
# Run (standalone — requires Ollama and MQTT running separately):
#   docker run \
#     -e MQTT_HOST=192.168.1.10 \
#     -e OLLAMA_HOST=http://192.168.1.10:11434 \
#     -e OLLAMA_MODEL=phi3 \
#     -v chromadb_data:/app/incident_store \
#     llm-service:1.0.0

# ── Stage 1: Build dependencies ───────────────────────────────────────────────
# Use a full image to compile C extensions (chromadb, tokenizers)
FROM python:3.11-slim AS builder

WORKDIR /build

# System packages required to compile sentence-transformers / chromadb deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps into a prefix directory
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime image ────────────────────────────────────────────────────
# Copy only the installed packages — keeps final image lean
FROM python:3.11-slim AS runtime

WORKDIR /app

# curl is needed by entrypoint.sh to health-check the Ollama API
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder stage
COPY --from=builder /install /usr/local

# Copy application code
COPY llm_service.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Pre-download the embedding model at BUILD time so container starts instantly.
# all-MiniLM-L6-v2 is only ~90MB — acceptable to bake into the image.
# The LLM (phi3) is NOT downloaded here — that happens via Ollama at runtime.
RUN python -c "\
from sentence_transformers import SentenceTransformer; \
print('Downloading all-MiniLM-L6-v2 embedding model...'); \
SentenceTransformer('all-MiniLM-L6-v2'); \
print('Embedding model ready.')"

# ChromaDB vector store — mount a volume here so data persists across restarts
RUN mkdir -p /app/incident_store
VOLUME ["/app/incident_store"]

# ── Environment defaults ───────────────────────────────────────────────────────
# Override all of these in docker-compose.yml or Helm values.yaml

# MQTT broker address (service name in docker-compose)
ENV MQTT_HOST=mqtt-broker
ENV MQTT_PORT=1883

# Ollama container address (service name in docker-compose)
ENV OLLAMA_HOST=http://ollama:11434

# LLM model to use — phi3 recommended for 4GB RAM devices
# Other options: llama3.2:1b (smaller/faster), mistral (larger/better)
ENV OLLAMA_MODEL=phi3

# Where ChromaDB stores its vector index on disk
ENV CHROMA_PATH=/app/incident_store

# Site identifier included in every published report
ENV SITE_ID=SUBSTATION-ABU-01

# ── Startup ────────────────────────────────────────────────────────────────────
# entrypoint.sh polls Ollama API until healthy, then starts llm_service.py
ENTRYPOINT ["./entrypoint.sh"]
