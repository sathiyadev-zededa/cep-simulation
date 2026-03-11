#!/bin/sh
# Environment variables (all have defaults):
#   MQTT_HOST      MQTT broker hostname        (default: mqtt-broker)
#   MQTT_PORT      MQTT broker port            (default: 1883)
#   OLLAMA_HOST    Ollama API base URL         (default: http://ollama:11434)
#   OLLAMA_MODEL   Model name to use           (default: phi3)
#   MAX_WAIT       Seconds to wait per service (default: 120)
#   INTERVAL       Seconds between retries     (default: 5)

set -e

# ── Config ────────────────────────────────────────────────────────────────────
MQTT_HOST="${MQTT_HOST:-mqtt-broker}"
MQTT_PORT="${MQTT_PORT:-1883}"
OLLAMA_HOST="${OLLAMA_HOST:-http://ollama:11434}"
OLLAMA_MODEL="${OLLAMA_MODEL:-phi3}"
MAX_WAIT="${MAX_WAIT:-120}"
INTERVAL="${INTERVAL:-5}"

# ── Banner ────────────────────────────────────────────────────────────────────
echo "========================================================"
echo "  LLM Incident Intelligence Service"
echo "========================================================"
echo "  MQTT:         ${MQTT_HOST}:${MQTT_PORT}"
echo "  Ollama:       ${OLLAMA_HOST}"
echo "  Model:        ${OLLAMA_MODEL}"
echo "  Max wait:     ${MAX_WAIT}s per dependency"
echo "========================================================"

# ── Helper: wait_for ──────────────────────────────────────────────────────────
# Usage: wait_for "label" check_command
# Retries check_command every INTERVAL seconds until it succeeds or MAX_WAIT.
wait_for() {
    label="$1"
    shift
    check_cmd="$*"

    elapsed=0
    echo "[WAIT] Waiting for ${label}..."

    until eval "$check_cmd" > /dev/null 2>&1; do
        if [ "$elapsed" -ge "$MAX_WAIT" ]; then
            echo "[WARN] ${label} not reachable after ${MAX_WAIT}s — continuing anyway."
            echo "[WARN] llm_service.py has built-in fallback handling."
            return 0
        fi
        echo "[WAIT] ${label} not ready — retry in ${INTERVAL}s (${elapsed}/${MAX_WAIT}s)"
        sleep "$INTERVAL"
        elapsed=$((elapsed + INTERVAL))
    done

    echo "[OK]   ${label} is ready."
}

# ── Step 1: Wait for MQTT broker ──────────────────────────────────────────────
# Use /dev/tcp to check TCP port — available in sh without extra tools.
# Falls back gracefully if broker is slow to start.
wait_for "MQTT broker (${MQTT_HOST}:${MQTT_PORT})" \
    "curl -sf --max-time 3 telnet://${MQTT_HOST}:${MQTT_PORT}"

# ── Step 2: Wait for Ollama API ───────────────────────────────────────────────
# /api/tags returns the list of downloaded models — confirms API is live.
wait_for "Ollama API (${OLLAMA_HOST})" \
    "curl -sf --max-time 5 ${OLLAMA_HOST}/api/tags"

# ── Step 3: Log model status ──────────────────────────────────────────────────
# Check if the model is already in Ollama's local store.
# llm_service.py will pull it automatically if missing — just informational here.
echo "[INFO] Checking if model '${OLLAMA_MODEL}' is already downloaded..."
MODEL_LIST=$(curl -sf --max-time 5 "${OLLAMA_HOST}/api/tags" 2>/dev/null || echo "")

if echo "$MODEL_LIST" | grep -q "${OLLAMA_MODEL}"; then
    echo "[OK]   Model '${OLLAMA_MODEL}' found in local store — fast startup."
else
    echo "[INFO] Model '${OLLAMA_MODEL}' not yet downloaded."
    echo "[INFO] llm_service.py will pull it automatically on first run (~2-3GB download)."
    echo "[INFO] This only happens ONCE — model is saved to the ollama_data volume."
fi

# ── Step 4: Start the service ─────────────────────────────────────────────────
# exec replaces this shell process with Python so signals (SIGTERM) are
# forwarded correctly — enables graceful shutdown with docker stop.
echo ""
echo "[START] Launching llm_service.py..."
echo "========================================================"
exec python llm_service.py
