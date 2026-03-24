# CEP Edge Intelligence — Power Substation Fault Detection

A real-time **Complex Event Processing (CEP)** simulation platform for power distribution substations. Detects faults, correlates telemetry events, generates AI-powered incident reports, and visualises everything on a live dashboard — all running locally on Kubernetes.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services](#services)
- [MQTT Topics](#mqtt-topics)
- [Fault Types](#fault-types)
- [AI Intelligence Layer](#ai-intelligence-layer)
- [CEP Engine (Rust)](#cep-engine-rust)
- [Quick Start (Minikube)](#quick-start-minikube)
- [Deployment (NVIDIA Jetson)](#deployment-nvidia-jetson)
- [Helm Configuration](#helm-configuration)
- [Dashboard](#dashboard)
- [Troubleshooting](#troubleshooting)

---

## Overview

```
Simulator → MQTT Broker → CEP Engine (Rust) → Incidents → LLM Service → Reports
                ↓                                  ↓                        ↓
           Dashboard UI ←──────────── WebSocket bridge ────────────────────←
```

The platform simulates a power distribution substation (`SUBSTATION-ABU-01`) with:

- Continuous telemetry (voltage, current, frequency, power factor)
- One-click **transient fault** injection (momentary sag, relay trip, auto-reclose)
- One-click **permanent fault** injection (4 realistic scenarios with relay codes)
- 13 CEP rules in a **Rust engine** that fires incidents when fault patterns are detected
- RAG-based LLM reports using ChromaDB + Sentence Transformers + Ollama

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                        │
│                                                                  │
│  ┌─────────────┐     ┌──────────────┐     ┌──────────────────┐  │
│  │  Simulator  │────▶│ MQTT Broker  │────▶│  CEP Engine      │  │
│  │ simulator   │     │  (Mosquitto) │     │  Rust · ~10 MB   │  │
│  │ _v2.py      │     │  port 1883   │     │  13 rules        │  │
│  └─────────────┘     └──────┬───────┘     │  WindowBuffer    │  │
│                             │             │  IncidentDedup   │  │
│                             │             └────────┬─────────┘  │
│                             │                      │incidents   │
│                             │             ┌────────▼─────────┐  │
│                             │             │   LLM Service    │  │
│                             │             │  ChromaDB (RAG)  │  │
│                             │             │  Ollama / phi3   │  │
│                             │             └────────┬─────────┘  │
│                             │                      │reports     │
│                             ▼                      ▼            │
│                    ┌─────────────────────────────────────────┐  │
│                    │         Dashboard UI (FastAPI)           │  │
│                    │    WebSocket bridge  ·  port 8080        │  │
│                    └─────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Services

| Service | Image | Role |
|---------|-------|------|
| `mqtt-broker` | `eclipse-mosquitto` | Central message bus for all event data |
| `simulator` | custom (Python) | Generates telemetry + fault events on button click |
| `cep-engine` | custom (**Rust**) | Evaluates 13 rules, publishes incidents — ~10 MB image, ~4 MB RAM |
| `dashboard-ui` | custom (Python) | FastAPI + WebSocket + HTML live dashboard |
| `ollama` | `ollama/ollama` | Local LLM server (phi3 / gemma2:2b) |
| `llm-service` | custom (Python) | RAG pipeline — ChromaDB + Ollama → reports |

---

## MQTT Topics

| Topic | Publisher | Subscriber | Content |
|-------|-----------|------------|---------|
| `edge/events` | simulator | cep-engine, llm-service | Raw telemetry and fault events |
| `edge/incidents` | cep-engine | llm-service, dashboard-ui | Detected incidents with severity + confidence |
| `edge/commands` | cep-engine | simulator | Auto-reclose and isolation commands |
| `edge/summaries` | llm-service | dashboard-ui | 10-second rolling plain-English narrative |
| `edge/reports` | llm-service | dashboard-ui | Full structured incident reports |

### Event payload structure

```json
{
  "id": "1741862400000-a3f2c1",
  "event_type": "relay_trip",
  "ts_ms": 1741862400000,
  "site_id": "SUBSTATION-ABU-01",
  "feeder_id": "FEEDER-12",
  "payload": {
    "relay_code": "50/51",
    "v_sag_kv": 3.2,
    "i_fault_a": 4800
  }
}
```

### Incident payload structure

```json
{
  "id": "inc-1741862400000",
  "event_type": "incident",
  "ts_ms": 1741862400000,
  "payload": {
    "incident_type": "PERMANENT_FAULT",
    "severity": "CRITICAL",
    "confidence": 0.95,
    "summary": "Breaker lockout on FEEDER-12 after 3 failed reclose attempts",
    "feeder_id": "FEEDER-12",
    "details": { ... }
  }
}
```

---

## Fault Types

### Transient Fault
A brief electrical disturbance that clears on its own. The simulator fires a voltage sag, relay trip, and automatic reclose sequence.

| Step | Event | Delay |
|------|-------|-------|
| 1 | `voltage_sag` — V drops to fault level | 0s |
| 2 | `relay_trip` — protection relay opens breaker | 0.5s |
| 3 | `breaker_reclose` — auto-reclose attempt | 2s |
| 4 | Telemetry returns to normal | 3s |

### Permanent Fault
A sustained fault that cannot be cleared by auto-reclose. Four realistic scenarios are randomly selected:

| Scenario | Relay Codes | Key Events |
|----------|-------------|------------|
| `BREAKER_LOCKOUT` | 50/51 | 3 failed reclose attempts → lockout |
| `TRANSFORMER_WINDING_FAILURE` | 87T / 63 | Differential + gas relay → equipment offline |
| `CABLE_INSULATION_FAILURE` | 21 / 67 | Distance + directional → zone isolation |
| `SWITCHGEAR_BUSBAR_FAULT` | 87B / 50BF | Busbar differential → all 3 feeders down |

---

## AI Intelligence Layer

The LLM service implements a **RAG (Retrieval-Augmented Generation)** pipeline:

```
New Incident
     │
     ▼
ChromaDB query ──▶ Find top-3 similar past incidents (cosine similarity)
     │
     ▼
Prompt builder ──▶ Incident data + RAG context + recent events (last 30s)
     │
     ▼
Ollama / phi3  ──▶ Generate structured 6-section report
     │
     ▼
ChromaDB store ──▶ Save report as embedding for future RAG
     │
     ▼
edge/reports   ──▶ Publish to dashboard LLM Intelligence tab
```

### Report sections

1. **Root Cause Analysis** — what caused the incident based on telemetry
2. **Immediate Impact** — affected assets and customers
3. **Recommended Actions** — step-by-step operator instructions
4. **Estimated Resolution Time** — based on incident type and history
5. **Historical Comparison** — similar past incidents from ChromaDB
6. **Preventive Measures** — recurrence reduction recommendations

### Supported models

| Model | RAM | Speed (CPU) | Quality |
|-------|-----|-------------|---------|
| `phi3` | 3.5GB | 60–120s | Best |
| `gemma2:2b` | 2.0GB | 30–60s | Good |
| `qwen2.5:1.5b` | 1.2GB | 15–30s | Decent |
| `tinyllama` | 0.9GB | 10–20s | Basic |

Change model at any time without redeploying code:
```bash
kubectl set env deployment/llm-service -n cep-edge OLLAMA_MODEL=gemma2:2b
```

---

## CEP Engine (Rust)

The CEP engine is written in **Rust** for minimal resource usage and fast startup. It replaces the original Python `cep_engine.py`.

### Project structure

```
cep-engine-rust/
├── Cargo.toml           # tokio · rumqttc · serde · dashmap · uuid · tracing
├── Dockerfile           # multi-stage: rust:1.85-slim → scratch (~10 MB)
├── k8s-deployment.yaml  # k8s deployment manifest
└── src/
    ├── main.rs          # MQTT wiring, env config, async event loop
    ├── types.rs         # EdgeEvent, Incident, IncidentPayload structs
    ├── rules.rs         # all 13 CEP rules with priority ordering
    ├── window.rs        # sliding time-window buffer (DashMap, lock-free)
    ├── dedup.rs         # incident dedup / 20-second cooldown
    └── comms_loss.rs    # background COMMS_LOSS detector (15s timeout)
```

### Resource comparison

| | Python `cep_engine.py` | Rust `cep-engine` |
|---|---|---|
| Image size | ~800 MB | ~10 MB |
| RAM at runtime | ~120 MB | ~4 MB |
| Startup time | ~3s | <50ms |
| CPU (idle) | ~5m cores | ~0.5m cores |

### Build on Mac M4

Mac M4 and Jetson are both **ARM64** — the same binary runs on both with no cross-compilation:

```bash
# Install Rust (once)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustup target add aarch64-unknown-linux-musl
brew install filosottile/musl-cross/musl-cross

# Local build (for development and testing)
cd cep-engine-rust
cargo build --release

# Docker build (for Minikube and Jetson)
eval $(minikube docker-env)
docker build -t cep-engine-rust:1.0.0 .
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_HOST` | `mqtt-broker` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `SITE_ID` | `SUBSTATION-ABU-01` | Substation identifier |
| `RUST_LOG` | `info` | Log level (`debug` / `info` / `warn`) |

---

## Quick Start (Minikube)

### Prerequisites

- Docker Desktop
- minikube
- kubectl
- helm
- Rust (for local development only)

### 1 — Start Minikube with enough memory

```bash
minikube start --memory=6144 --cpus=4
```

### 2 — Build the Rust CEP engine image

```bash
eval $(minikube docker-env)
cd cep-engine-rust
docker build -t cep-engine-rust:1.0.0 .
```

### 3 — Deploy with Helm

```bash
helm install cep-edge ./charts/cep-edge -n cep-edge --create-namespace
```

### 4 — Wait for all pods to be ready

```bash
kubectl get pods -n cep-edge -w
```

All 6 pods should reach `Running` state. The `llm-service` pod has two init containers (`wait-for-mqtt`, `wait-for-ollama`) that must complete first.

### 5 — Open the dashboard

```bash
minikube service dashboard-ui -n cep-edge
```

### 6 — Trigger a fault

Click **Permanent Fault** or **Transient Fault** on the dashboard. Within 10–120 seconds (depending on model) you will see:

- **Incidents tab** — incident card with type, severity, confidence
- **LLM Intelligence tab** — full structured report from Ollama

---

## Deployment (NVIDIA Jetson)

Mac M4 and Jetson Orin/NX are both **ARM64** — the same Docker image built on your Mac runs directly on the Jetson with no changes.

### Prerequisites on Jetson

```bash
# Install k3s
curl -sfL https://get.k3s.io | sh -

# Install NVIDIA Container Toolkit
sudo apt install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=containerd
sudo systemctl restart containerd k3s
```

### Transfer images from Mac to Jetson

```bash
# Save the Rust CEP engine image built on Mac M4
docker save cep-engine-rust:1.0.0 | gzip > cep-engine-rust.tar.gz

# Copy to Jetson and load into k3s
scp cep-engine-rust.tar.gz user@jetson-ip:~
ssh user@jetson-ip "sudo k3s ctr images import cep-engine-rust.tar.gz"
```

### Deploy with GPU enabled

```bash
helm install cep-edge ./charts/cep-edge \
  -n cep-edge --create-namespace \
  -f values-jetson.yaml
```

With GPU, phi3 inference drops from **60–120s → 3–8s**.

---

## Helm Configuration

### `values.yaml` reference

```yaml
# MQTT Broker
mqtt:
  image: eclipse-mosquitto:2
  port: 1883

# Simulator
simulator:
  image: your-registry/cep-simulator:latest
  resources:
    limits:
      memory: 256Mi

# CEP Engine (Rust) — very low resource footprint
cepEngine:
  image:
    repository: cep-engine-rust
    tag: "1.0.0"
    pullPolicy: IfNotPresent
  env:
    MQTT_HOST: mqtt-broker
    MQTT_PORT: "1883"
    SITE_ID: SUBSTATION-ABU-01
    RUST_LOG: info
  resources:
    requests:
      cpu: "50m"
      memory: "32Mi"
    limits:
      cpu: "200m"
      memory: "128Mi"

# Dashboard
dashboard:
  image: your-registry/dashboard-ui:latest
  port: 8080
  resources:
    limits:
      memory: 256Mi

# Ollama LLM Server
ollama:
  image: ollama/ollama:latest
  model: phi3                    # change to gemma2:2b for lower memory
  storage: 10Gi                  # PVC for model persistence
  gpu:
    enabled: false               # set true on Jetson
    count: 1
  resources:
    requests:
      cpu: "1"
      memory: 4Gi
    limits:
      cpu: "2"
      memory: 6Gi

# LLM Service
llmService:
  image: your-registry/llm-service:latest
  chromaPath: /data/incident_store
  summaryIntervalS: 10
  ragTopK: 3
  resources:
    limits:
      memory: 1Gi
```

### Updating the CEP engine image

The only required `values.yaml` change when upgrading the Rust engine is the image tag:

```yaml
cepEngine:
  image:
    repository: cep-engine-rust
    tag: "1.1.0"    # bump this
```

### Memory sizing by model

| Model | `requests.memory` | `limits.memory` |
|-------|-------------------|-----------------|
| phi3 | 4Gi | 6Gi |
| gemma2:2b | 2Gi | 3Gi |
| qwen2.5:1.5b | 1Gi | 2Gi |
| tinyllama | 512Mi | 1Gi |

> **Rule of thumb:** `requests` = model file size on disk. `limits` = requests × 1.5.

---

## Dashboard

### Incidents Tab

Displays every incident fired by the CEP engine in real time.

| Field | Description |
|-------|-------------|
| Type | Incident classification (e.g. `PERMANENT_FAULT`) |
| Severity | `CRITICAL` / `HIGH` / `MEDIUM` / `LOW` |
| Confidence | 0–100% — how certain the CEP engine is |
| Feeder | Affected feeder ID |
| Summary | One-line human-readable description |
| Raw JSON | Full incident payload for debugging |

### LLM Intelligence Tab

Displays AI-generated reports from Ollama. Each report includes root cause, impact, recommended actions, resolution time, historical comparison, and preventive measures.

A **Live Narration** panel updates every 10 seconds with a plain-English summary of current substation activity.

---

## CEP Engine Rules

The Rust engine evaluates 13 rules in priority order. Priority 1–2 rules trigger an early return so lower-priority rules do not fire on the same event.

| Priority | Rule | Trigger | Severity |
|----------|------|---------|----------|
| 1 | `BUSBAR_FAULT` | Relay 87B or 50BF | CRITICAL |
| 2 | `TRANSFORMER_FAULT` | Relay 87T or 63 | CRITICAL |
| 3 | `DISTANCE_FAULT` | Relay 21 or 67 | HIGH |
| 4 | `OVERCURRENT_FAULT` | Relay 50 or 51 | HIGH |
| 5 | `BREAKER_LOCKOUT` | 3+ failed reclosures in 30s | CRITICAL |
| 6 | `RECLOSE_SUCCESS` | Reclose after relay trip | LOW |
| 7 | `EQUIPMENT_OFFLINE` | Asset goes offline | HIGH |
| 8 | `ZONE_ISOLATION` | Zone isolation command | HIGH |
| 9 | `TRANSFORMER_OVERLOAD` | Load > 105% rated capacity | HIGH / CRITICAL |
| 10 | `PRE_FAULT_WARNING` | V sag > 10% or I spike > 20% | MEDIUM |
| 11 | `VOLTAGE_QUALITY` | 3+ harmonic events in 60s | MEDIUM |
| 12 | `LIGHTNING_IMPACT` | Lightning strike event | HIGH |
| 13 | `FREQUENCY_DEVIATION` | 2+ frequency events in 60s | MEDIUM |

Incidents are deduplicated with a **20-second cooldown** per `incident_type + feeder_id` pair. A background task fires `COMMS_LOSS` if no telemetry is received from a feeder for more than **15 seconds**.

---

## Troubleshooting

### Incidents tab is empty

```bash
# Confirm CEP engine is publishing
kubectl exec -n cep-edge deploy/mqtt-broker -- \
  mosquitto_sub -t edge/incidents -W 10 -v

# Check CEP engine logs (Rust uses structured tracing logs)
kubectl logs -n cep-edge -l app=cep-engine --tail=50
# Expected output:
# INFO cep_engine: [CEP] Connected to MQTT broker at mqtt-broker:1883
# INFO cep_engine: [CEP] Subscribed to edge/events
```

### CEP engine pod not starting

```bash
kubectl describe pod -n cep-edge -l app=cep-engine
# Common cause: wrong image name or tag in values.yaml
# Fix: ensure cepEngine.image.repository and tag match the built image
```

### LLM reports not appearing

```bash
# Check llm-service is receiving incidents
kubectl logs -n cep-edge -l app=llm-service --tail=50 | grep -E "Incident|REPORT|error"

# Confirm Ollama has the model
kubectl exec -n cep-edge \
  $(kubectl get pod -n cep-edge -l app=ollama -o jsonpath='{.items[0].metadata.name}') \
  -- ollama list
```

### phi3 / model OOM crash

Check memory:
```bash
kubectl describe pod -n cep-edge -l app=ollama | grep -E "OOM|Exit Code|Limits"
```

Fix — switch to a smaller model:
```bash
kubectl set env deployment/llm-service -n cep-edge OLLAMA_MODEL=gemma2:2b
kubectl exec -n cep-edge <ollama-pod> -- ollama pull gemma2:2b
```

### Model lost after pod restart

The Ollama PVC is not mounted at `/root/.ollama`. Verify:
```bash
kubectl exec -n cep-edge <ollama-pod> -- df -h /root/.ollama
# Should show a real volume, not "overlay"
```

Fix in Helm values:
```yaml
ollama:
  persistentVolume:
    mountPath: /root/.ollama
```

### Docker build fails with `edition2024` error

The Dockerfile requires Rust 1.85+. Verify the first line of `Dockerfile`:
```dockerfile
FROM rust:1.85-slim AS builder   # must be 1.85 or newer
```

### paho-mqtt version errors (llm-service / simulator)

All Python services detect paho v1/v2 automatically:
```python
_PAHO_V2 = hasattr(mqtt, "CallbackAPIVersion")
```
No manual version management needed.

---

## Environment Variables

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `MQTT_HOST` | all | `mqtt-broker` | MQTT broker hostname |
| `MQTT_PORT` | all | `1883` | MQTT broker port |
| `RUST_LOG` | cep-engine | `info` | Rust log level (`debug`/`info`/`warn`) |
| `SITE_ID` | cep-engine, simulator | `SUBSTATION-ABU-01` | Substation identifier |
| `OLLAMA_HOST` | llm-service | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_MODEL` | llm-service | `phi3` | Model name |
| `CHROMA_PATH` | llm-service | `./incident_store` | ChromaDB persistence path |
| `SUMMARY_INTERVAL_S` | llm-service | `10` | Narrative summary frequency |
| `RAG_TOP_K` | llm-service | `3` | Similar incidents to retrieve |

---

## License

MIT
