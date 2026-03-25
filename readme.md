# CEP Edge Intelligence — Power Substation Fault Detection

A real-time **Complex Event Processing (CEP)** simulation platform for power distribution substations. Detects faults, correlates telemetry patterns, aggregates related incidents into fault events, and generates AI-powered temporal reports — all running on Kubernetes at the edge.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services](#services)
- [MQTT Topics](#mqtt-topics)
- [CEP Engine (Rust DSL)](#cep-engine-rust-dsl)
- [CEP Rules](#cep-rules)
- [Fault Event Aggregator](#fault-event-aggregator)
- [AI Intelligence Layer](#ai-intelligence-layer)
- [Dashboard Scenarios](#dashboard-scenarios)
- [Quick Start](#quick-start)
- [Helm Configuration](#helm-configuration)
- [Image Tags](#image-tags)
- [Troubleshooting](#troubleshooting)
- [Environment Variables](#environment-variables)

---

## Overview

```
Simulator → MQTT Broker → CEP Engine (Rust DSL) → edge/incidents  → Dashboard UI
                                   │
                                   └─ FaultEventAggregator → edge/fault_events → LLM Service → Reports
```

The platform simulates `SUBSTATION-ABU-01` with three feeders (FEEDER-12, FEEDER-13, FEEDER-14):

- Continuous background telemetry on all feeders (voltage, current, frequency, temperature, power factor)
- One-click **scenario injection** — 9 fault scenarios across Critical, High, and Temporal categories
- **17 CEP rules** in a YAML-driven Rust engine with Pattern A (sustained) and Pattern B (window) detection
- **Dual-channel publishing** — all incidents to `edge/incidents` (dashboard), aggregated HIGH/CRITICAL to `edge/fault_events` (LLM)
- **RAG-based LLM reports** using ChromaDB + Sentence Transformers + Ollama — only for CRITICAL/HIGH fault events, with full temporal incident timeline

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Kubernetes Cluster                              │
│                                                                          │
│  ┌─────────────┐     ┌──────────────┐     ┌──────────────────────────┐  │
│  │  Simulator  │────▶│ MQTT Broker  │────▶│      CEP Engine          │  │
│  │  v4 (Python)│     │  Mosquitto   │     │   Rust · YAML DSL        │  │
│  │  9 scenarios│     │  port 1883   │     │   17 rules               │  │
│  └─────────────┘     └──────┬───────┘     │   WindowBuffer (Pattern B)│  │
│                             │             │   SustainedTracker (Pat.A)│  │
│                             │             │   FaultEventAggregator   │  │
│                             │             │   IncidentDedup (20s)    │  │
│                             │             │   FaultSuppression (60s) │  │
│                             │             └──────┬─────────┬─────────┘  │
│                             │                    │         │            │
│                             │             edge/incidents  edge/fault_events
│                             │                    │         │            │
│                             │                    │   ┌─────▼──────────┐ │
│                             │                    │   │   LLM Service  │ │
│                             │                    │   │  ChromaDB RAG  │ │
│                             │                    │   │  Ollama/gemma2 │ │
│                             │                    │   └─────┬──────────┘ │
│                             │                    │         │ edge/reports│
│                             ▼                    ▼         ▼            │
│                    ┌────────────────────────────────────────────────┐   │
│                    │          Dashboard UI (FastAPI + WebSocket)     │   │
│                    │   Fault Events · Telemetry · Incidents · LLM   │   │
│                    └────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Services

| Service | Image | Role |
|---------|-------|------|
| `mqtt-broker` | `eclipse-mosquitto:2` | Central message bus |
| `simulator` | `simulator-arm64:3.1` | Telemetry + scenario event injection |
| `cep-engine` | `cep-engine-arm64:3.1` | Rust CEP engine — 17 YAML rules, dual-channel publish |
| `dashboard-ui` | `dashboard-ui-arm64:3.0` | FastAPI + WebSocket live dashboard |
| `ollama` | `ollama/ollama:latest` | Local LLM inference server |
| `llm-service` | `llm_service:2.1` | RAG pipeline — fault event reports only |

---

## MQTT Topics

| Topic | Publisher | Subscriber | Content |
|-------|-----------|------------|---------|
| `edge/events` | simulator | cep-engine, llm-service | Raw telemetry and fault events |
| `edge/incidents` | cep-engine | dashboard-ui | All detected incidents (every severity) |
| `edge/fault_events` | cep-engine (aggregator) | llm-service, dashboard-ui | Aggregated HIGH/CRITICAL fault events with root cause + contributing factors |
| `edge/commands` | cep-engine | simulator | Auto-reclose and isolation commands |
| `edge/summaries` | llm-service | dashboard-ui | 10-second rolling plain-English narrative |
| `edge/reports` | llm-service | dashboard-ui | Full structured LLM incident reports |

### Why two incident channels?

`edge/incidents` carries every CEP firing (including MEDIUM/LOW) — useful for the dashboard's All Incidents tab. `edge/fault_events` is a separate aggregated channel: the `FaultEventAggregator` collects HIGH/CRITICAL incidents per feeder within a 5-second window, selects the highest-severity as the root cause, and publishes a single structured `FaultEvent`. This keeps the LLM focused on serious faults without noise from routine alerts.

### FaultEvent payload structure

```json
{
  "event_type": "fault_event",
  "ts_ms": 1741862400000,
  "feeder_id": "FEEDER-12",
  "root_cause": "TRANSFORMER_OVERLOAD",
  "severity": "CRITICAL",
  "confidence": 0.92,
  "message": "Transformer TX-12 load warning: 122% of rated capacity...",
  "contributing_factors": ["TRANSFORMER_TEMPERATURE", "TRANSFORMER_OVERHEATING_SUSTAINED"],
  "incident_count": 3
}
```

---

## CEP Engine (Rust DSL)

The engine is written in **Rust** with rules defined in `rules.yaml` — no recompilation needed to add or change rules.

### Project structure

```
cep-engine-rust-dsl/
├── Cargo.toml            # tokio · rumqttc · serde_yaml · dashmap · uuid · tracing
├── Dockerfile            # multi-stage: rust:1.85-slim → debian-slim (~25 MB)
├── rules.yaml            # all 17 CEP rules — edit without recompiling
└── src/
    ├── main.rs           # MQTT wiring, event loop, dual-channel publish
    ├── types.rs          # EdgeEvent, Incident, FaultEvent structs
    ├── config.rs         # YAML DSL: Rule, Condition, SustainedConfig, FireConfig
    ├── rules.rs          # Rule evaluation loop (priority-ordered)
    ├── window.rs         # Sliding time-window buffer (Pattern B)
    ├── sustained.rs      # Sustained condition tracker (Pattern A)
    ├── aggregator.rs     # FaultEventAggregator — 5s collection window
    ├── dedup.rs          # Incident dedup — 20s cooldown per type+feeder
    └── comms_loss.rs     # Background COMMS_LOSS detector (15s timeout)
```

### Resource footprint

| | Python engine | Rust DSL engine |
|---|---|---|
| Image size | ~800 MB | ~25 MB |
| RAM at runtime | ~120 MB | ~6 MB |
| Startup time | ~3s | <50ms |

### Rule DSL

Rules are defined in `rules.yaml`. The engine supports:

- **`where`** — AND conditions across any payload field (`lt`, `lte`, `gt`, `gte`, `eq`, `in`, `bool`)
- **`window`** — Pattern B: count events in a sliding time window (`bucket`, `seconds`, `count_gte`)
- **`sustained`** — Pattern A: condition must be continuously true for N seconds (`key`, `for_secs`)
- **`escalate`** — severity promotion when additional conditions are met
- **`clear_window_on_fire`** — reset the bucket after the rule fires
- **`early_return`** — stop evaluating lower-priority rules after this one fires

Example rule (Pattern A — sustained condition):

```yaml
- id: transformer_overheating_sustained
  priority: 15
  on: temperature_warning
  sustained:
    key: tx_temp_high
    for_secs: 60
  fire:
    type: TRANSFORMER_OVERHEATING_SUSTAINED
    severity: HIGH
    confidence: 0.90
    message: "Transformer chronic overheating confirmed on {feeder_id} — sustained 60+ seconds, current {temperature_c}°C"
```

Example rule (compound AND condition + 2-second window):

```yaml
- id: severe_grid_disturbance
  priority: 17
  on: telemetry
  where:
    frequency_hz: {lt: 49.5}
    v_kv: {lt: 10.45}           # 0.95 pu × 11kV nominal
  window:
    bucket: grid_disturbance
    seconds: 2
    count_gte: 2
  fire:
    type: SEVERE_GRID_DISTURBANCE
    severity: CRITICAL
    confidence: 0.93
    message: "Severe grid disturbance on {feeder_id}: {frequency_hz} Hz and {v_kv} kV (below 0.95 pu)"
  clear_window_on_fire: true
```

---

## CEP Rules

17 rules evaluated in priority order. Rules with `early_return: true` stop evaluation for lower-priority rules on the same event.

| Priority | Rule | Event Type | Detection | Severity |
|----------|------|-----------|-----------|----------|
| 1 | `BUSBAR_FAULT` | relay_trip | 87B or 50BF code | CRITICAL |
| 2 | `TRANSFORMER_FAULT` | relay_trip | 87T or 63 code | CRITICAL |
| 3 | `DISTANCE_FAULT` | relay_trip | 21 or 67 code | HIGH |
| 4 | `OVERCURRENT_FAULT` | relay_trip | 50, 51, or 50/51 code | HIGH / CRITICAL |
| 5 | `BREAKER_LOCKOUT` | breaker_open | 3+ failures in 30s (Pattern B) | CRITICAL |
| 6 | `RECLOSE_SUCCESS` | breaker_close | reclose after relay trip | LOW |
| 7 | `EQUIPMENT_OFFLINE` | equipment_offline | asset goes offline | HIGH |
| 8 | `ZONE_ISOLATION` | isolation_command | zone isolation command | HIGH |
| 9 | `TRANSFORMER_OVERLOAD` | telemetry | load > 105% rated | HIGH / CRITICAL |
| 10 | `TRANSFORMER_TEMPERATURE` | temperature_warning | temp > 90°C | MEDIUM |
| 11 | `PRE_FAULT_WARNING` | telemetry | V sag > 10% or I spike > 20% | MEDIUM |
| 12 | `VOLTAGE_QUALITY` | harmonic_distortion | 3+ harmonic events in 60s (Pattern B) | MEDIUM |
| 13 | `LIGHTNING_IMPACT` | weather_lightning | lightning strike event | HIGH |
| 14 | `FREQUENCY_DEVIATION` | frequency_deviation | 2+ freq events in 60s (Pattern B) | MEDIUM |
| 15 | `TRANSFORMER_OVERHEATING_SUSTAINED` | temperature_warning | temp alarm sustained 60s (Pattern A) | HIGH |
| 16 | `REPEATED_TRANSIENT_FAULT` | relay_trip | 2+ overcurrent trips in 30s (Pattern B) | HIGH |
| 17 | `SEVERE_GRID_DISTURBANCE` | telemetry | freq < 49.5 Hz AND voltage < 0.95 pu in 2s window | CRITICAL |

**Dedup:** 20-second cooldown per `incident_type + feeder_id`. **Fault suppression:** 60 seconds per feeder after a CRITICAL fires (prevents cascade noise). **COMMS_LOSS:** fires if no telemetry from a feeder for > 15 seconds.

---

## Fault Event Aggregator

The `FaultEventAggregator` runs as a background task in the CEP engine. It:

1. Collects HIGH and CRITICAL incidents per feeder within a **5-second window**
2. Selects the highest-severity incident as the **root cause**
3. Lists all others as **contributing factors**
4. Publishes a single `FaultEvent` to `edge/fault_events`

This means a transformer stress scenario that fires `TRANSFORMER_OVERLOAD` (CRITICAL) + `TRANSFORMER_TEMPERATURE` (MEDIUM) + `TRANSFORMER_OVERHEATING_SUSTAINED` (HIGH) within 5 seconds produces **one** fault event, not three LLM reports.

---

## AI Intelligence Layer

The LLM service generates reports **only for HIGH/CRITICAL fault events** on `edge/fault_events`. Individual incidents on `edge/incidents` are silently buffered for timeline context — no per-incident LLM calls.

### Pipeline

```
edge/fault_events (FaultEvent)
        │
        ├─ on_incident() → incident_buffer (120s window, no report)
        │
        └─ on_fault_event() [HIGH/CRITICAL only]
                │
                ├─ RAG: ChromaDB cosine similarity → top-3 similar past reports
                │
                ├─ Prompt builder:
                │     FAULT EVENT summary
                │     INCIDENT TIMELINE (all buffered incidents, timestamped)
                │     RAW TELEMETRY (load%, temp, freq over 120s window)
                │     SIMILAR PAST INCIDENTS (RAG context)
                │
                ├─ Ollama → generate 6-section temporal report (max 1200 tokens)
                │
                ├─ ChromaDB store → save for future RAG
                │
                └─ edge/reports → publish to dashboard LLM Intelligence tab
```

### Report sections

1. **Fault Origin & Progression** — how the fault developed over time with specific timestamps
2. **Root Cause** — most likely cause in one or two sentences
3. **Asset & Customer Impact** — affected transformer/feeder and customer risk
4. **Immediate Actions** — numbered priority-ordered operator steps
5. **Historical Comparison** — similar past incidents from ChromaDB
6. **Preventive Measures** — specific recurrence-reduction actions

### Supported models

| Model | RAM | Speed (CPU) | Quality |
|-------|-----|-------------|---------|
| `phi3` | 3.5GB | 60–120s | Best |
| `gemma2:2b` | 2.0GB | 30–60s | Good |
| `llama3.2:1b` | 1.2GB | 15–30s | Decent |
| `tinyllama` | 0.9GB | 10–20s | Basic |

Change model without redeploying:
```bash
kubectl set env deployment/llm-service -n cep-edge OLLAMA_MODEL=gemma2:2b
```

---

## Dashboard Scenarios

The dashboard has a feeder selector (FEEDER-12 / FEEDER-13 / FEEDER-14) and three scenario rows:

### ⛔ Critical — instant single-event detections

| Button | Scenario | CEP Rule Fired |
|--------|----------|----------------|
| Busbar Fault | 87B relay trip | BUSBAR_FAULT CRITICAL |
| Transformer Fault | 87T relay trip | TRANSFORMER_FAULT CRITICAL |
| Line Lockout | 3 failed reclosures over 7s | BREAKER_LOCKOUT CRITICAL |

### ⚠ High — single-event HIGH severity

| Button | Scenario | CEP Rule Fired |
|--------|----------|----------------|
| Cable Fault | Zone-2 distance relay | DISTANCE_FAULT HIGH |
| Lightning Strike | 48.5 kA surge event | LIGHTNING_IMPACT HIGH |

### ⏱ Temporal — window-based and sustained detection

| Button | Scenario | CEP Pattern | Severity |
|--------|----------|-------------|----------|
| TX Overload (Pattern E) | Load ramp → 14 temp alarms over 70s | Pattern A (sustained 60s) | CRITICAL |
| Voltage Quality (Pattern B) | 3 harmonic events in 3s | Pattern B (window count) | MEDIUM |
| Freq Deviation (Pattern B) | 2 frequency dips in 60s | Pattern B (window count) | MEDIUM |
| Severe Grid Disturbance | Freq < 49.5 Hz AND voltage < 0.95 pu, 2 readings in 2s | AND + window | CRITICAL |
| Reclose Success | Relay trip → successful auto-reclose | Single event | LOW |

---

###  — Trigger a scenario

Select a feeder, click a scenario button. Within seconds you will see:

- **Fault Events tab** — aggregated fault event card with root cause, severity, contributing factors
- **All Incidents tab** — individual CEP incidents as they fire
- **LLM Intelligence tab** — full temporal report (CRITICAL/HIGH only, ~30–120s depending on model)

---

### Memory sizing by model

| Model | `requests.memory` | `limits.memory` |
|-------|-------------------|-----------------|
| phi3 | 4Gi | 6Gi |
| gemma2:2b | 2Gi | 3Gi |
| llama3.2:1b | 1.5Gi | 2.5Gi |
| tinyllama | 512Mi | 1Gi |

---



## Environment Variables

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `MQTT_HOST` | all | `mqtt-broker` | MQTT broker hostname |
| `MQTT_PORT` | all | `1883` | MQTT broker port |
| `SITE_ID` | cep-engine, simulator | `SUBSTATION-ABU-01` | Substation identifier |
| `RUST_LOG` | cep-engine | `info` | Log level (`debug` / `info` / `warn`) |
| `OLLAMA_HOST` | llm-service | `http://ollama:11434` | Ollama server URL |
| `OLLAMA_MODEL` | llm-service | `llama3.2:1b` | Model name |
| `CHROMA_PATH` | llm-service | `/app/incident_store` | ChromaDB persistence path |
| `SUMMARY_INTERVAL_S` | llm-service | `10` | Narrative summary frequency (seconds) |
| `RAG_TOP_K` | llm-service | `3` | Similar incidents to retrieve for RAG |

---

## License

MIT
