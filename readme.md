## Cep Simulation

This repository contains an end‑to‑end edge Complex Event Processing (CEP) demo. It simulates IoT devices publishing raw signals, processes them through a CEP engine, and exposes incidents and controls via a dashboard UI, all connected through an MQTT broker.

### Repository structure

- **`mosquitto/`**: MQTT broker configuration (Eclipse Mosquitto).
- **`simulator/`**: Signal generator that publishes raw events into MQTT.
- **`cep-engine/`**: CEP engine that correlates events and emits incidents.
- **`dashboard-ui/`**: Web UI to visualize events/incidents and send commands.
- **`edge-cep-helm/`**: Helm chart to deploy the whole stack into Kubernetes.

---

## Architecture

At a high level, the system looks like this:

- The **Simulator** publishes raw signals on MQTT topics.
- The **MQTT Broker** routes all messages between components.
- The **CEP Engine** subscribes to raw signals, applies rules, and publishes incident events.
- The **Dashboard UI** visualizes both raw events and incidents, and can publish commands.
- A **Consumer** (external to this repo or conceptual) subscribes to incidents to take real‑world actions.

### Topics and responsibilities

| Component    | Publishes            | Subscribes                        | Responsibility           |
|-------------|----------------------|-----------------------------------|--------------------------|
| Simulator   | `edge/events`        | `edge/commands`                   | Generate raw signals     |
| MQTT Broker | —                    | —                                 | Route messages           |
| CEP Engine  | `edge/incidents`     | `edge/events`                     | Correlate & decide       |
| Dashboard UI| `edge/commands`      | `edge/events`, `edge/incidents`   | Visualize & control      |
| Consumer    | —                    | `edge/incidents`                  | Act on decisions         |

---

## Components

### Simulator (`simulator/`)

- Publishes synthetic telemetry to `edge/events`.
- Listens on `edge/commands` for control messages (e.g. change mode, raise/lower load).
- Configurable to adjust event rates and payloads.

### MQTT Broker (`mosquitto/`)

- Eclipse Mosquitto configuration for the broker.
- Acts as the central messaging hub between all components.
- Configuration is also mounted into the broker when deployed via the Helm chart.

### CEP Engine (`cep-engine/`)

- Subscribes to `edge/events`.
- Applies CEP rules (e.g. thresholds, windows, correlation) to detect anomalies or patterns.
- Publishes resulting incidents to `edge/incidents`.

### Dashboard UI (`dashboard-ui/`)

- Frontend to:
  - Visualize raw events (`edge/events`) and incidents (`edge/incidents`).
  - Send commands on `edge/commands` (e.g. start/stop, acknowledge, change thresholds).
- Typically exposed as an HTTP service (NodePort in the default Helm values).

---

## Kubernetes deployment (Helm)

The `edge-cep-helm` chart wires all components together in a Kubernetes cluster.

### Chart contents

- **Deployments**:
  - `deployment-mqtt.yaml`: MQTT broker
  - `deployment-simulator.yaml`: Simulator
  - `deployment-cep.yaml`: CEP engine
  - `deployment-dashboard.yaml`: Dashboard UI
- **Services**:
  - `service-mqtt.yaml`: Internal MQTT service
  - `service-dashboard.yaml`: Dashboard UI service (NodePort by default)
- **Config & secrets**:
  - `configmap-mosquitto.yaml`: Mosquitto configuration
  - `secret.yaml`: Docker registry credentials (if using private images)
  - `namespace.yaml`: Dedicated namespace for the stack


