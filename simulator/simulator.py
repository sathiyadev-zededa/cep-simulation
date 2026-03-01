# simulator.py
#
# Publishes simulated "utility" raw events to MQTT topic: edge/events
# Subscribes to MQTT topic: edge/commands to switch scenarios on-demand
#
# Scenarios:
#   normal
#   transient_fault
#   permanent_fault
#   transformer_overload
#   weather_lightning
#   comms_loss
#
# Usage:
#   export MQTT_HOST=127.0.0.1
#   export MQTT_PORT=1883
#   python simulator.py
#
# Trigger scenarios (from another terminal):
#   mosquitto_pub -t edge/commands -m "permanent_fault 90"

import os
import random
import time
import threading
from typing import Dict, Any, Optional

import paho.mqtt.client as mqtt

# ----------------------------
# MQTT + Topics
# ----------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

TOPIC_EVENTS = "edge/events"
TOPIC_COMMANDS = "edge/commands"

# Asset identifiers (demo metadata)
ASSETS = {
    "feeder_id": "FEEDER-12",
    "breaker_id": "BRK-12A",
    "recloser_id": "RCL-12A",
    "transformer_id": "TX-12",
    "site_id": "SUBSTATION-ABU-01",
}


def now_ms() -> int:
    return int(time.time() * 1000)


def new_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    # lightweight event schema
    return {
        "id": f"{now_ms()}-{random.randint(1000,9999)}",
        "event_type": event_type,
        "ts_ms": now_ms(),
        "site_id": ASSETS["site_id"],
        **ASSETS,
        "payload": payload,
    }


# ----------------------------
# Scenario State
# ----------------------------
class ScenarioState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.mode = "normal"
        self.mode_until = 0.0  # epoch seconds; if > now -> scenario active

        # comms loss special handling
        self.comms_silence_until = 0.0

    def set_mode(self, mode: str, duration_s: int = 60) -> None:
        with self._lock:
            self.mode = mode
            self.mode_until = time.time() + max(1, duration_s)
            if mode != "comms_loss":
                self.comms_silence_until = 0.0

    def get_mode(self) -> str:
        with self._lock:
            if self.mode != "normal" and time.time() > self.mode_until:
                self.mode = "normal"
            return self.mode

    def start_comms_loss(self, duration_s: int) -> None:
        with self._lock:
            self.mode = "comms_loss"
            self.mode_until = time.time() + max(1, duration_s)
            self.comms_silence_until = self.mode_until


state = ScenarioState()


# ----------------------------
# MQTT Callbacks
# ----------------------------
def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC_COMMANDS)


def on_message(client, userdata, msg):
    cmd = msg.payload.decode("utf-8", errors="ignore").strip()
    if not cmd:
        return

    # Commands:
    #   "permanent_fault 90"
    #   "normal 10"
    parts = cmd.split()
    mode = parts[0].strip()
    duration = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 60

    if mode == "comms_loss":
        state.start_comms_loss(duration)
    else:
        state.set_mode(mode, duration_s=duration)


# ----------------------------
# Publisher helper
# ----------------------------
def publish(client: mqtt.Client, event_type: str, payload: Dict[str, Any]) -> None:
    import json

    evt = new_event(event_type, payload)
    client.publish(TOPIC_EVENTS, json.dumps(evt, ensure_ascii=False, separators=(",", ":")), qos=0)


# ----------------------------
# Main Simulator Loop
# ----------------------------
def run_simulator() -> None:
    client = mqtt.Client(client_id="simulator")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    # Base "utility-ish" values (distribution feeder example)
    v_kv = 11.0
    i_a = 120.0
    breaker_closed = True
    reclose_attempts = 0
    tx_temp_c = 55.0
    tx_load_pct = 70.0

    print("Simulator started.")
    print(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")
    print(f"Publishes: {TOPIC_EVENTS}")
    print(f"Subscribes: {TOPIC_COMMANDS}")
    print("Commands examples:")
    print("  normal 10")
    print("  transient_fault 30")
    print("  permanent_fault 90")
    print("  transformer_overload 120")
    print("  weather_lightning 60")
    print("  comms_loss 30")

    while True:
        mode = state.get_mode()

        # COMM LOSS: publish nothing while active
        if mode == "comms_loss":
            # stay silent; when scenario ends, it will revert to normal
            time.sleep(0.2)
            continue

        # normal drift (noise)
        v_kv = max(10.5, min(11.5, v_kv + random.uniform(-0.03, 0.03)))
        i_a = max(60.0, min(250.0, i_a + random.uniform(-5, 5)))
        tx_load_pct = max(40.0, min(110.0, tx_load_pct + random.uniform(-2, 2)))

        # temperature responds to load (very rough)
        tx_temp_c = max(35.0, min(95.0, tx_temp_c + (tx_load_pct - 70.0) * 0.01 + random.uniform(-0.2, 0.2)))

        # Scenario: weather_lightning (publishes lightning events intermittently)
        if mode == "weather_lightning":
            if random.random() < 0.35:
                publish(
                    client,
                    "weather_lightning",
                    {"distance_km": round(random.uniform(0.2, 2.5), 2), "intensity": random.choice(["low", "medium", "high"])},
                )

        # Scenario: transient_fault (single trip + successful reclose)
        if mode == "transient_fault":
            # occasionally trigger a short sag + trip
            if breaker_closed and random.random() < 0.15:
                v_kv = random.uniform(8.5, 9.8)
                publish(client, "voltage_sag", {"v_kv": round(v_kv, 2), "duration_ms_hint": 800})

                breaker_closed = False
                publish(client, "relay_trip", {"code": random.choice(["51", "50", "67"]), "phase": random.choice(["A", "B", "C"])})
                publish(client, "breaker_open", {"reason": "trip", "source": "relay_trip"})

            # reclose after a moment and succeed
            if not breaker_closed and random.random() < 0.30:
                reclose_attempts += 1
                publish(client, "reclose_attempt", {"attempt": reclose_attempts})
                breaker_closed = True
                publish(client, "breaker_close", {"reason": "reclose_success", "attempt": reclose_attempts})
                v_kv = random.uniform(10.8, 11.2)
                reclose_attempts = 0

        # Scenario: permanent_fault (trip + repeated reclose failures + AMI burst)
        if mode == "permanent_fault":
            if breaker_closed and random.random() < 0.12:
                v_kv = random.uniform(7.5, 9.0)
                publish(client, "voltage_sag", {"v_kv": round(v_kv, 2), "duration_ms_hint": 5000})

                breaker_closed = False
                publish(client, "relay_trip", {"code": random.choice(["50", "51", "67"]), "phase": random.choice(["A", "B", "C"])})
                publish(client, "breaker_open", {"reason": "trip", "source": "relay_trip"})

                # AMI last-gasp burst (simulate 10–80 meters in a short window)
                n = random.randint(10, 80)
                publish(client, "ami_last_gasp_burst", {"count": n, "window_s": 20})

            # repeated reclose attempts that fail (breaker stays open)
            if not breaker_closed and random.random() < 0.30:
                reclose_attempts += 1
                publish(client, "reclose_attempt", {"attempt": reclose_attempts})
                publish(client, "breaker_open", {"reason": "reclose_failed", "attempt": reclose_attempts})

        # Scenario: transformer_overload (ramp load + temp)
        if mode == "transformer_overload":
            tx_load_pct = min(160.0, tx_load_pct + random.uniform(3, 7))
            tx_temp_c = min(130.0, tx_temp_c + random.uniform(0.6, 1.5))
            i_a = min(500.0, i_a + random.uniform(10, 20))

        # Always publish telemetry (1 Hz) unless comms_loss
        publish(
            client,
            "telemetry",
            {
                "v_kv": round(v_kv, 2),
                "i_a": round(i_a, 1),
                "breaker_closed": breaker_closed,
                "reclose_attempts": reclose_attempts,
                "tx_load_pct": round(tx_load_pct, 1),
                "tx_temp_c": round(tx_temp_c, 1),
                "mode": mode,
            },
        )

        time.sleep(1.0)


if __name__ == "__main__":
    run_simulator()
