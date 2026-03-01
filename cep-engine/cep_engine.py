# cep_engine.py
#
# CEP Engine (hardcoded rules version)
# - Subscribes to raw events:     edge/events
# - Publishes complex incidents:  edge/incidents
#
# Rules detected:
# 1) TRANSIENT_FAULT:
#    breaker_open + breaker_close within 60s AND voltage_sag near open
#
# 2) PERSISTENT_FEEDER_FAULT:
#    breaker_open + >=2 reclose_attempt within 60s + voltage_sag AND no breaker_close after open
#
# 3) DOWNSTREAM_OUTAGE:
#    breaker_open + AMI last-gasp burst count > 20 within 60s
#
# 4) TRANSFORMER_OVERLOAD_RISK:
#    tx_load_pct >= 120% for sustained period OR temperature rising fast
#
# Confidence boost:
# - If weather_lightning within 2 km in last 5 minutes, increase confidence.
#
# Usage:
#   export MQTT_HOST=127.0.0.1
#   export MQTT_PORT=1883
#   python cep_engine.py

import os
import json
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

TOPIC_EVENTS = "edge/events"
TOPIC_INCIDENTS = "edge/incidents"

# Optional metadata (not required, but nice for tracing)
ASSETS = {
    "feeder_id": "FEEDER-12",
    "breaker_id": "BRK-12A",
    "recloser_id": "RCL-12A",
    "transformer_id": "TX-12",
    "site_id": "SUBSTATION-ABU-01",
}


def now_s() -> float:
    return time.time()


def now_ms() -> int:
    return int(time.time() * 1000)


def new_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": f"{now_ms()}-{int(now_s()*1000)%100000}",
        "event_type": event_type,
        "ts_ms": now_ms(),
        "site_id": ASSETS["site_id"],
        **ASSETS,
        "payload": payload,
    }


def prune_by_recv_ts(q: Deque[Dict[str, Any]], max_age_s: float) -> None:
    cutoff = now_s() - max_age_s
    while q and q[0].get("_recv_s", 0) < cutoff:
        q.popleft()


class CEPEngine:
    def __init__(self, publish_fn):
        self.publish_fn = publish_fn

        # Sliding windows for correlation (arrival-time windows for demo)
        self.breaker: Deque[Dict[str, Any]] = deque()       # breaker_open/close
        self.reclose: Deque[Dict[str, Any]] = deque()       # reclose_attempt
        self.sags: Deque[Dict[str, Any]] = deque()          # voltage_sag
        self.ami: Deque[Dict[str, Any]] = deque()           # ami_last_gasp_burst
        self.lightning: Deque[Dict[str, Any]] = deque()     # weather_lightning
        self.telemetry: Deque[Dict[str, Any]] = deque()     # telemetry history

        # last known state
        self.state: Dict[str, Any] = {
            "breaker_closed": None,
            "reclose_attempts": 0,
            "tx_load_pct": None,
            "tx_temp_c": None,
        }

        self.last_breaker_open_s: Optional[float] = None
        self.last_breaker_close_s: Optional[float] = None

        # incident cooldowns to avoid spamming
        self.last_fired: Dict[str, float] = {}

    def cooldown(self, key: str, min_gap_s: float) -> bool:
        t = now_s()
        last = self.last_fired.get(key, 0.0)
        if t - last < min_gap_s:
            return True
        self.last_fired[key] = t
        return False

    def lightning_boost(self) -> Tuple[float, Optional[Dict[str, Any]]]:
        # boost if lightning within 2km in last 5 minutes
        prune_by_recv_ts(self.lightning, 300.0)
        for e in reversed(self.lightning):
            dist = float(e.get("payload", {}).get("distance_km", 999))
            if dist <= 2.0:
                return 0.30, e.get("payload", {})
        return 0.0, None

    def on_event(self, evt: Dict[str, Any]) -> None:
        evt["_recv_s"] = now_s()
        et = evt.get("event_type", "")

        # Route into windows
        if et in ("breaker_open", "breaker_close"):
            self.breaker.append(evt)
            prune_by_recv_ts(self.breaker, 180.0)
            if et == "breaker_open":
                self.last_breaker_open_s = evt["_recv_s"]
            else:
                self.last_breaker_close_s = evt["_recv_s"]

        elif et == "reclose_attempt":
            self.reclose.append(evt)
            prune_by_recv_ts(self.reclose, 180.0)

        elif et == "voltage_sag":
            self.sags.append(evt)
            prune_by_recv_ts(self.sags, 180.0)

        elif et == "ami_last_gasp_burst":
            self.ami.append(evt)
            prune_by_recv_ts(self.ami, 600.0)

        elif et == "weather_lightning":
            self.lightning.append(evt)
            prune_by_recv_ts(self.lightning, 900.0)

        elif et == "telemetry":
            self.telemetry.append(evt)
            prune_by_recv_ts(self.telemetry, 900.0)  # 15 min
            p = evt.get("payload", {})
            # update last-known state snapshot
            if "breaker_closed" in p:
                self.state["breaker_closed"] = p["breaker_closed"]
            if "reclose_attempts" in p:
                self.state["reclose_attempts"] = p["reclose_attempts"]
            if "tx_load_pct" in p:
                self.state["tx_load_pct"] = p["tx_load_pct"]
            if "tx_temp_c" in p:
                self.state["tx_temp_c"] = p["tx_temp_c"]

        # Evaluate rules after every event
        self.evaluate()

    def evaluate(self) -> None:
        # prune
        prune_by_recv_ts(self.breaker, 180.0)
        prune_by_recv_ts(self.reclose, 180.0)
        prune_by_recv_ts(self.sags, 180.0)
        prune_by_recv_ts(self.ami, 600.0)
        prune_by_recv_ts(self.telemetry, 900.0)

        boost, lightning_payload = self.lightning_boost()

        # -----------------------
        # Rule 1: TRANSIENT_FAULT
        # -----------------------
        if self.last_breaker_open_s and self.last_breaker_close_s:
            if (
                self.last_breaker_close_s > self.last_breaker_open_s
                and (self.last_breaker_close_s - self.last_breaker_open_s) <= 60.0
            ):
                sag_near_open = any(abs(e["_recv_s"] - self.last_breaker_open_s) <= 10.0 for e in self.sags)
                if sag_near_open and not self.cooldown("TRANSIENT_FAULT", 20.0):
                    conf = min(0.70 + boost, 0.95)
                    self.emit_incident(
                        "TRANSIENT_FAULT",
                        conf,
                        "Breaker opened and reclosed successfully (transient disturbance).",
                        {"window_s": 60, "lightning": lightning_payload},
                    )

        # -----------------------------
        # Rule 2: PERSISTENT_FEEDER_FAULT
        # -----------------------------
        if self.last_breaker_open_s:
            attempts = [
                e for e in self.reclose
                if e["_recv_s"] >= self.last_breaker_open_s and (e["_recv_s"] - self.last_breaker_open_s) <= 60.0
            ]
            sag_recent = any(
                e["_recv_s"] >= self.last_breaker_open_s and (e["_recv_s"] - self.last_breaker_open_s) <= 60.0
                for e in self.sags
            )

            # breaker is still open if we haven't seen a close after the last open
            breaker_closed_after_open = (
                self.last_breaker_close_s is not None and self.last_breaker_close_s > self.last_breaker_open_s
            )

            if len(attempts) >= 2 and sag_recent and (not breaker_closed_after_open) and not self.cooldown("PERSISTENT_FEEDER_FAULT", 30.0):
                conf = min(0.80 + boost, 0.98)
                self.emit_incident(
                    "PERSISTENT_FEEDER_FAULT",
                    conf,
                    "Repeated reclose attempts failed with sustained voltage sag (likely permanent fault).",
                    {"reclose_attempts": len(attempts), "lightning": lightning_payload},
                )

        # -----------------------
        # Rule 3: DOWNSTREAM_OUTAGE
        # -----------------------
        if self.last_breaker_open_s:
            bursts = [
                b for b in self.ami
                if b["_recv_s"] >= self.last_breaker_open_s and (b["_recv_s"] - self.last_breaker_open_s) <= 60.0
            ]
            if bursts:
                max_count = max(int(b.get("payload", {}).get("count", 0)) for b in bursts)
                if max_count > 20 and not self.cooldown("DOWNSTREAM_OUTAGE", 30.0):
                    conf = min(0.75 + boost + (0.10 if max_count > 50 else 0.0), 0.98)
                    self.emit_incident(
                        "DOWNSTREAM_OUTAGE",
                        conf,
                        "AMI last-gasp burst correlated with breaker open (downstream outage likely).",
                        {"ami_last_gasp_max": max_count, "lightning": lightning_payload},
                    )

        # -----------------------------
        # Rule 4: TRANSFORMER_OVERLOAD_RISK
        # -----------------------------
        # Use last 10 minutes telemetry to check:
        # - overload persist (>=120% for majority of samples)
        # - fast temp rise (>=2°C/min)
        ten_min = [t for t in self.telemetry if (now_s() - t["_recv_s"]) <= 600.0]
        if len(ten_min) >= 30:  # at least ~30s evidence (with 1Hz telemetry)
            loads = [float(t.get("payload", {}).get("tx_load_pct", 0)) for t in ten_min]
            temps = [float(t.get("payload", {}).get("tx_temp_c", 0)) for t in ten_min]
            dt_min = max((ten_min[-1]["_recv_s"] - ten_min[0]["_recv_s"]) / 60.0, 0.01)
            temp_rate = (temps[-1] - temps[0]) / dt_min

            overload_persist = sum(1 for x in loads if x >= 120.0) >= max(5, int(len(loads) * 0.6))
            fast_rise = temp_rate >= 2.0

            if (overload_persist or fast_rise) and not self.cooldown("TRANSFORMER_OVERLOAD_RISK", 60.0):
                conf = 0.85 if overload_persist else 0.75
                if fast_rise:
                    conf += 0.10
                conf = min(conf, 0.95)
                self.emit_incident(
                    "TRANSFORMER_OVERLOAD_RISK",
                    conf,
                    "Transformer loading/temperature trend indicates overload risk.",
                    {"temp_rate_c_per_min": round(temp_rate, 2), "overload_persist": overload_persist},
                )

    def emit_incident(self, incident_type: str, confidence: float, summary: str, details: Dict[str, Any]) -> None:
        payload = {
            "incident_type": incident_type,
            "confidence": round(float(confidence), 2),
            "summary": summary,
            "details": details,
        }
        self.publish_fn(payload)


def main() -> None:
    pub = mqtt.Client(client_id="cep_publisher")
    pub.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    pub.loop_start()

    def publish_incident(incident_payload: Dict[str, Any]) -> None:
        evt = new_event("incident", incident_payload)
        pub.publish(TOPIC_INCIDENTS, json.dumps(evt, ensure_ascii=False, separators=(",", ":")), qos=0)

    engine = CEPEngine(publish_fn=publish_incident)

    sub = mqtt.Client(client_id="cep_engine")
    def on_connect(client, userdata, flags, rc):
        client.subscribe(TOPIC_EVENTS)

    def on_message(client, userdata, msg):
        try:
            evt = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        engine.on_event(evt)

    sub.on_connect = on_connect
    sub.on_message = on_message
    sub.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    print("CEP Engine running.")
    print(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")
    print(f"Subscribing: {TOPIC_EVENTS}")
    print(f"Publishing:  {TOPIC_INCIDENTS}")
    sub.loop_forever()


if __name__ == "__main__":
    main()
