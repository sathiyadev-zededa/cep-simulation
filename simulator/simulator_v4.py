#!/usr/bin/env python3
"""
simulator_v4.py — Substation Scenario Simulator
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Drop-in replacement for simulator_v3.py.

Key change: scenario commands are now JSON with a specific feeder target.
Each scenario fires a deterministic, scripted event sequence (not random tick-based).
This makes demos predictable — you know exactly what CEP will detect.

Command format (sent by dashboard via edge/commands):
    Old (still supported): "permanent_fault 90"
    New:  {"scenario": "busbar_fault",       "feeder": "FEEDER-12"}
          {"scenario": "line_fault_lockout",  "feeder": "FEEDER-13"}
          {"scenario": "transformer_stress",  "feeder": "FEEDER-12"}
          {"scenario": "cable_fault",         "feeder": "FEEDER-12"}
          {"scenario": "voltage_quality",     "feeder": "FEEDER-13"}
          {"scenario": "frequency_deviation", "feeder": "FEEDER-12"}
          {"scenario": "lightning",           "feeder": "FEEDER-14"}
          {"scenario": "reclose_success",     "feeder": "FEEDER-12"}

Timings are compressed for demo (real minutes → seconds).
Background telemetry continues on all feeders during scenarios.
"""

import json, os, random, threading, time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import paho.mqtt.client as mqtt

# ── Config ────────────────────────────────────────────────────────────────────
MQTT_HOST      = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT      = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_EVENTS   = "edge/events"
TOPIC_COMMANDS = "edge/commands"
SITE_ID        = os.getenv("SITE_ID", "SUBSTATION-ABU-01")

FEEDERS = [
    {"feeder_id": "FEEDER-12", "transformer_id": "TX-12", "site_id": SITE_ID},
    {"feeder_id": "FEEDER-13", "transformer_id": "TX-12", "site_id": SITE_ID},
    {"feeder_id": "FEEDER-14", "transformer_id": "TX-13", "site_id": SITE_ID},
]
FEEDER_MAP = {f["feeder_id"]: f for f in FEEDERS}

# ── MQTT ──────────────────────────────────────────────────────────────────────
client = mqtt.Client(client_id="simulator-v4")

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def now_ms():
    return int(time.time() * 1000)

def pub(event_type: str, payload: Dict, feeder_id: str = "FEEDER-12"):
    asset = FEEDER_MAP.get(feeder_id, FEEDERS[0])
    event = {
        "id":         f"{now_ms()}-{random.randint(1000,9999)}",
        "event_type": event_type,
        "ts_ms":      now_ms(),
        "timestamp":  now_iso(),
        "site_id":    SITE_ID,
        "feeder_id":  feeder_id,
        "transformer_id": asset.get("transformer_id", "TX-12"),
        "payload":    payload,
    }
    client.publish(TOPIC_EVENTS, json.dumps(event), qos=0)
    print(f"[SIM] → {event_type:<22} feeder={feeder_id}  {list(payload.items())[:3]}")

def seq(steps: list, feeder_id: str):
    """Run a list of (delay_secs, event_type, payload_dict) in a background thread."""
    def _run():
        for delay, event_type, payload in steps:
            if delay > 0:
                time.sleep(delay)
            pub(event_type, payload, feeder_id)
    threading.Thread(target=_run, daemon=True).start()

# ── Background telemetry ───────────────────────────────────────────────────────
# Continuous low-noise telemetry on all feeders so the dashboard is always live.
# Scenarios add fault events ON TOP of this baseline.

_telemetry_state: Dict[str, Dict] = {
    f["feeder_id"]: {"v_kv": 11.0, "i_a": 120.0, "tx_load_pct": 70.0,
                     "tx_temp_c": 55.0, "freq_hz": 50.0, "power_factor": 0.92,
                     "harmonics_thd_pct": 3.0}
    for f in FEEDERS
}

def _telemetry_loop():
    while True:
        for feeder_id, s in _telemetry_state.items():
            s["v_kv"]            = max(10.6, min(11.4, s["v_kv"]            + random.gauss(0, 0.02)))
            s["i_a"]             = max(70,   min(200,  s["i_a"]             + random.gauss(0, 2)))
            s["tx_load_pct"]     = max(45,   min(85,   s["tx_load_pct"]     + random.gauss(0, 0.8)))
            s["tx_temp_c"]       = max(40,   min(70,   s["tx_temp_c"]       + (s["tx_load_pct"] - 70) * 0.005 + random.gauss(0, 0.1)))
            s["freq_hz"]         = max(49.95,min(50.05,s["freq_hz"]         + random.gauss(0, 0.005)))
            s["power_factor"]    = max(0.88, min(0.97, s["power_factor"]    + random.gauss(0, 0.002)))
            s["harmonics_thd_pct"] = max(2.0,min(5.0, s["harmonics_thd_pct"]+ random.gauss(0, 0.05)))

            pub("telemetry", {
                "v_kv":               round(s["v_kv"], 2),
                "i_a":                round(s["i_a"], 1),
                "breaker_closed":     True,
                "reclose_attempts":   0,
                "tx_load_pct":        round(s["tx_load_pct"], 1),
                "tx_temp_c":          round(s["tx_temp_c"], 1),
                "frequency_hz":       round(s["freq_hz"], 3),
                "power_factor":       round(s["power_factor"], 3),
                "harmonics_thd_pct":  round(s["harmonics_thd_pct"], 2),
                "mode":               "normal",
            }, feeder_id)
        time.sleep(2)

# ── Scripted scenarios ────────────────────────────────────────────────────────
# Each scenario sends a deterministic event sequence.
# Delays are compressed: real minutes → seconds for demo.

def run_busbar_fault(feeder_id: str):
    """
    Story:  Pre-fault voltage sag then busbar differential relay 87B operates.
    CEP:    PRE_FAULT_WARNING → BUSBAR_FAULT CRITICAL (instant)
    """
    seq([
        (0,   "telemetry",   {"v_kv": 9.1, "i_a": 380, "v_sag_pct": 18.5,
                               "i_spike_pct": 12.0, "tx_load_pct": 88.0,
                               "tx_temp_c": 62.0, "breaker_closed": True,
                               "mode": "pre_fault"}),
        (0.8, "relay_trip",  {"code": "87B", "equipment_id": "BUS-A1",
                               "location": "busbar_section_A",
                               "current_A": 4200, "voltage_kV": 0.0}),
    ], feeder_id)

def run_transformer_fault(feeder_id: str):
    """
    Story:  Transformer differential relay 87T operates — internal winding fault.
    CEP:    TRANSFORMER_FAULT CRITICAL (instant)
    """
    seq([
        (0, "relay_trip", {"code": "87T", "equipment_id": "TX-01",
                            "winding": "primary", "differential_A": 320,
                            "location": "transformer_HV_winding"}),
    ], feeder_id)

def run_line_fault_lockout(feeder_id: str):
    """
    Story:  Line trips 3× (transient each time), breaker locks out on 3rd.
    CEP:    REPEATED_TRANSIENT_FAULT (Pattern B, on 3rd trip)
            → PERMANENT_FAULT CRITICAL (Pattern B, on 3rd reclose failure)
    Compressed: real ~40 min → 7s
    """
    cb = f"CB-{feeder_id}-01"
    seq([
        # Trip 1 — transient, reclose succeeds
        (0,   "relay_trip",    {"code": "51", "current_A": 2200, "trip_no": 1, "phase": "A"}),
        (1.0, "breaker_close", {"reason": "reclose_success", "attempt": 1, "equipment_id": cb}),
        # Trip 2 — transient, reclose succeeds
        (1.0, "relay_trip",    {"code": "51", "current_A": 2350, "trip_no": 2, "phase": "B"}),
        (1.0, "breaker_close", {"reason": "reclose_success", "attempt": 1, "equipment_id": cb}),
        # Trip 3 — permanent, 3 reclose attempts all fail → lockout
        (1.0, "relay_trip",    {"code": "51", "current_A": 2500, "trip_no": 3, "phase": "A"}),
        (0.5, "breaker_open",  {"reason": "reclose_failed", "attempt": 1, "equipment_id": cb}),
        (0.5, "breaker_open",  {"reason": "reclose_failed", "attempt": 2, "equipment_id": cb}),
        (0.5, "breaker_open",  {"reason": "reclose_failed", "attempt": 3, "equipment_id": cb}),
    ], feeder_id)

def run_cable_fault(feeder_id: str):
    """
    Story:  Distance relay detects impedance drop — cable insulation fault.
    CEP:    CABLE_FAULT HIGH (instant)
    """
    seq([
        (0, "relay_trip", {"code": "21", "equipment_id": f"CABLE-{feeder_id}-SEC3",
                            "zone": 2, "impedance_ohm": 0.38,
                            "fault_distance_km": 4.2, "phase": "C"}),
    ], feeder_id)

def run_transformer_stress(feeder_id: str):
    """
    Story:  Load rises → temperature climbs → alarm fires and KEEPS firing for 60s.
    CEP:    TRANSFORMER_OVERLOAD HIGH fires immediately (load > 105%)
            TRANSFORMER_OVERHEATING_SUSTAINED HIGH fires after 60s of continuous
            temperature_warning events (Pattern A — sustained condition).
    Demo:   Operator triggers this and explains: "watch the temperature keep rising —
            after 60 seconds of sustained alarms, CEP confirms chronic overheating."
    """
    tx = FEEDER_MAP.get(feeder_id, FEEDERS[0]).get("transformer_id", "TX-01")

    # Phase 1 — load ramps up, temperature starts climbing (first 5 seconds)
    seq([
        (0,   "telemetry", {"tx_load_pct": 98.0,  "tx_temp_c": 84.0,
                             "v_kv": 11.0, "i_a": 490, "equipment_id": tx,
                             "mode": "overload"}),
        (2.0, "telemetry", {"tx_load_pct": 109.0, "tx_temp_c": 91.0,
                             "v_kv": 10.9, "i_a": 545, "equipment_id": tx,
                             "mode": "overload"}),
        (3.0, "telemetry", {"tx_load_pct": 118.0, "tx_temp_c": 97.0,
                             "v_kv": 10.7, "i_a": 590, "equipment_id": tx,
                             "mode": "overload_critical"}),
    ], feeder_id)

    # Phase 2 — temperature alarm fires repeatedly for 70s (every 5s).
    # Pattern A (sustained) requires 60s of continuous temperature_warning events.
    # Temperature gradually rises: 91 → 95 → 99 → 103 → 107°C over the period.
    def _sustained_alarms():
        temps = [91, 92, 94, 96, 97, 99, 101, 103, 104, 106, 107, 108, 109, 110]
        for i, temp_c in enumerate(temps):
            time.sleep(5)
            pub("temperature_warning", {
                "equipment_id":      tx,
                "temperature_c":     float(temp_c),
                "threshold_c":       90.0,
                "rate_degC_per_min": round(0.8 + i * 0.1, 1),
            }, feeder_id)
        # Final telemetry at peak load
        pub("telemetry", {"tx_load_pct": 122.0, "tx_temp_c": 110.0,
                          "v_kv": 10.6, "i_a": 610, "equipment_id": tx,
                          "mode": "overload_critical"}, feeder_id)

    threading.Thread(target=_sustained_alarms, daemon=True).start()

def run_voltage_quality(feeder_id: str):
    """
    Story:  3 harmonic events in quick succession — CEP fires on 3rd.
    CEP:    silent → silent → VOLTAGE_QUALITY_ISSUE MEDIUM (Pattern B)
    """
    seq([
        (0,   "voltage_harmonics", {"thd_pct": 7.8,  "harmonic_order": 5, "voltage_kV": 10.9}),
        (1.0, "voltage_harmonics", {"thd_pct": 8.4,  "harmonic_order": 5, "voltage_kV": 10.8}),
        (1.0, "voltage_harmonics", {"thd_pct": 9.1,  "harmonic_order": 5, "voltage_kV": 10.7}),
    ], feeder_id)

def run_frequency_deviation(feeder_id: str):
    """
    Story:  Grid frequency dips twice — generation shortfall.
    CEP:    silent → FREQUENCY_DEVIATION MEDIUM (Pattern B)
    """
    seq([
        (0,   "frequency_deviation", {"frequency_hz": 49.55, "nominal_hz": 50.0, "deviation_pct": 0.90}),
        (1.5, "frequency_deviation", {"frequency_hz": 49.38, "nominal_hz": 50.0, "deviation_pct": 1.24}),
    ], feeder_id)

def run_lightning(feeder_id: str):
    """
    Story:  Lightning strike near substation — surge risk.
    CEP:    LIGHTNING_IMPACT HIGH (instant)
    """
    seq([
        (0, "weather_lightning", {"strike_kA": 48.5, "location": "tower_12",
                                   "distance_m": 150, "surge_risk": True}),
    ], feeder_id)

def run_severe_grid_disturbance(feeder_id: str):
    """
    Story:  Simultaneous under-frequency and under-voltage — grid instability.
            Frequency drops to 49.2 Hz (generation shortfall) while voltage
            sags to 10.3 kV (0.94 pu) from reactive power deficit.
            Two consecutive telemetry readings 0.8 s apart both show both
            conditions — CEP 2-second window triggers SEVERE_GRID_DISTURBANCE.
    CEP:    SEVERE_GRID_DISTURBANCE CRITICAL (AND condition + 2s window)
    """
    seq([
        (0,   "telemetry", {
            "v_kv": 10.28, "i_a": 185.0, "breaker_closed": True,
            "reclose_attempts": 0, "tx_load_pct": 88.0, "tx_temp_c": 72.0,
            "frequency_hz": 49.21, "power_factor": 0.87,
            "harmonics_thd_pct": 4.2, "mode": "disturbance",
        }),
        (0.8, "telemetry", {
            "v_kv": 10.31, "i_a": 182.0, "breaker_closed": True,
            "reclose_attempts": 0, "tx_load_pct": 87.5, "tx_temp_c": 72.3,
            "frequency_hz": 49.18, "power_factor": 0.86,
            "harmonics_thd_pct": 4.5, "mode": "disturbance",
        }),
    ], feeder_id)

def run_reclose_success(feeder_id: str):
    """
    Story:  Transient fault clears on first reclose — system recovers.
    CEP:    RECLOSE_SUCCESS LOW (dashboard only, not sent to LLM)
    """
    cb = f"CB-{feeder_id}-01"
    seq([
        (0,   "relay_trip",    {"code": "51", "current_A": 1900, "trip_no": 1}),
        (1.0, "breaker_close", {"reason": "reclose_success", "attempt": 1,
                                 "equipment_id": cb}),
    ], feeder_id)

# ── Scenario dispatch table ───────────────────────────────────────────────────
SCENARIO_HANDLERS = {
    "busbar_fault":        run_busbar_fault,
    "transformer_fault":   run_transformer_fault,
    "line_fault_lockout":  run_line_fault_lockout,
    "cable_fault":         run_cable_fault,
    "transformer_stress":  run_transformer_stress,
    "voltage_quality":     run_voltage_quality,
    "frequency_deviation":     run_frequency_deviation,
    "lightning":               run_lightning,
    "reclose_success":         run_reclose_success,
    "severe_grid_disturbance": run_severe_grid_disturbance,
}

# ── MQTT callbacks ────────────────────────────────────────────────────────────
def on_connect(c, userdata, flags, rc):
    c.subscribe(TOPIC_COMMANDS)
    print(f"[SIM] Connected — subscribed to {TOPIC_COMMANDS}")

def on_message(c, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="ignore").strip()
    if not raw:
        return

    # Try JSON first (new format from updated dashboard)
    try:
        obj = json.loads(raw)
        if "scenario" in obj:
            scenario = obj["scenario"]
            feeder   = obj.get("feeder", "FEEDER-12")
            handler  = SCENARIO_HANDLERS.get(scenario)
            if handler:
                print(f"[SIM] Scenario: {scenario} → feeder={feeder}")
                handler(feeder)
            else:
                print(f"[SIM] Unknown scenario: {scenario}")
            return
    except json.JSONDecodeError:
        pass

    # Fall back to old string format: "mode duration"
    parts    = raw.split()
    mode     = parts[0].strip()
    print(f"[SIM] Legacy command: {mode}")

client.on_connect = on_connect
client.on_message = on_message

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[SIM] Simulator v4 starting — MQTT {MQTT_HOST}:{MQTT_PORT}")
    print(f"[SIM] Scenarios: {', '.join(SCENARIO_HANDLERS.keys())}")

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    # Start background telemetry
    threading.Thread(target=_telemetry_loop, daemon=True).start()
    print("[SIM] Background telemetry running on all feeders")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SIM] Stopping")
        client.loop_stop()
