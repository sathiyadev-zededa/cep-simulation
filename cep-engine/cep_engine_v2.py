# cep_engine.py
#
# Complex Event Processing Engine
# Subscribes to edge/events, detects fault patterns, publishes incidents to edge/incidents.
#
# Supported event types (simulator_v2 compatible):
#   telemetry             — periodic metrics (v_kv, i_a, tx_load_pct, tx_temp_c, ...)
#   voltage_sag           — voltage drop detected
#   voltage_harmonics     — THD rising (pre-fault early warning)
#   partial_discharge     — insulation stress
#   frequency_deviation   — grid frequency deviation
#   relay_pickup          — relay picked up but not yet tripped
#   relay_trip            — protective relay operated
#   breaker_open          — circuit breaker opened
#   reclose_attempt       — automatic recloser attempted
#   breaker_close         — breaker successfully reclosed
#   ami_last_gasp_burst   — AMI meter outage burst (lockout indicator)
#   weather_lightning      — lightning event detected
#   temperature_warning   — transformer thermal alarm
#   zone_isolation        — cable section isolated (crew dispatch)
#   equipment_offline     — transformer / switchgear taken offline
#
# Relay code classification:
#   50 / 51 / 67   — overcurrent (phase-to-ground / phase fault)
#   21             — distance relay zone 1 (cable fault)
#   87T            — transformer differential (internal TX fault)
#   87B            — busbar differential (busbar fault)
#   63             — buchholz / oil pressure (TX internal fault)
#   50BF           — breaker failure (busbar / severe fault)
#
# Incident types published to edge/incidents:
#   TRANSIENT_FAULT          — breaker opened, reclosed successfully
#   PERMANENT_FAULT          — breaker locked out (generic)
#   TRANSFORMER_FAULT        — 87T / 63 relay, TX winding or oil fault
#   CABLE_FAULT              — relay 21 or zone_isolation event
#   BUSBAR_FAULT             — 87B / 50BF relay, all feeders affected
#   TRANSFORMER_OVERLOAD     — TX load/temperature threshold crossed
#   VOLTAGE_QUALITY_ISSUE    — sustained harmonics / voltage sag without trip
#   LIGHTNING_IMPACT         — lightning-induced voltage event
#   COMMS_LOSS               — telemetry gap > threshold
#   PRE_FAULT_WARNING        — early warning before fault (harmonics / partial discharge)

import json
import os
import random
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MQTT_HOST      = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT      = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_EVENTS   = "edge/events"
TOPIC_INCIDENTS = "edge/incidents"

# Thresholds
THD_WARNING_PCT        = 8.0    # % — harmonic distortion alert
THD_CRITICAL_PCT       = 15.0   # % — critical distortion
VOLTAGE_SAG_THRESHOLD  = 10.0   # kV — below this is a sag event
TX_TEMP_WARNING_C      = 90.0   # °C — transformer temperature warning
TX_TEMP_CRITICAL_C     = 110.0  # °C — transformer critical temperature
TX_LOAD_WARNING_PCT    = 110.0  # % — transformer overload warning
TX_LOAD_CRITICAL_PCT   = 130.0  # % — transformer critical overload
FREQ_DEVIATION_HZ      = 0.1    # Hz — frequency deviation threshold
COMMS_LOSS_TIMEOUT_S   = 15     # seconds without telemetry → comms loss incident

# Relay code → fault classification mapping
RELAY_OVERCURRENT  = {"50", "51", "67"}
RELAY_DISTANCE     = {"21"}
RELAY_TX_DIFF      = {"87T", "63"}
RELAY_BUSBAR       = {"87B", "50BF"}

# ─── UTILITIES ────────────────────────────────────────────────────────────────

def now_ms() -> int:
    return int(time.time() * 1000)


def _severity_to_confidence(severity: str) -> float:
    """Map severity string to a 0–1 confidence score for the dashboard."""
    return {"CRITICAL": 0.95, "HIGH": 0.85, "MEDIUM": 0.70, "LOW": 0.55}.get(severity, 0.60)


def make_incident(incident_type: str, severity: str, description: str,
                  source_event: Optional[Dict] = None,
                  extra: Optional[Dict] = None) -> Dict:
    """
    Incident envelope published to edge/incidents.

    Top-level fields the dashboard reads directly:
      event_type  → must be "incident" for dashboard filter (index.html line 498)
      ts_ms       → timestamp for the card clock

    Payload fields the dashboard renders (index.html renderIncidents):
      incident_type  → card heading
      severity       → CRITICAL / HIGH / MEDIUM / LOW
      confidence     → 0–1 mapped from severity (shown as confidence %)
      summary        → one-line text shown on card body
      feeder_id      → location info
      transformer_id → location info
      fault_scenario → scenario tag (BREAKER_LOCKOUT, CABLE_FAULT, etc.)
    """
    src          = source_event or {}
    src_payload  = src.get("payload") or {}

    # Resolve feeder_id — transformer-based events won't have it at top level
    feeder_id = (src.get("feeder_id") or
                 src_payload.get("feeder_id") or
                 (extra or {}).get("feeder_id") or
                 "—")

    transformer_id = (src.get("transformer_id") or
                      src_payload.get("transformer_id") or
                      (extra or {}).get("transformer_id") or
                      "—")

    # Resolve fault_scenario from payload (simulator puts it there)
    fault_scenario = (src.get("fault_scenario") or
                      src_payload.get("fault_scenario") or
                      (extra or {}).get("fault_scenario") or
                      incident_type)   # fallback to incident type, never "UNKNOWN"

    inc_id  = f"INC-{now_ms()}-{random.randint(1000, 9999)}"
    ts      = now_ms()

    payload_block = {
        "incident_type":  incident_type,
        "severity":       severity,
        "confidence":     _severity_to_confidence(severity),
        "summary":        description,          # dashboard shows this as card body
        "feeder_id":      feeder_id,
        "transformer_id": transformer_id,
        "site_id":        src.get("site_id", "—"),
        "fault_scenario": fault_scenario,
        "source_event": {
            "event_type": src.get("event_type"),
            "event_id":   src.get("id"),
            "ts_ms":      src.get("ts_ms"),
        } if source_event else None,
    }

    # Merge any extra fields the rule wants to add into the payload block
    if extra:
        payload_block.update(extra)

    return {
        "id":         inc_id,
        "event_type": "incident",    # ← required by dashboard filter
        "ts_ms":      ts,
        "payload":    payload_block, # ← dashboard reads everything from here
    }


# ─── WINDOW STATE ─────────────────────────────────────────────────────────────
#
# Rolling time-window buffers used for multi-event correlation rules.
# All deques store (timestamp_ms, event_dict) tuples.

WINDOW_60S  = 60_000   # ms
WINDOW_300S = 300_000  # ms

class WindowBuffer:
    """Thread-safe rolling event buffer keyed by event_type."""
    def __init__(self, window_ms: int = WINDOW_60S) -> None:
        self._window_ms = window_ms
        self._lock      = threading.Lock()
        self._buckets: Dict[str, deque] = {}

    def add(self, event_type: str, event: Dict) -> None:
        with self._lock:
            if event_type not in self._buckets:
                self._buckets[event_type] = deque()
            self._buckets[event_type].append((now_ms(), event))
            self._evict(event_type)

    def count(self, event_type: str) -> int:
        with self._lock:
            self._evict(event_type)
            return len(self._buckets.get(event_type, []))

    def recent(self, event_type: str) -> List[Dict]:
        with self._lock:
            self._evict(event_type)
            return [e for _, e in self._buckets.get(event_type, [])]

    def clear(self, event_type: str) -> None:
        with self._lock:
            self._buckets[event_type] = deque()

    def _evict(self, event_type: str) -> None:
        cutoff = now_ms() - self._window_ms
        bucket = self._buckets.get(event_type)
        if bucket:
            while bucket and bucket[0][0] < cutoff:
                bucket.popleft()


window = WindowBuffer(WINDOW_60S)


# ─── INCIDENT DEDUPLICATION ───────────────────────────────────────────────────

class IncidentDedup:
    """Suppress duplicate incidents of the same type within a cooldown period."""
    def __init__(self, cooldown_s: int = 30) -> None:
        self._cooldown_ms = cooldown_s * 1000
        self._lock        = threading.Lock()
        self._last: Dict[str, int] = {}   # incident_type → last_published_ms

    def should_publish(self, incident_type: str) -> bool:
        with self._lock:
            last = self._last.get(incident_type, 0)
            if now_ms() - last > self._cooldown_ms:
                self._last[incident_type] = now_ms()
                return True
            return False

    def reset(self, incident_type: str) -> None:
        with self._lock:
            self._last[incident_type] = 0


dedup = IncidentDedup(cooldown_s=20)


# ─── COMMS LOSS TRACKER ───────────────────────────────────────────────────────

class CommsLossTracker:
    def __init__(self, timeout_s: int = COMMS_LOSS_TIMEOUT_S) -> None:
        self._timeout_s = timeout_s
        self._last_telemetry_s = time.time()
        self._comms_lost = False

    def heartbeat(self) -> None:
        self._last_telemetry_s = time.time()
        self._comms_lost = False

    def is_lost(self) -> bool:
        return time.time() - self._last_telemetry_s > self._timeout_s

    def mark_lost(self) -> None:
        self._comms_lost = True

    def was_lost(self) -> bool:
        return self._comms_lost


comms_tracker = CommsLossTracker()


# ─── RULE ENGINE ─────────────────────────────────────────────────────────────
#
# Each rule is a function:  rule_xxx(event, payload, client) -> Optional[Dict]
# Returns an incident dict if a rule fires, None otherwise.
# Rules are evaluated in order for every incoming event.

def rule_relay_trip_overcurrent(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: relay_trip with code 50 / 51 / 67
    → Could be transient or permanent depending on fault_type field.
    """
    if event.get("event_type") != "relay_trip":
        return None
    code = str(payload.get("code", ""))
    if code not in RELAY_OVERCURRENT:
        return None

    fault_type = payload.get("fault_type", "unknown")
    if fault_type == "transient":
        incident_type = "TRANSIENT_FAULT"
        severity      = "MEDIUM"
        desc = (f"Relay {code} tripped on transient overcurrent — "
                f"feeder {event.get('feeder_id','?')}, "
                f"fault current {payload.get('i_fault_a','?')} A")
    else:
        incident_type = "PERMANENT_FAULT"
        severity      = "HIGH"
        desc = (f"Relay {code} tripped on permanent overcurrent — "
                f"feeder {event.get('feeder_id','?')}, "
                f"fault current {payload.get('i_fault_a','?')} A, manual restoration required")

    if not dedup.should_publish(f"{incident_type}_{event.get('feeder_id')}"):
        return None

    return make_incident(incident_type, severity, desc, event, {
        "relay_code":    code,
        "fault_type":    fault_type,
        "fault_current": payload.get("i_fault_a"),
    })


def rule_relay_trip_transformer(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: relay_trip with code 87T (transformer differential) or 63 (buchholz)
    → TRANSFORMER_FAULT — internal winding or oil fault
    """
    if event.get("event_type") != "relay_trip":
        return None
    code = str(payload.get("code", ""))
    if code not in RELAY_TX_DIFF:
        return None

    if not dedup.should_publish(f"TRANSFORMER_FAULT_{event.get('transformer_id')}"):
        return None

    code_desc = {
        "87T": "transformer differential relay operated — internal winding fault suspected",
        "63":  "buchholz relay operated — oil pressure surge, gas accumulation detected",
    }.get(code, f"relay {code} operated")

    return make_incident(
        "TRANSFORMER_FAULT", "CRITICAL",
        f"[TRANSFORMER_FAULT] {code_desc} — "
        f"TX {event.get('transformer_id','?')} taken offline, oil sample and inspection required",
        event,
        {"relay_code": code, "fault_type": "transformer_internal",
         "action_required": "oil_sample_winding_inspection"}
    )


def rule_relay_trip_distance(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: relay_trip with code 21 (distance zone 1)
    → CABLE_FAULT — underground cable insulation failure
    """
    if event.get("event_type") != "relay_trip":
        return None
    code = str(payload.get("code", ""))
    if code not in RELAY_DISTANCE:
        return None

    if not dedup.should_publish(f"CABLE_FAULT_{event.get('feeder_id')}"):
        return None

    return make_incident(
        "CABLE_FAULT", "HIGH",
        f"[CABLE_FAULT] Distance relay zone 1 operated — "
        f"feeder {event.get('feeder_id','?')} cable fault, "
        f"fault current {payload.get('i_fault_a','?')} A. "
        f"TDR test and excavation required.",
        event,
        {"relay_code": code, "fault_type": "cable_insulation",
         "action_required": "tdr_test_excavation"}
    )


def rule_relay_trip_busbar(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: relay_trip with code 87B (busbar differential) or 50BF (breaker failure)
    → BUSBAR_FAULT — all feeders on this bus affected
    """
    if event.get("event_type") != "relay_trip":
        return None
    code = str(payload.get("code", ""))
    if code not in RELAY_BUSBAR:
        return None

    if not dedup.should_publish(f"BUSBAR_FAULT_{event.get('site_id')}"):
        return None

    code_desc = {
        "87B":  "busbar differential relay operated — bus fault detected",
        "50BF": "breaker failure relay operated — failed breaker isolation in progress",
    }.get(code, f"relay {code} operated")

    return make_incident(
        "BUSBAR_FAULT", "CRITICAL",
        f"[BUSBAR_FAULT] {code_desc} — "
        f"site {event.get('site_id','?')}, all feeders de-energised, "
        f"fault current {payload.get('i_fault_a','?')} A. "
        f"Substation inspection mandatory before restoration.",
        event,
        {"relay_code": code, "fault_type": "busbar_fault",
         "all_feeders_affected": True,
         "action_required": "substation_inspection"}
    )


def rule_breaker_lockout(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: ami_last_gasp_burst with status=lockout
    → Confirms permanent fault lockout — customers lost power.
    """
    if event.get("event_type") != "ami_last_gasp_burst":
        return None
    if payload.get("status") != "lockout":
        return None

    count     = payload.get("count", 0)
    feeder_id = payload.get("feeder_id") or event.get("feeder_id", "?")
    scenario  = payload.get("fault_scenario", "PERMANENT_FAULT")

    if not dedup.should_publish(f"LOCKOUT_{feeder_id}"):
        return None

    return make_incident(
        "PERMANENT_FAULT", "CRITICAL",
        f"[{scenario}] Breaker locked out on feeder {feeder_id} — "
        f"{count} customer meters reporting power loss. "
        f"Manual crew dispatch required.",
        event,
        {"customers_affected": count, "feeder_id": feeder_id,
         "action_required": "crew_dispatch"}
    )


def rule_reclose_success(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: breaker_close with reason=reclose_success
    → Transient fault confirmed cleared — feeder restored.
    """
    if event.get("event_type") != "breaker_close":
        return None
    if payload.get("reason") != "reclose_success":
        return None

    feeder_id = event.get("feeder_id", "?")

    # Reset dedup for this feeder so next fault fires immediately
    dedup.reset(f"TRANSIENT_FAULT_{feeder_id}")

    if not dedup.should_publish(f"RECLOSE_SUCCESS_{feeder_id}"):
        return None

    return make_incident(
        "TRANSIENT_FAULT", "LOW",
        f"Transient fault cleared — feeder {feeder_id} restored after "
        f"{payload.get('attempt', '?')} reclose attempt(s). No action required.",
        event,
        {"reclose_attempts": payload.get("attempt"), "feeder_id": feeder_id,
         "resolved": True}
    )


def rule_transformer_overload(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: telemetry OR temperature_warning — TX temperature or load exceeded threshold.
    """
    if event.get("event_type") not in ("telemetry", "temperature_warning"):
        return None

    tx_temp   = payload.get("tx_temp_c", 0)
    tx_load   = payload.get("tx_load_pct", 0)
    tx_id     = event.get("transformer_id", "?")

    if tx_temp >= TX_TEMP_CRITICAL_C:
        severity = "CRITICAL"
        desc     = (f"Transformer {tx_id} temperature CRITICAL: {tx_temp:.1f}°C "
                    f"(limit: {TX_TEMP_CRITICAL_C}°C). Immediate load shedding required.")
        key      = f"TX_CRITICAL_{tx_id}"
    elif tx_temp >= TX_TEMP_WARNING_C:
        severity = "HIGH"
        desc     = (f"Transformer {tx_id} temperature WARNING: {tx_temp:.1f}°C "
                    f"(threshold: {TX_TEMP_WARNING_C}°C). Monitor cooling system.")
        key      = f"TX_WARN_{tx_id}"
    elif tx_load >= TX_LOAD_CRITICAL_PCT:
        severity = "CRITICAL"
        desc     = (f"Transformer {tx_id} overloaded: {tx_load:.1f}% of rated capacity. "
                    f"Risk of thermal damage. Shed load immediately.")
        key      = f"TX_OVERLOAD_CRITICAL_{tx_id}"
    elif tx_load >= TX_LOAD_WARNING_PCT:
        severity = "HIGH"
        desc     = (f"Transformer {tx_id} load warning: {tx_load:.1f}% of rated capacity. "
                    f"Monitor temperature closely.")
        key      = f"TX_OVERLOAD_WARN_{tx_id}"
    else:
        return None

    if not dedup.should_publish(key):
        return None

    return make_incident(
        "TRANSFORMER_OVERLOAD", severity, desc, event,
        {"tx_temp_c": tx_temp, "tx_load_pct": tx_load, "tx_id": tx_id}
    )


def rule_voltage_quality(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: voltage_harmonics — sustained THD above threshold.
    Only fires if seen 3+ times in the last 60 seconds (avoids single-spike noise).
    """
    if event.get("event_type") != "voltage_harmonics":
        return None

    thd = payload.get("thd_pct", 0)
    if thd < THD_WARNING_PCT:
        return None

    window.add("voltage_harmonics", event)
    if window.count("voltage_harmonics") < 3:
        return None   # wait for sustained pattern

    feeder_id = event.get("feeder_id", "?")
    severity  = "HIGH" if thd >= THD_CRITICAL_PCT else "MEDIUM"

    if not dedup.should_publish(f"VQ_{feeder_id}"):
        return None

    return make_incident(
        "VOLTAGE_QUALITY_ISSUE", severity,
        f"Sustained harmonic distortion on feeder {feeder_id}: "
        f"THD={thd:.1f}% (threshold={THD_WARNING_PCT}%). "
        f"Possible arc flash, capacitor bank resonance, or pre-fault insulation stress.",
        event,
        {"thd_pct": thd, "feeder_id": feeder_id,
         "fault_scenario": payload.get("fault_scenario")}
    )


def rule_pre_fault_warning(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: partial_discharge event — insulation stress early warning.
    High count_per_min OR high magnitude fires immediately.
    """
    if event.get("event_type") != "partial_discharge":
        return None

    magnitude = payload.get("magnitude_db", 0)
    count_pm  = payload.get("count_per_min", 0)
    feeder_id = event.get("feeder_id", "?")

    # Only fire on significant PD
    if magnitude < 20 and count_pm < 30:
        return None

    if not dedup.should_publish(f"PD_{feeder_id}"):
        return None

    severity = "HIGH" if magnitude >= 35 or count_pm >= 60 else "MEDIUM"

    return make_incident(
        "PRE_FAULT_WARNING", severity,
        f"Partial discharge detected on feeder {feeder_id}: "
        f"magnitude={magnitude} dB, rate={count_pm}/min. "
        f"Insulation degradation in progress — schedule inspection before outage occurs.",
        event,
        {"magnitude_db": magnitude, "count_per_min": count_pm,
         "phase": payload.get("phase"),
         "fault_scenario": payload.get("fault_scenario")}
    )


def rule_lightning_impact(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: weather_lightning + voltage_sag in same window
    → LIGHTNING_IMPACT incident when lightning is close and causes a sag.
    """
    if event.get("event_type") != "voltage_sag":
        return None
    if payload.get("cause") != "lightning_impulse":
        return None

    feeder_id = event.get("feeder_id", "?")

    if not dedup.should_publish(f"LIGHTNING_{feeder_id}"):
        return None

    return make_incident(
        "LIGHTNING_IMPACT", "MEDIUM",
        f"Lightning-induced voltage impulse on feeder {feeder_id}: "
        f"impulse={payload.get('impulse_kv','?')} kV, "
        f"voltage sagged to {payload.get('v_kv','?')} kV. "
        f"Surge arrestors activated. Check for equipment damage.",
        event,
        {"impulse_kv": payload.get("impulse_kv"),
         "v_kv":       payload.get("v_kv")}
    )


def rule_zone_isolation(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: zone_isolation event
    → CABLE_FAULT — section isolated, crew dispatch needed.
    """
    if event.get("event_type") != "zone_isolation":
        return None

    zone      = payload.get("zone", "?")
    feeder_id = event.get("feeder_id", "?")

    if not dedup.should_publish(f"ZONE_ISO_{zone}"):
        return None

    return make_incident(
        "CABLE_FAULT", "HIGH",
        f"[CABLE_FAULT] Cable zone {zone} isolated on feeder {feeder_id}. "
        f"Isolators opened: {payload.get('isolators_open', [])}. "
        f"Dispatch crew for TDR test and cable splice.",
        event,
        {"zone": zone, "isolators_open": payload.get("isolators_open"),
         "fault_scenario": payload.get("fault_scenario"),
         "action_required": "crew_dispatch_tdr_splice"}
    )


def rule_equipment_offline(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: equipment_offline event
    → TRANSFORMER_FAULT — equipment taken out of service.
    """
    if event.get("event_type") != "equipment_offline":
        return None

    equipment_id = payload.get("equipment_id", "?")

    if not dedup.should_publish(f"EQUIP_OFFLINE_{equipment_id}"):
        return None

    return make_incident(
        "TRANSFORMER_FAULT", "CRITICAL",
        f"[TRANSFORMER_FAULT] {equipment_id} taken offline — "
        f"reason: {payload.get('reason','?')}. "
        f"Oil sample required: {payload.get('oil_sample_due', False)}. "
        f"Winding inspection and DGA test needed before return to service.",
        event,
        {"equipment_id": equipment_id,
         "reason":       payload.get("reason"),
         "oil_sample_due": payload.get("oil_sample_due"),
         "fault_scenario": payload.get("fault_scenario"),
         "action_required": "oil_sample_dga_winding_inspection"}
    )


def rule_frequency_deviation(event: Dict, payload: Dict, client: mqtt.Client) -> Optional[Dict]:
    """
    Rule: frequency_deviation — grid frequency outside ±0.1 Hz band.
    Fires only if 2+ deviations seen in last 60 seconds.
    """
    if event.get("event_type") != "frequency_deviation":
        return None

    deviation = abs(payload.get("deviation_hz", 0))
    if deviation < FREQ_DEVIATION_HZ:
        return None

    window.add("frequency_deviation", event)
    if window.count("frequency_deviation") < 2:
        return None

    feeder_id = event.get("feeder_id", "?")
    if not dedup.should_publish(f"FREQ_{feeder_id}"):
        return None

    return make_incident(
        "VOLTAGE_QUALITY_ISSUE", "MEDIUM",
        f"Grid frequency deviation detected on {feeder_id}: "
        f"f={payload.get('frequency_hz','?')} Hz "
        f"(deviation={payload.get('deviation_hz','?')} Hz). "
        f"Possible generation loss or large fault on upstream network.",
        event,
        {"frequency_hz": payload.get("frequency_hz"),
         "deviation_hz": payload.get("deviation_hz")}
    )


# ─── RULE REGISTRY ────────────────────────────────────────────────────────────
#
# Evaluated in order for every incoming event.
# Add new rules here — no other changes needed.

RULES = [
    rule_relay_trip_busbar,          # highest severity first
    rule_relay_trip_transformer,
    rule_relay_trip_distance,
    rule_relay_trip_overcurrent,
    rule_breaker_lockout,
    rule_reclose_success,
    rule_equipment_offline,
    rule_zone_isolation,
    rule_transformer_overload,
    rule_pre_fault_warning,
    rule_voltage_quality,
    rule_lightning_impact,
    rule_frequency_deviation,
]


# ─── EVENT PROCESSOR ──────────────────────────────────────────────────────────

def process_event(raw: str, client: mqtt.Client) -> None:
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[CEP] Invalid JSON: {raw[:80]}")
        return

    event_type = event.get("event_type", "unknown")
    payload    = event.get("payload", {})

    # Telemetry heartbeat for comms loss detection
    if event_type == "telemetry":
        comms_tracker.heartbeat()

    # Run all rules
    for rule in RULES:
        try:
            incident = rule(event, payload, client)
            if incident:
                msg = json.dumps(incident, ensure_ascii=False, separators=(",", ":"))
                client.publish(TOPIC_INCIDENTS, msg, qos=1)
                print(f"[CEP] ▶ {incident['incident_type']} | {incident['severity']} | "
                      f"{incident['description'][:80]}")
                break  # one incident per event (first matching rule wins)
        except Exception as e:
            print(f"[CEP] Rule {rule.__name__} error: {e}")


# ─── COMMS LOSS WATCHER ───────────────────────────────────────────────────────

def comms_loss_watcher(client: mqtt.Client) -> None:
    """Background thread: publishes COMMS_LOSS incident if telemetry stops."""
    while True:
        time.sleep(5)
        if comms_tracker.is_lost() and not comms_tracker.was_lost():
            comms_tracker.mark_lost()
            if dedup.should_publish("COMMS_LOSS"):
                incident = make_incident(
                    "COMMS_LOSS", "HIGH",
                    f"Telemetry gap exceeds {COMMS_LOSS_TIMEOUT_S}s — "
                    f"possible edge device offline or MQTT broker disconnected.",
                )
                client.publish(TOPIC_INCIDENTS,
                               json.dumps(incident, ensure_ascii=False, separators=(",", ":")),
                               qos=1)
                print(f"[CEP] ▶ COMMS_LOSS | HIGH | Telemetry gap detected")


# ─── PAHO-MQTT VERSION COMPATIBILITY ─────────────────────────────────────────
#
# paho-mqtt v1.x : Client(client_id=...)           on_connect(c, u, f, rc)
# paho-mqtt v2.x : Client(CallbackAPIVersion.VERSION2, ...) on_connect(c, u, f, rc, props)
#
# Detect version once at import time and branch accordingly.

_PAHO_V2 = hasattr(mqtt, "CallbackAPIVersion")


# ─── MQTT CALLBACKS ───────────────────────────────────────────────────────────

if _PAHO_V2:
    def on_connect(client, userdata, flags, reason_code, properties=None):
        # reason_code is a ReasonCode object in paho v2 — use is_failure to check
        success = not reason_code.is_failure if hasattr(reason_code, "is_failure") else (reason_code == 0)
        if success:
            client.subscribe(TOPIC_EVENTS, qos=0)
            print(f"[CEP] Connected to MQTT (paho v2) — subscribed to {TOPIC_EVENTS}")
        else:
            print(f"[CEP] MQTT connection failed: reason_code={reason_code}")

    def on_disconnect(client, userdata, flags, reason_code, properties=None):
        print(f"[CEP] MQTT disconnected (paho v2): reason_code={reason_code} — reconnecting...")
else:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(TOPIC_EVENTS, qos=0)
            print(f"[CEP] Connected to MQTT (paho v1) — subscribed to {TOPIC_EVENTS}")
        else:
            print(f"[CEP] MQTT connection failed: rc={rc}")

    def on_disconnect(client, userdata, rc):
        print(f"[CEP] MQTT disconnected (paho v1): rc={rc} — reconnecting...")


def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="ignore").strip()
    if raw:
        process_event(raw, client)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    # Create client — compatible with paho-mqtt v1 and v2
    if _PAHO_V2:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="cep_engine")
    else:
        client = mqtt.Client(client_id="cep_engine")

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    print(f"[CEP] Starting — MQTT {MQTT_HOST}:{MQTT_PORT}")
    print(f"[CEP] Subscribes: {TOPIC_EVENTS}")
    print(f"[CEP] Publishes:  {TOPIC_INCIDENTS}")
    print(f"[CEP] Rules loaded: {len(RULES)}")

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    # Start comms-loss watcher in background
    watcher = threading.Thread(target=comms_loss_watcher, args=(client,), daemon=True)
    watcher.start()

    client.loop_forever()


if __name__ == "__main__":
    main()
