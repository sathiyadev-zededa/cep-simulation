# simulator_v3.py
#
# Drop-in replacement for simulator.py
# Same 6 dashboard buttons work identically — richer events generated per scenario.
#
# Flow (unchanged):
#   Dashboard button → WebSocket → FastAPI → MQTT edge/commands
#       → simulator receives command → publishes richer events to edge/events
#           → CEP engine → edge/incidents → Dashboard
#
# What's improved vs v1:
#   1. Richer telemetry   — voltage, current, frequency, power_factor, harmonics_thd, tx_temp_c
#   2. Pre-fault warnings — harmonic distortion and partial_discharge events BEFORE the trip
#   3. Multi-device scope — 3 feeders + 2 transformers; faults affect correlated assets
#   4. Realistic physics  — voltage/current values follow proper electrical relationships
#   5. Fault escalation   — scenarios progress through stages (pre-fault → fault → recovery)
#   6. Richer payloads    — more context fields for future LLM report generation
#
# New event types (CEP engine ignores unknown types — safe to add):
#   voltage_harmonics    — THD rising before fault (pre-fault early warning)
#   partial_discharge    — insulation stress indicator
#   temperature_warning  — transformer thermal pre-alarm
#   frequency_deviation  — grid instability (multiple feeders faulted)
#   relay_pickup         — relay has picked up but not yet tripped (pre-trip warning)
#
# Original event types preserved (100% backward compatible):
#   telemetry, voltage_sag, relay_trip, breaker_open, reclose_attempt,
#   breaker_close, ami_last_gasp_burst, weather_lightning
#
# Usage:
#   pip install paho-mqtt     (no new dependencies)
#   export MQTT_HOST=127.0.0.1
#   export MQTT_PORT=1883
#   python simulator_v2.py

import json
import os
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt

# ─── CONFIG ──────────────────────────────────────────────────────────────────

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_EVENTS   = "edge/events"
TOPIC_COMMANDS = "edge/commands"

SITE_ID = "SUBSTATION-ABU-01"

# ─── ASSET DEFINITIONS ───────────────────────────────────────────────────────
#
#  Two transformers feed three feeders (mirrors a real distribution substation):
#
#   TX-12 (5 MVA) ──┬── FEEDER-12  (BRK-12A, RCL-12A)   ← primary feeder
#                   └── FEEDER-13  (BRK-13A, RCL-13A)   ← adjacent feeder
#   TX-13 (3 MVA) ───── FEEDER-14  (BRK-14A, RCL-14A)   ← separate feeder
#
FEEDERS = [
    {"feeder_id": "FEEDER-12", "breaker_id": "BRK-12A", "recloser_id": "RCL-12A",
     "transformer_id": "TX-12", "site_id": SITE_ID},
    {"feeder_id": "FEEDER-13", "breaker_id": "BRK-13A", "recloser_id": "RCL-13A",
     "transformer_id": "TX-12", "site_id": SITE_ID},
    {"feeder_id": "FEEDER-14", "breaker_id": "BRK-14A", "recloser_id": "RCL-14A",
     "transformer_id": "TX-13", "site_id": SITE_ID},
]

TRANSFORMERS = [
    {"transformer_id": "TX-12", "site_id": SITE_ID, "rating_kva": 5000,
     "feeders": ["FEEDER-12", "FEEDER-13"]},
    {"transformer_id": "TX-13", "site_id": SITE_ID, "rating_kva": 3000,
     "feeders": ["FEEDER-14"]},
]

# Primary asset (matches v1 — ensures CEP engine rules still trigger on FEEDER-12)
PRIMARY_FEEDER = FEEDERS[0]
PRIMARY_TX     = TRANSFORMERS[0]


# ─── UTILITIES ───────────────────────────────────────────────────────────────

def now_ms() -> int:
    return int(time.time() * 1000)


def make_event(event_type: str, payload: Dict[str, Any],
               asset: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Standard event envelope — same schema as v1."""
    a = asset or PRIMARY_FEEDER
    return {
        "id": f"{now_ms()}-{random.randint(1000, 9999)}",
        "event_type": event_type,
        "ts_ms": now_ms(),
        "site_id": a.get("site_id", SITE_ID),
        # include all asset fields except list fields (e.g. transformer feeders list)
        **{k: v for k, v in a.items() if not isinstance(v, list)},
        "payload": payload,
    }


def pub(client: mqtt.Client, event_type: str,
        payload: Dict[str, Any], asset: Optional[Dict[str, Any]] = None) -> None:
    evt = make_event(event_type, payload, asset)
    client.publish(TOPIC_EVENTS,
                   json.dumps(evt, ensure_ascii=False, separators=(",", ":")),
                   qos=0)


# ─── ELECTRICAL HELPERS ───────────────────────────────────────────────────────

def nominal_telemetry(v_kv=11.0, i_a=120.0, tx_load=70.0, tx_temp=55.0,
                      pf=0.92, thd=3.0, freq=50.0, mode="normal") -> Dict:
    """Base telemetry payload with all fields the dashboard + CEP engine expect."""
    return {
        # v1 fields (unchanged — dashboard KPI cards read these)
        "v_kv":             round(v_kv, 2),
        "i_a":              round(i_a, 1),
        "breaker_closed":   True,
        "reclose_attempts": 0,
        "tx_load_pct":      round(tx_load, 1),
        "tx_temp_c":        round(tx_temp, 1),
        "mode":             mode,
        # Extended fields (new — dashboard ignores unknown fields safely)
        "frequency_hz":     round(freq, 3),
        "power_factor":     round(pf, 3),
        "harmonics_thd_pct": round(thd, 2),
    }


# ─── SCENARIO STATE (same structure as v1 ScenarioState) ─────────────────────

class ScenarioState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.mode = "normal"
        self.mode_until = 0.0
        self.comms_silence_until = 0.0

    def set_mode(self, mode: str, duration_s: int = 60) -> None:
        with self._lock:
            self.mode = mode
            self.mode_until = time.time() + max(1, duration_s)
            if mode != "comms_loss":
                self.comms_silence_until = 0.0

    def start_comms_loss(self, duration_s: int) -> None:
        with self._lock:
            self.mode = "comms_loss"
            self.mode_until = time.time() + max(1, duration_s)
            self.comms_silence_until = self.mode_until

    def get_mode(self) -> str:
        with self._lock:
            if self.mode not in ("normal", "comms_loss") and time.time() > self.mode_until:
                self.mode = "normal"
            return self.mode

    def is_active(self) -> bool:
        return time.time() < self.mode_until


state = ScenarioState()


# ─── MQTT CALLBACKS ───────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC_COMMANDS)


def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="ignore").strip()
    if not raw:
        return
    parts = raw.split()
    mode = parts[0].strip()
    duration = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 60

    if mode == "comms_loss":
        state.start_comms_loss(duration)
    else:
        state.set_mode(mode, duration_s=duration)


# ─── PER-SCENARIO SIMULATION STATE ───────────────────────────────────────────
#
# These track where each scenario is within its progression so events escalate
# realistically rather than firing randomly from tick 0.

@dataclass
class SimVars:
    # Electrical
    v_kv:          float = 11.0
    i_a:           float = 120.0
    tx_load_pct:   float = 70.0
    tx_temp_c:     float = 55.0
    freq_hz:       float = 50.0
    power_factor:  float = 0.92
    harmonics_thd: float = 3.0

    # Breaker / recloser state
    breaker_closed:   bool = True
    reclose_attempts: int  = 0

    # Scenario progression counters
    tick:            int   = 0    # ticks since current scenario started
    pre_fault_done:  bool  = False
    fault_triggered: bool  = False
    lockout:         bool  = False   # permanent fault — breaker locked open

    def reset_fault_state(self) -> None:
        self.breaker_closed   = True
        self.reclose_attempts = 0
        self.pre_fault_done   = False
        self.fault_triggered  = False
        self.lockout          = False
        self.tick             = 0

    def reset_electrical(self) -> None:
        self.v_kv          = 11.0 + random.uniform(-0.2, 0.2)
        self.i_a           = 120.0 + random.uniform(-10, 10)
        self.tx_load_pct   = 70.0
        self.tx_temp_c     = 55.0
        self.freq_hz       = 50.0
        self.power_factor  = 0.92
        self.harmonics_thd = 3.0


sv = SimVars()
_last_mode = "normal"   # detect mode transitions to reset state


# ─── SCENARIO IMPLEMENTATIONS ─────────────────────────────────────────────────
#
# Each function handles one tick (1 second) of a scenario.
# Returns the telemetry dict to publish at the end of the tick.


def scenario_normal(client: mqtt.Client) -> Dict:
    """Steady-state: small noise drift around nominal values."""
    sv.v_kv          = max(10.6, min(11.4, sv.v_kv + random.gauss(0, 0.025)))
    sv.i_a           = max(70.0, min(200.0, sv.i_a + random.gauss(0, 3)))
    sv.tx_load_pct   = max(45.0, min(85.0, sv.tx_load_pct + random.gauss(0, 1.0)))
    sv.tx_temp_c     = max(40.0, min(70.0, sv.tx_temp_c + (sv.tx_load_pct - 70) * 0.008 + random.gauss(0, 0.15)))
    sv.harmonics_thd = max(2.0,  min(5.0,  sv.harmonics_thd + random.gauss(0, 0.08)))
    sv.power_factor  = max(0.88, min(0.97, sv.power_factor + random.gauss(0, 0.003)))
    sv.freq_hz       = max(49.95, min(50.05, sv.freq_hz + random.gauss(0, 0.01)))

    return nominal_telemetry(
        sv.v_kv, sv.i_a, sv.tx_load_pct, sv.tx_temp_c,
        sv.power_factor, sv.harmonics_thd, sv.freq_hz, "normal"
    )


def scenario_transient_fault(client: mqtt.Client) -> Dict:
    """
    Realistic transient fault progression:
      Ticks  0–4  : pre-fault (harmonics rise, partial discharge)
      Tick   5    : voltage sag + relay pickup
      Tick   6    : relay trip + breaker opens
      Ticks  7–10 : recloser works, breaker closes (success)
      Ticks 11+   : recovery back to normal
    """
    sv.tick += 1
    t = sv.tick

    # ── Stage 1: Pre-fault early warning (0–4s) ──────────────────────────
    if t <= 5 and not sv.pre_fault_done:
        sv.harmonics_thd = min(18.0, sv.harmonics_thd + random.uniform(1.5, 3.0))
        sv.v_kv = max(10.3, sv.v_kv - random.uniform(0.05, 0.15))
        sv.power_factor = max(0.82, sv.power_factor - random.uniform(0.01, 0.02))

        pub(client, "voltage_harmonics", {
            "thd_pct":       round(sv.harmonics_thd, 2),
            "threshold_pct": 8.0,
            "severity":      "warning" if sv.harmonics_thd < 12 else "critical",
            "description":   "Harmonic distortion rising — possible arc or insulation stress",
        })

        if sv.harmonics_thd > 10 and random.random() < 0.5:
            pub(client, "partial_discharge", {
                "magnitude_db": round(random.uniform(12, 28), 1),
                "phase":        random.choice(["A", "B", "C"]),
                "description":  "Partial discharge detected — insulation under stress",
            })

    # ── Stage 2: Relay pickup (t=5) ──────────────────────────────────────
    if t == 5:
        sv.v_kv = random.uniform(9.5, 10.2)
        pub(client, "relay_pickup", {
            "code":        random.choice(["51", "50"]),
            "phase":       random.choice(["A", "B", "C"]),
            "i_pickup_a":  round(sv.i_a * 1.4, 1),
            "description": "Overcurrent relay picked up — monitoring for trip threshold",
        })

    # ── Stage 3: Voltage sag + relay trip + breaker open (t=6) ──────────
    if t == 6 and not sv.fault_triggered:
        sv.fault_triggered = True
        sv.v_kv  = random.uniform(8.5, 9.5)
        sv.i_a   = random.uniform(380, 520)    # fault current spike
        sv.freq_hz = 49.92

        pub(client, "voltage_sag", {
            "v_kv":             round(sv.v_kv, 2),
            "duration_ms_hint": 800,
            "pre_fault_thd":    round(sv.harmonics_thd, 2),
            "fault_current_a":  round(sv.i_a, 1),
            "description":      "Voltage sag detected — temporary fault on feeder",
        })

        relay_code = random.choice(["51", "50", "67"])
        pub(client, "relay_trip", {
            "code":         relay_code,
            "phase":        random.choice(["A", "B", "C"]),
            "fault_type":   "transient",
            "i_fault_a":    round(sv.i_a, 1),
            "description":  f"Relay {relay_code} tripped on overcurrent",
        })

        sv.breaker_closed = False
        sv.i_a = 0.0
        pub(client, "breaker_open", {
            "reason":      "trip",
            "source":      "relay_trip",
            "fault_type":  "transient",
            "description": "Breaker opened — feeder de-energised, recloser armed",
        })

        sv.pre_fault_done = True

    # ── Stage 4: Reclose attempts (t=7–10) ───────────────────────────────
    if not sv.breaker_closed and t > 6 and t <= 11:
        if random.random() < 0.45:
            sv.reclose_attempts += 1
            pub(client, "reclose_attempt", {
                "attempt":     sv.reclose_attempts,
                "delay_s":     random.randint(1, 3),
                "description": f"Recloser attempt {sv.reclose_attempts} — testing if fault cleared",
            })

            if sv.reclose_attempts >= 1 and random.random() < 0.85:
                # Transient fault clears — reclose success
                sv.breaker_closed = True
                sv.v_kv = random.uniform(10.8, 11.1)
                sv.i_a  = random.uniform(100, 140)
                sv.harmonics_thd = 3.5
                sv.power_factor  = 0.91
                sv.freq_hz       = 50.0
                pub(client, "breaker_close", {
                    "reason":       "reclose_success",
                    "attempt":      sv.reclose_attempts,
                    "fault_type":   "transient",
                    "description":  "Reclose successful — fault cleared, feeder restored",
                })
                sv.reclose_attempts = 0

    # ── Stage 5: Recovery (t>10) ─────────────────────────────────────────
    if sv.breaker_closed and t > 10:
        sv.v_kv = min(11.0, sv.v_kv + 0.05)
        sv.harmonics_thd = max(3.0, sv.harmonics_thd - 0.3)
        sv.i_a  = min(130.0, sv.i_a + 5)

    return nominal_telemetry(
        sv.v_kv, sv.i_a, sv.tx_load_pct, sv.tx_temp_c,
        sv.power_factor, sv.harmonics_thd, sv.freq_hz, "transient_fault"
    ) | {"breaker_closed": sv.breaker_closed, "reclose_attempts": sv.reclose_attempts}



# ─── PERMANENT FAULT SCENARIO REGISTRY ───────────────────────────────────────
#
# Each time the dashboard button is clicked, one of these scenarios is chosen
# at random so the LLM generates a different incident report every time.
#
PERMANENT_FAULT_SCENARIOS = [
    {
        "id":          "BREAKER_LOCKOUT",
        "label":       "Phase-to-Ground Fault — Breaker Lockout",
        "fault_type":  "phase_to_ground",
        "relay_codes": ["50", "51"],
        "v_sag_kv":    (4.5, 7.5),
        "i_fault_a":   (600, 950),
        "asset":       "feeder",
        "description": "Hard phase-to-ground fault — 3 reclose attempts failed, breaker locked out",
    },
    {
        "id":          "TRANSFORMER_WINDING_FAILURE",
        "label":       "Transformer Internal Winding Failure",
        "fault_type":  "transformer_internal",
        "relay_codes": ["87T", "63"],      # differential + buchholz
        "v_sag_kv":    (6.0, 8.5),
        "i_fault_a":   (400, 700),
        "asset":       "transformer",
        "description": "TX-12 winding failure — differential relay operated, oil pressure surge, forced outage",
    },
    {
        "id":          "CABLE_INSULATION_FAILURE",
        "label":       "Underground Cable Insulation Breakdown",
        "fault_type":  "cable_insulation",
        "relay_codes": ["21", "67"],       # distance + directional overcurrent
        "v_sag_kv":    (5.0, 8.0),
        "i_fault_a":   (350, 650),
        "asset":       "feeder",
        "description": "Underground cable XLPE insulation failed — distance relay zone 1 operated, cable out of service",
    },
    {
        "id":          "SWITCHGEAR_BUSBAR_FAULT",
        "label":       "Switchgear Busbar Fault",
        "fault_type":  "busbar_fault",
        "relay_codes": ["87B", "50BF"],    # busbar diff + breaker failure
        "v_sag_kv":    (2.0, 5.0),
        "i_fault_a":   (800, 1400),
        "asset":       "transformer",
        "description": "Busbar fault at TX-12 LV panel — busbar differential relay operated, all feeders de-energised",
    },
]

# Module-level: track which scenario is active this invocation
_active_pf_scenario: Optional[Dict] = None


def scenario_permanent_fault(client: mqtt.Client) -> Dict:
    """
    Permanent fault — randomly picks one of 4 fault sub-types each time the
    dashboard button is pressed.  All sub-types follow the same 3-stage flow:

      Stage 1 (Ticks  1–6) : Pre-fault warnings  — harmonics, partial discharge, voltage drift
      Stage 2 (Tick   7)   : Main fault trigger   — deep voltage sag, relay trip, breaker opens
      Stage 3 (Ticks 8–30) : Reclose sequence     — 3 attempts, all fail → lockout + AMI burst

    Sub-types:
      BREAKER_LOCKOUT           — Phase-to-ground fault, feeder locked out (original)
      TRANSFORMER_WINDING_FAILURE — Internal TX fault, differential relay, oil pressure trip
      CABLE_INSULATION_FAILURE   — Underground XLPE cable failure, distance relay zone 1
      SWITCHGEAR_BUSBAR_FAULT    — Busbar fault, all feeders affected, breaker failure relay
    """
    global _active_pf_scenario

    sv.tick += 1
    t = sv.tick

    # Pick a random sub-type on first tick of this scenario
    if t == 1 or _active_pf_scenario is None:
        _active_pf_scenario = random.choice(PERMANENT_FAULT_SCENARIOS)
        print(f"[PERM FAULT] Scenario: {_active_pf_scenario['id']} — {_active_pf_scenario['label']}")

    sc = _active_pf_scenario
    is_tx_fault  = sc["asset"] == "transformer"
    fault_asset  = PRIMARY_TX if is_tx_fault else PRIMARY_FEEDER

    # ── Stage 1 : Pre-fault warnings (ticks 1–6) ─────────────────────────
    if t <= 6 and not sv.pre_fault_done:
        sv.harmonics_thd = min(30.0, sv.harmonics_thd + random.uniform(2.0, 4.5))
        sv.v_kv          = max(9.6, sv.v_kv - random.uniform(0.1, 0.3))
        sv.power_factor  = max(0.75, sv.power_factor - random.uniform(0.02, 0.05))
        sv.freq_hz       = max(49.85, sv.freq_hz - random.uniform(0.01, 0.04))

        pub(client, "voltage_harmonics", {
            "thd_pct":         round(sv.harmonics_thd, 2),
            "threshold_pct":   8.0,
            "severity":        "critical" if sv.harmonics_thd > 15 else "warning",
            "fault_scenario":  sc["id"],
            "description":     f"[{sc['id']}] Harmonic distortion escalating — {sc['description']}",
        })

        pub(client, "partial_discharge", {
            "magnitude_db":    round(random.uniform(28, 60), 1),
            "phase":           random.choice(["A", "B", "C"]),
            "count_per_min":   random.randint(50, 140),
            "fault_scenario":  sc["id"],
            "description":     f"[{sc['id']}] Severe partial discharge — insulation breakdown imminent",
        })

        # Transformer-specific pre-fault: oil temperature + pressure rising
        if is_tx_fault and t >= 3:
            pub(client, "temperature_warning", {
                "tx_temp_c":      round(sv.tx_temp_c + random.uniform(5, 18), 1),
                "oil_pressure_kpa": round(random.uniform(120, 220), 1),
                "severity":       "critical",
                "fault_scenario": sc["id"],
                "description":    f"[{sc['id']}] TX-12 oil temperature and pressure rising — internal fault suspected",
            }, PRIMARY_TX)

        # Frequency deviation on adjacent feeder
        if t >= 3:
            pub(client, "frequency_deviation", {
                "frequency_hz":   round(sv.freq_hz, 3),
                "deviation_hz":   round(sv.freq_hz - 50.0, 3),
                "fault_scenario": sc["id"],
                "source_feeder":  "FEEDER-12",
                "description":    f"[{sc['id']}] Grid frequency deviation on TX-12 bus",
            }, FEEDERS[1])

    # ── Stage 2 : Main fault trigger (tick 7) ────────────────────────────
    if t == 7 and not sv.fault_triggered:
        sv.fault_triggered = True
        v_lo, v_hi = sc["v_sag_kv"]
        i_lo, i_hi = sc["i_fault_a"]
        sv.v_kv    = random.uniform(v_lo, v_hi)
        sv.i_a     = random.uniform(i_lo, i_hi)
        sv.freq_hz = 49.78

        pub(client, "voltage_sag", {
            "v_kv":             round(sv.v_kv, 2),
            "duration_ms_hint": 20000,
            "fault_current_a":  round(sv.i_a, 1),
            "fault_type":       "permanent",
            "fault_scenario":   sc["id"],
            "pre_fault_thd":    round(sv.harmonics_thd, 2),
            "description":      f"[{sc['id']}] {sc['description']}",
        }, fault_asset)

        relay_code = random.choice(sc["relay_codes"])
        pub(client, "relay_trip", {
            "code":           relay_code,
            "phase":          random.choice(["A", "B", "C", "ABC"]) if sc["fault_type"] != "busbar_fault" else "ABC",
            "fault_type":     "permanent",
            "fault_scenario": sc["id"],
            "i_fault_a":      round(sv.i_a, 1),
            "description":    f"[{sc['id']}] Relay {relay_code} operated — {sc['label']}",
        }, fault_asset)

        sv.breaker_closed = False
        sv.i_a = 0.0

        # Busbar fault: ALL feeders go down, not just primary
        if sc["fault_type"] == "busbar_fault":
            for feeder in FEEDERS:
                pub(client, "breaker_open", {
                    "reason":         "busbar_trip",
                    "fault_type":     "permanent",
                    "fault_scenario": sc["id"],
                    "description":    f"[{sc['id']}] {feeder['breaker_id']} opened — busbar fault isolation",
                }, feeder)
        else:
            pub(client, "breaker_open", {
                "reason":         "trip",
                "source":         f"relay_{relay_code}",
                "fault_type":     "permanent",
                "fault_scenario": sc["id"],
                "description":    f"[{sc['id']}] Breaker opened — {sc['label']}",
            }, fault_asset)

        sv.pre_fault_done = True

    # ── Stage 3 : Reclose attempts — all fail (ticks 8–30) ───────────────
    if not sv.breaker_closed and not sv.lockout and sv.fault_triggered:
        max_attempts = 3

        # Transformer/busbar faults: only 1 reclose attempt (too risky to reclose onto)
        if sc["fault_type"] in ("transformer_internal", "busbar_fault"):
            max_attempts = 1

        if sv.reclose_attempts < max_attempts and t in [10, 16, 22]:
            sv.reclose_attempts += 1
            pub(client, "reclose_attempt", {
                "attempt":        sv.reclose_attempts,
                "max_attempts":   max_attempts,
                "delay_s":        random.choice([5, 10, 15]),
                "fault_scenario": sc["id"],
                "description":    f"[{sc['id']}] Reclose {sv.reclose_attempts}/{max_attempts} — fault still present",
            })
            pub(client, "breaker_open", {
                "reason":         "reclose_failed",
                "attempt":        sv.reclose_attempts,
                "fault_type":     "permanent",
                "fault_scenario": sc["id"],
                "description":    f"[{sc['id']}] Reclose failed — fault impedance unchanged",
            })

            # Brief partial voltage restoration then collapse
            sv.v_kv = random.uniform(6.5, 9.0)
            time.sleep(0.1)
            sv.v_kv = random.uniform(2.5, 6.0)

        # Lockout after all attempts exhausted
        if sv.reclose_attempts >= max_attempts and not sv.lockout:
            sv.lockout = True
            n_customers = random.randint(40, 200)

            pub(client, "ami_last_gasp_burst", {
                "count":          n_customers,
                "window_s":       20,
                "status":         "lockout",
                "feeder_id":      PRIMARY_FEEDER["feeder_id"],
                "fault_scenario": sc["id"],
                "description":    f"[{sc['id']}] {n_customers} customer meters reported power loss — manual restoration required",
            })

            # Adjacent feeder disturbance (always — shares transformer bus)
            pub(client, "voltage_sag", {
                "v_kv":           round(random.uniform(9.3, 10.1), 2),
                "duration_ms_hint": 4000,
                "cause":          f"adjacent_feeder_{sc['id']}",
                "fault_scenario": sc["id"],
                "description":    f"[{sc['id']}] FEEDER-13 affected by TX-12 bus disturbance",
            }, FEEDERS[1])

            # Cable fault: publish cable isolation event
            if sc["fault_type"] == "cable_insulation":
                pub(client, "zone_isolation", {
                    "zone":           "FEEDER-12-MID",
                    "isolators_open": ["SW-12-A", "SW-12-B"],
                    "fault_scenario": sc["id"],
                    "description":    f"[{sc['id']}] Cable section isolated for crew dispatch — excavation required",
                })

            # TX fault: publish transformer offline event
            if sc["fault_type"] == "transformer_internal":
                pub(client, "equipment_offline", {
                    "equipment_id":   "TX-12",
                    "reason":         "internal_fault",
                    "oil_sample_due": True,
                    "fault_scenario": sc["id"],
                    "description":    f"[{sc['id']}] TX-12 taken offline — oil sample and winding inspection required",
                }, PRIMARY_TX)

    return nominal_telemetry(
        sv.v_kv, sv.i_a, sv.tx_load_pct, sv.tx_temp_c,
        sv.power_factor, sv.harmonics_thd, sv.freq_hz, "permanent_fault"
    ) | {"breaker_closed": sv.breaker_closed, "reclose_attempts": sv.reclose_attempts,
         "fault_scenario": sc["id"] if _active_pf_scenario else "UNKNOWN"}


def scenario_transformer_overload(client: mqtt.Client) -> Dict:
    """
    Transformer overload — load ramps, temperature climbs, warnings escalate.
    Both TX-12 feeders (FEEDER-12, FEEDER-13) show increased load/current.
    """
    sv.tick += 1

    # Ramp load and temperature (same as v1 but smoother and richer)
    sv.tx_load_pct = min(165.0, sv.tx_load_pct + random.uniform(2.5, 6.0))
    sv.i_a         = min(550.0, sv.i_a + random.uniform(8, 18))
    sv.power_factor = max(0.78, sv.power_factor - random.uniform(0.005, 0.012))

    # Temperature lags load (thermal time constant ~5s in sim)
    target_temp = 30.0 + sv.tx_load_pct * 0.55
    sv.tx_temp_c = sv.tx_temp_c * 0.92 + target_temp * 0.08 + random.gauss(0, 0.3)
    sv.tx_temp_c = min(140.0, sv.tx_temp_c)

    # Harmonics rise with overload (nonlinear magnetic saturation)
    sv.harmonics_thd = min(20.0, sv.harmonics_thd + random.uniform(0.2, 0.8))

    # Temperature warning events (escalating severity)
    if sv.tx_temp_c > 90.0:
        severity = "critical" if sv.tx_temp_c > 110.0 else "warning"
        pub(client, "temperature_warning", {
            "tx_temp_c":    round(sv.tx_temp_c, 1),
            "threshold_c":  90.0,
            "tx_load_pct":  round(sv.tx_load_pct, 1),
            "severity":     severity,
            "description":  f"Transformer TX-12 temperature {severity} — {sv.tx_temp_c:.1f}°C (limit: 105°C)",
        }, PRIMARY_TX)

    # FEEDER-13 also sees load rise (shares TX-12)
    if sv.tick % 3 == 0:
        pub(client, "telemetry", {
            "v_kv":             round(sv.v_kv - random.uniform(0.1, 0.3), 2),
            "i_a":              round(sv.i_a * 0.6 + random.gauss(0, 5), 1),
            "breaker_closed":   True,
            "reclose_attempts": 0,
            "tx_load_pct":      round(sv.tx_load_pct, 1),
            "tx_temp_c":        round(sv.tx_temp_c, 1),
            "mode":             "transformer_overload",
            "harmonics_thd_pct": round(sv.harmonics_thd, 2),
        }, FEEDERS[1])   # FEEDER-13

    return nominal_telemetry(
        sv.v_kv, sv.i_a, sv.tx_load_pct, sv.tx_temp_c,
        sv.power_factor, sv.harmonics_thd, sv.freq_hz, "transformer_overload"
    )


def scenario_weather_lightning(client: mqtt.Client) -> Dict:
    """
    Lightning storm:
      - Frequent lightning events with distance/intensity
      - Voltage impulses on the feeder
      - If lightning is close (<0.5 km), triggers a brief voltage sag
    """
    sv.tick += 1

    # Normal drift continues
    sv.v_kv = max(10.4, min(11.6, sv.v_kv + random.gauss(0, 0.04)))
    sv.i_a  = max(80.0, min(200.0, sv.i_a + random.gauss(0, 4)))

    # Lightning strike probability (35% per second — same as v1 but richer)
    if random.random() < 0.35:
        distance = round(random.uniform(0.1, 4.0), 2)
        intensity = random.choices(
            ["low", "medium", "high"],
            weights=[40, 40, 20]
        )[0]
        peak_kA = {
            "low":    round(random.uniform(5, 20), 1),
            "medium": round(random.uniform(20, 60), 1),
            "high":   round(random.uniform(60, 150), 1),
        }[intensity]

        pub(client, "weather_lightning", {
            "distance_km": distance,
            "intensity":   intensity,
            "peak_kA":     peak_kA,
            "description": f"Lightning strike {distance} km away, {peak_kA} kA peak current",
        })

        # Close strikes cause voltage impulses / transient faults
        if distance < 0.8 and intensity in ("medium", "high"):
            impulse_kv = round(sv.v_kv + peak_kA * 0.12, 2)
            pub(client, "voltage_sag", {
                "v_kv":             round(random.uniform(8.5, 10.0), 2),
                "duration_ms_hint": 200,
                "cause":            "lightning_impulse",
                "impulse_kv":       impulse_kv,
                "description":      f"Voltage impulse from nearby lightning ({distance} km) — surge arrestors activated",
            })
            sv.harmonics_thd = min(15.0, sv.harmonics_thd + random.uniform(2, 6))

        # Very close high-intensity strike → relay pickup
        if distance < 0.3 and intensity == "high" and random.random() < 0.5:
            pub(client, "relay_pickup", {
                "code":        "50",
                "phase":       random.choice(["A", "B", "C"]),
                "cause":       "lightning_surge",
                "description": "Relay picked up due to lightning surge — monitoring",
            })

    # Harmonics decay slowly between strikes
    sv.harmonics_thd = max(3.0, sv.harmonics_thd - 0.2)

    return nominal_telemetry(
        sv.v_kv, sv.i_a, sv.tx_load_pct, sv.tx_temp_c,
        sv.power_factor, sv.harmonics_thd, sv.freq_hz, "weather_lightning"
    )


def scenario_comms_loss() -> None:
    """Comms loss: do nothing — main loop detects this and skips publishing."""
    pass


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────

def run_simulator() -> None:
    client = mqtt.Client(client_id="simulator_v2")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    global _last_mode

    print("Simulator v2 started.")
    print(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")
    print(f"Publishes: {TOPIC_EVENTS}")
    print(f"Subscribes: {TOPIC_COMMANDS}")
    print()
    print("Dashboard buttons (unchanged):")
    print("  normal 10 | transient_fault 30 | permanent_fault 90")
    print("  transformer_overload 120 | weather_lightning 60 | comms_loss 30")
    print()
    print("Simulating assets:")
    for f in FEEDERS:
        print(f"  {f['feeder_id']} → {f['breaker_id']}, {f['recloser_id']}, {f['transformer_id']}")

    while True:
        mode = state.get_mode()

        # ── Detect mode change → reset simulation variables ───────────────
        if mode != _last_mode:
            sv.reset_fault_state()
            if mode in ("normal", "comms_loss"):
                sv.reset_electrical()
            _last_mode = mode
            print(f"[MODE] → {mode}")

        # ── Comms loss: publish nothing ───────────────────────────────────
        if mode == "comms_loss":
            time.sleep(0.2)
            continue

        # ── Dispatch to scenario function ─────────────────────────────────
        if mode == "normal":
            telemetry_payload = scenario_normal(client)

        elif mode == "transient_fault":
            telemetry_payload = scenario_transient_fault(client)

        elif mode == "permanent_fault":
            telemetry_payload = scenario_permanent_fault(client)

        elif mode == "transformer_overload":
            telemetry_payload = scenario_transformer_overload(client)

        elif mode == "weather_lightning":
            telemetry_payload = scenario_weather_lightning(client)

        else:
            # Unknown mode — fall back to normal
            telemetry_payload = scenario_normal(client)

        # ── Always publish telemetry from primary feeder (1 Hz) ───────────
        pub(client, "telemetry", telemetry_payload)

        time.sleep(1.0)


if __name__ == "__main__":
    run_simulator()
