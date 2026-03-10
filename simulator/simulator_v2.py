import json
import os
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt


MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_EVENTS   = "edge/events"
TOPIC_COMMANDS = "edge/commands"

SITE_ID = "SUBSTATION-ABU-01"

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


def scenario_permanent_fault(client: mqtt.Client) -> Dict:
    """
    Permanent fault — reclose fails repeatedly, breaker locks out, AMI last-gasp.
      Ticks 0–5  : pre-fault (harmonics, partial discharge, voltage drift)
      Tick  6    : hard fault — deep voltage sag, relay trip, breaker opens
      Ticks 7–25 : up to 3 reclose attempts, all fail
      Tick  ~25  : lockout declared, AMI burst, feeder stays dead
    """
    sv.tick += 1
    t = sv.tick

    # ── Pre-fault period ─────────────────────────────────────────────────
    if t <= 6 and not sv.pre_fault_done:
        sv.harmonics_thd = min(28.0, sv.harmonics_thd + random.uniform(2.0, 4.5))
        sv.v_kv  = max(9.8, sv.v_kv - random.uniform(0.1, 0.25))
        sv.power_factor = max(0.78, sv.power_factor - random.uniform(0.02, 0.04))
        sv.freq_hz = max(49.88, sv.freq_hz - random.uniform(0.01, 0.03))

        pub(client, "voltage_harmonics", {
            "thd_pct":       round(sv.harmonics_thd, 2),
            "threshold_pct": 8.0,
            "severity":      "critical" if sv.harmonics_thd > 15 else "warning",
            "description":   "Severe harmonic distortion — sustained insulation failure suspected",
        })

        pub(client, "partial_discharge", {
            "magnitude_db": round(random.uniform(25, 55), 1),
            "phase":        random.choice(["A", "B", "C"]),
            "count_per_min": random.randint(40, 120),
            "description":  "Repeated partial discharge — insulation breakdown in progress",
        })

        # Frequency deviation on adjacent feeder (FEEDER-13, same transformer TX-12)
        if t >= 3:
            pub(client, "frequency_deviation", {
                "frequency_hz":  round(sv.freq_hz, 3),
                "deviation_hz":  round(sv.freq_hz - 50.0, 3),
                "source_feeder": "FEEDER-12",
                "description":   "Frequency deviation detected on TX-12 bus",
            }, FEEDERS[1])  # published from FEEDER-13 perspective

    # ── Main fault trigger ────────────────────────────────────────────────
    if t == 7 and not sv.fault_triggered:
        sv.fault_triggered = True
        sv.v_kv  = random.uniform(4.5, 7.5)   # deep sag — hard fault
        sv.i_a   = random.uniform(600, 950)    # very high fault current
        sv.freq_hz = 49.80

        pub(client, "voltage_sag", {
            "v_kv":             round(sv.v_kv, 2),
            "duration_ms_hint": 15000,
            "fault_current_a":  round(sv.i_a, 1),
            "fault_type":       "permanent",
            "pre_fault_thd":    round(sv.harmonics_thd, 2),
            "description":      "Deep voltage sag — hard fault detected, likely phase-to-ground",
        })

        pub(client, "relay_trip", {
            "code":        random.choice(["50", "51"]),
            "phase":       random.choice(["A", "B", "C", "ABC"]),
            "fault_type":  "permanent",
            "i_fault_a":   round(sv.i_a, 1),
            "description": "Instantaneous overcurrent trip — high fault current",
        })

        sv.breaker_closed = False
        sv.i_a = 0.0
        pub(client, "breaker_open", {
            "reason":      "trip",
            "source":      "relay_trip",
            "fault_type":  "permanent",
            "description": "Breaker opened on permanent fault — all downstream customers de-energised",
        })

        sv.pre_fault_done = True

    # ── Reclose attempts — all fail ───────────────────────────────────────
    if not sv.breaker_closed and not sv.lockout and sv.fault_triggered:
        max_attempts = 3
        if sv.reclose_attempts < max_attempts and t in [10, 16, 22]:
            sv.reclose_attempts += 1
            pub(client, "reclose_attempt", {
                "attempt":       sv.reclose_attempts,
                "max_attempts":  max_attempts,
                "delay_s":       random.choice([5, 10, 15]),
                "description":   f"Reclose attempt {sv.reclose_attempts}/{max_attempts} — fault still present",
            })
            pub(client, "breaker_open", {
                "reason":        "reclose_failed",
                "attempt":       sv.reclose_attempts,
                "fault_type":    "permanent",
                "description":   "Reclose failed — fault impedance unchanged, breaker re-opened",
            })

            # Partial restoration of voltage briefly then collapses
            sv.v_kv = random.uniform(7.0, 9.0)
            time.sleep(0.1)   # brief restore
            sv.v_kv = random.uniform(3.0, 6.0)

        # Lockout after 3 failed attempts
        if sv.reclose_attempts >= max_attempts and not sv.lockout:
            sv.lockout = True
            n_customers = random.randint(35, 180)

            pub(client, "ami_last_gasp_burst", {
                "count":        n_customers,
                "window_s":     20,
                "status":       "lockout",
                "feeder_id":    PRIMARY_FEEDER["feeder_id"],
                "description":  f"{n_customers} customer meters reported power loss (AMI last-gasp)",
            })

            # FEEDER-13 on same transformer also sees disturbance
            pub(client, "voltage_sag", {
                "v_kv":        round(random.uniform(9.5, 10.2), 2),
                "duration_ms_hint": 3000,
                "cause":       "adjacent_feeder_fault",
                "description": "Voltage disturbance on FEEDER-13 due to FEEDER-12 permanent fault",
            }, FEEDERS[1])

    return nominal_telemetry(
        sv.v_kv, sv.i_a, sv.tx_load_pct, sv.tx_temp_c,
        sv.power_factor, sv.harmonics_thd, sv.freq_hz, "permanent_fault"
    ) | {"breaker_closed": sv.breaker_closed, "reclose_attempts": sv.reclose_attempts}


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
