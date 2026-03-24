// rules.rs — All 13 CEP rules
//
// Direct port of cep_engine.py rules to Rust.
// Rules are evaluated in priority order (1 = highest).
// Early return after CRITICAL rules prevents lower-priority rules
// from firing on the same event when a higher-priority match is found.
//
// Each rule calls make_incident() which checks IncidentDedup before firing.

use crate::dedup::IncidentDedup;
use crate::types::{EdgeEvent, Incident, IncidentPayload};
use crate::window::WindowBuffer;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;
use uuid::Uuid;

// ─── PUBLIC ENTRY POINT ───────────────────────────────────────────────────────

/// Evaluate all 13 rules against one incoming event.
/// Returns a Vec of incidents to publish (may be empty).
pub fn evaluate(
    event:  &EdgeEvent,
    window: &Arc<WindowBuffer>,
    dedup:  &Arc<IncidentDedup>,
) -> Vec<Incident> {
    let mut out = Vec::new();

    // Track telemetry heartbeat per feeder (used by CommsLossTracker)
    if event.event_type == "telemetry" {
        window.add(&event.feeder(), "telemetry");
    }

    // ── Priority 1: Busbar fault ─────────────────────────────────────────────
    if let Some(rc) = event.str_field("relay_code") {
        if event.event_type == "relay_trip" && (rc == "87B" || rc == "50BF") {
            debug!("Rule 1 match: busbar relay {}", rc);
            if let Some(inc) = make_incident(
                "BUSBAR_FAULT", "CRITICAL", 0.95,
                &format!("Busbar protection relay {} operated — possible busbar fault", rc),
                Some(vec![rc.to_string()]),
                None, event, dedup,
            ) {
                out.push(inc);
            }
            return out;  // highest priority — stop evaluating
        }

        // ── Priority 2: Transformer fault ────────────────────────────────────
        if event.event_type == "relay_trip" && (rc == "87T" || rc == "63") {
            debug!("Rule 2 match: transformer relay {}", rc);
            if let Some(inc) = make_incident(
                "TRANSFORMER_FAULT", "CRITICAL", 0.92,
                &format!("Transformer protection relay {} operated — differential or gas relay", rc),
                Some(vec![rc.to_string()]),
                None, event, dedup,
            ) {
                out.push(inc);
            }
            return out;
        }

        // ── Priority 3: Distance / cable fault ───────────────────────────────
        if event.event_type == "relay_trip" && (rc == "21" || rc == "67") {
            debug!("Rule 3 match: distance relay {}", rc);
            if let Some(inc) = make_incident(
                "CABLE_FAULT", "HIGH", 0.88,
                &format!("Distance relay {} operated — possible cable insulation fault", rc),
                Some(vec![rc.to_string()]),
                Some("CABLE_INSULATION_FAILURE".to_string()),
                event, dedup,
            ) {
                out.push(inc);
            }
        }

        // ── Priority 4: Overcurrent ───────────────────────────────────────────
        if event.event_type == "relay_trip"
            && (rc == "50" || rc == "51" || rc == "50/51")
        {
            debug!("Rule 4 match: overcurrent relay {}", rc);
            if let Some(inc) = make_incident(
                "RELAY_TRIP", "HIGH", 0.85,
                &format!("Overcurrent relay {} operated on {}", rc, event.feeder()),
                Some(vec![rc.to_string()]),
                None, event, dedup,
            ) {
                out.push(inc);
            }
        }
    }

    // ── Priority 5: Breaker lockout ───────────────────────────────────────────
    if event.event_type == "breaker_reclose" {
        let success = event.bool_field("success").unwrap_or(true);
        let feeder  = event.feeder();

        if !success {
            window.add(&feeder, "reclose_fail");
            let fail_count = window.count(&feeder, "reclose_fail");
            debug!("Reclose fail #{} for {}", fail_count, feeder);

            if fail_count >= 3 {
                if let Some(inc) = make_incident(
                    "PERMANENT_FAULT", "CRITICAL", 0.95,
                    &format!(
                        "Breaker lockout on {} after {} failed reclose attempts",
                        feeder, fail_count
                    ),
                    None,
                    Some("BREAKER_LOCKOUT".to_string()),
                    event, dedup,
                ) {
                    window.clear(&feeder);  // reset so it can fire again later
                    out.push(inc);
                }
            }
        }

        // ── Priority 6: Reclose success ──────────────────────────────────────
        if success {
            window.clear(&feeder);  // clear fail count — transient, cleared
            if let Some(inc) = make_incident(
                "RECLOSE_SUCCESS", "LOW", 0.90,
                &format!("Auto-reclose successful on {} — transient fault cleared", feeder),
                None, None, event, dedup,
            ) {
                out.push(inc);
            }
        }
    }

    // ── Priority 7: Equipment offline ─────────────────────────────────────────
    if event.event_type == "equipment_offline" {
        let asset = event.str_field("asset").unwrap_or("unknown asset");
        if let Some(inc) = make_incident(
            "EQUIPMENT_OFFLINE", "HIGH", 0.88,
            &format!("Equipment offline: {} removed from service on {}", asset, event.feeder()),
            None, None, event, dedup,
        ) {
            out.push(inc);
        }
    }

    // ── Priority 8: Zone isolation ────────────────────────────────────────────
    if event.event_type == "zone_isolation" {
        let zone = event.str_field("zone").unwrap_or("unknown zone");
        if let Some(inc) = make_incident(
            "ZONE_ISOLATION", "HIGH", 0.90,
            &format!("Zone {} isolated on {} — section removed from service", zone, event.feeder()),
            None, None, event, dedup,
        ) {
            out.push(inc);
        }
    }

    // ── Priority 9: Transformer overload ──────────────────────────────────────
    if event.event_type == "telemetry" {
        if let Some(load_pct) = event.f64_field("load_pct") {
            if load_pct > 105.0 {
                let transformer_id = event.str_field("transformer_id")
                    .unwrap_or("unknown transformer");
                if let Some(inc) = make_incident(
                    "TRANSFORMER_OVERLOAD",
                    if load_pct > 120.0 { "CRITICAL" } else { "HIGH" },
                    0.85,
                    &format!(
                        "Transformer {} load warning: {:.1}% of rated capacity on {}",
                        transformer_id, load_pct, event.feeder()
                    ),
                    None, None, event, dedup,
                ) {
                    out.push(inc);
                }
            }
        }

        // ── Priority 10: Pre-fault warning ───────────────────────────────────
        let v_sag  = event.f64_field("v_sag_pct").unwrap_or(0.0);
        let i_spike = event.f64_field("i_spike_pct").unwrap_or(0.0);
        if v_sag > 10.0 || i_spike > 20.0 {
            window.add(&event.feeder(), "pre_fault");
            if let Some(inc) = make_incident(
                "PRE_FAULT_WARNING", "MEDIUM", 0.70,
                &format!(
                    "Pre-fault condition on {}: voltage sag {:.1}% / current spike {:.1}%",
                    event.feeder(), v_sag, i_spike
                ),
                None, None, event, dedup,
            ) {
                out.push(inc);
            }
        }
    }

    // ── Priority 11: Voltage quality (3+ harmonics in 60s) ───────────────────
    if event.event_type == "voltage_harmonics" {
        let feeder = event.feeder();
        window.add(&feeder, "harmonics");
        let count = window.count(&feeder, "harmonics");
        debug!("Harmonic event #{} for {}", count, feeder);
        if count >= 3 {
            if let Some(inc) = make_incident(
                "VOLTAGE_QUALITY_ISSUE", "MEDIUM", 0.75,
                &format!(
                    "Repeated voltage harmonic events on {}: {} occurrences in 60s",
                    feeder, count
                ),
                None, None, event, dedup,
            ) {
                out.push(inc);
            }
        }
    }

    // ── Priority 12: Lightning impact ─────────────────────────────────────────
    if event.event_type == "lightning_strike" {
        let ka = event.f64_field("strike_kA").unwrap_or(0.0);
        if let Some(inc) = make_incident(
            "LIGHTNING_IMPACT", "HIGH", 0.80,
            &format!(
                "Lightning strike detected on {}: {:.1} kA — possible surge damage",
                event.feeder(), ka
            ),
            None, None, event, dedup,
        ) {
            out.push(inc);
        }
    }

    // ── Priority 13: Frequency deviation (2+ events in 60s) ──────────────────
    if event.event_type == "frequency_deviation" {
        let feeder = event.feeder();
        window.add(&feeder, "freq_dev");
        let count = window.count(&feeder, "freq_dev");
        if count >= 2 {
            let freq_hz = event.f64_field("freq_hz").unwrap_or(50.0);
            if let Some(inc) = make_incident(
                "FREQUENCY_DEVIATION", "MEDIUM", 0.72,
                &format!(
                    "Repeated frequency deviation on {}: {:.2} Hz ({} events in 60s)",
                    feeder, freq_hz, count
                ),
                None, None, event, dedup,
            ) {
                out.push(inc);
            }
        }
    }

    out
}

// ─── HELPER ───────────────────────────────────────────────────────────────────

fn make_incident(
    incident_type:  &str,
    severity:       &str,
    confidence:     f64,
    summary:        &str,
    relay_codes:    Option<Vec<String>>,
    fault_scenario: Option<String>,
    event:          &EdgeEvent,
    dedup:          &Arc<IncidentDedup>,
) -> Option<Incident> {
    let feeder = event.feeder();

    // Check dedup — suppress if within cooldown window
    if !dedup.should_fire(incident_type, &feeder) {
        debug!("Dedup suppressed {} for {}", incident_type, feeder);
        return None;
    }

    Some(Incident {
        id:         format!("inc-{}", Uuid::new_v4().simple()),
        event_type: "incident".to_string(),
        ts_ms:      event.ts_ms,
        site_id:    event.site(),
        payload: IncidentPayload {
            incident_type:  incident_type.to_string(),
            severity:       severity.to_string(),
            confidence,
            summary:        summary.to_string(),
            feeder_id:      feeder,
            fault_scenario,
            relay_codes,
            details:        event.payload.clone(),
        },
    })
}

fn _now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
