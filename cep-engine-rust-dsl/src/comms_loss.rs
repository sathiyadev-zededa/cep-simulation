// comms_loss.rs — CommsLossTracker
//
// Replaces the Python CommsLossTracker background thread.
// Runs as a Tokio background task. Fires a COMMS_LOSS incident if no
// telemetry is received from a feeder for more than TIMEOUT_SECS seconds.
//
// The MQTT publish sender is passed in so this task can publish independently.

use crate::suppression::FaultSuppression;
use crate::types::{Incident, IncidentPayload};
use crate::window::WindowBuffer;
use rumqttc::AsyncClient;
use rumqttc::QoS;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::interval;
use tracing::{info, warn};
use uuid::Uuid;

const TIMEOUT_SECS: u64  = 15;    // fire COMMS_LOSS if gap > 15s
const CHECK_INTERVAL: u64 = 5;    // check every 5 seconds

/// Spawn a background task that monitors telemetry gaps per feeder.
/// `telemetry_window` is the same WindowBuffer used by the rules engine,
/// populated whenever a "telemetry" event arrives.
/// `suppression` prevents COMMS_LOSS firing right after a CRITICAL fault —
/// the fault IS the reason for the gap, so COMMS_LOSS would be noise.
pub fn spawn(
    client:           AsyncClient,
    telemetry_window: Arc<WindowBuffer>,
    suppression:      Arc<FaultSuppression>,
    site_id:          String,
) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(CHECK_INTERVAL));
        // track which feeders we have already alerted so we don't flood
        let mut alerted: HashMap<String, u64> = HashMap::new();

        loop {
            ticker.tick().await;
            let now = now_ms();

            for (feeder_id, last_ms) in telemetry_window.all_last_seen() {
                let gap_ms = now.saturating_sub(last_ms);

                if gap_ms > TIMEOUT_SECS * 1_000 {
                    // If a CRITICAL fault recently fired for this feeder,
                    // skip COMMS_LOSS — the fault is the root cause of the gap.
                    if suppression.is_suppressed(&feeder_id) {
                        info!("[COMMS_LOSS] Feeder {} gap {}s — suppressed by active CRITICAL fault", feeder_id, gap_ms / 1_000);
                        continue;
                    }

                    // Only alert once per feeder per 3x TIMEOUT window
                    let already_alerted = alerted
                        .get(feeder_id.as_str())
                        .map(|t| now.saturating_sub(*t) < TIMEOUT_SECS * 1_000 * 3)
                        .unwrap_or(false);

                    if !already_alerted {
                        warn!("[COMMS_LOSS] No telemetry from {} for {}s", feeder_id, gap_ms / 1_000);
                        alerted.insert(feeder_id.clone(), now);

                        let incident = make_comms_loss(&feeder_id, &site_id, gap_ms / 1_000);
                        if let Ok(payload) = serde_json::to_string(&incident) {
                            let _ = client
                                .publish("edge/incidents", QoS::AtLeastOnce, false, payload)
                                .await;
                            info!("[CEP] → COMMS_LOSS feeder={}", feeder_id);
                        }
                    }
                } else {
                    // Telemetry resumed — clear alert state
                    alerted.remove(&feeder_id);
                }
            }
        }
    });
}

fn make_comms_loss(feeder_id: &str, site_id: &str, gap_secs: u64) -> Incident {
    let mut details = HashMap::new();
    details.insert(
        "gap_secs".to_string(),
        serde_json::Value::Number(gap_secs.into()),
    );

    Incident {
        id:         format!("inc-{}", Uuid::new_v4().simple()),
        event_type: "incident".to_string(),
        ts_ms:      now_ms(),
        site_id:    site_id.to_string(),
        payload: IncidentPayload {
            incident_type:  "COMMS_LOSS".to_string(),
            severity:       "HIGH".to_string(),
            confidence:     0.90,
            summary:        format!(
                "No telemetry from {} for {}s — possible communication failure",
                feeder_id, gap_secs
            ),
            feeder_id:      feeder_id.to_string(),
            fault_scenario: None,
            relay_codes:    None,
            details,
        },
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
