// aggregator.rs — FaultEventAggregator
//
// Groups related incidents from the same feeder that arrive within a short
// collection window (5 s) into a single FaultEvent published to edge/fault_events.
//
// Architecture:
//   edge/incidents  ──► aggregator.add()  (called from main for HIGH/CRITICAL incidents)
//   aggregator      ──► edge/fault_events  (published by spawn_flusher every 1 s)
//
// FaultEvent structure:
//   root_cause           — incident_type of the highest-severity incident
//   severity             — severity of the root cause
//   confidence           — confidence of the root cause
//   message              — summary of the root cause incident
//   contributing_factors — incident_types of all other incidents in the window
//   incident_count       — total incidents in this fault event
//
// This keeps the LLM service input clean — one structured FaultEvent per fault
// instead of a cascade of individual incidents.

use crate::types::Incident;
use rumqttc::{AsyncClient, QoS};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info};

// ─── FaultEvent ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct FaultEvent {
    pub event_type:          String,  // always "fault_event"
    pub ts_ms:               u64,
    pub feeder_id:           String,
    pub root_cause:          String,
    pub severity:            String,
    pub confidence:          f64,
    pub message:             String,
    pub contributing_factors: Vec<String>,
    pub incident_count:      usize,
}

// ─── Aggregation window ───────────────────────────────────────────────────────

struct WindowEntry {
    incidents:  Vec<Incident>,
    created_at: Instant,
}

// ─── FaultEventAggregator ─────────────────────────────────────────────────────

pub struct FaultEventAggregator {
    windows:     Mutex<HashMap<String, WindowEntry>>,
    window_secs: u64,
}

impl FaultEventAggregator {
    pub fn new(window_secs: u64) -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
            window_secs,
        }
    }

    /// Add a HIGH or CRITICAL incident to the feeder's open collection window.
    /// If no window exists for this feeder, one is created now.
    pub fn add(&self, incident: Incident) {
        let feeder = incident.payload.feeder_id.clone();
        let mut map = self.windows.lock().unwrap();
        map.entry(feeder)
            .or_insert_with(|| WindowEntry {
                incidents:  Vec::new(),
                created_at: Instant::now(),
            })
            .incidents
            .push(incident);
    }

    /// Return and remove all windows that have exceeded the collection window.
    /// Called every 1 second by the background flusher task.
    pub fn drain_expired(&self) -> Vec<FaultEvent> {
        let mut map     = self.windows.lock().unwrap();
        let mut expired = Vec::new();
        let mut to_remove: Vec<String> = Vec::new();

        for (feeder, entry) in map.iter() {
            if entry.created_at.elapsed().as_secs() >= self.window_secs {
                if let Some(fe) = build_fault_event(feeder, &entry.incidents) {
                    expired.push(fe);
                }
                to_remove.push(feeder.clone());
            }
        }

        for k in to_remove {
            map.remove(&k);
        }

        expired
    }
}

// ─── FaultEvent builder ───────────────────────────────────────────────────────

fn sev_rank(severity: &str) -> u8 {
    match severity.to_ascii_uppercase().as_str() {
        "CRITICAL" => 4,
        "HIGH"     => 3,
        "MEDIUM"   => 2,
        "LOW"      => 1,
        _          => 0,
    }
}

fn build_fault_event(feeder: &str, incidents: &[Incident]) -> Option<FaultEvent> {
    if incidents.is_empty() {
        return None;
    }

    // Select root cause: highest-severity incident (first one wins on tie)
    let root_idx = incidents
        .iter()
        .enumerate()
        .max_by_key(|(_, i)| sev_rank(&i.payload.severity))?
        .0;

    let root = &incidents[root_idx];

    // All other incident types become contributing factors
    let contributing: Vec<String> = incidents
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != root_idx)
        .map(|(_, i)| i.payload.incident_type.clone())
        .collect();

    Some(FaultEvent {
        event_type:           "fault_event".to_string(),
        ts_ms:                now_ms(),
        feeder_id:            feeder.to_string(),
        root_cause:           root.payload.incident_type.clone(),
        severity:             root.payload.severity.clone(),
        confidence:           root.payload.confidence,
        message:              root.payload.summary.clone(),
        contributing_factors: contributing,
        incident_count:       incidents.len(),
    })
}

// ─── Background flusher ───────────────────────────────────────────────────────

/// Spawn a background Tokio task that flushes expired aggregation windows
/// every 1 second and publishes resulting FaultEvents to edge/fault_events.
pub fn spawn_flusher(aggregator: Arc<FaultEventAggregator>, client: AsyncClient) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;

            let events = aggregator.drain_expired();

            for fe in events {
                // Capture fields for logging before serialisation consumes fe
                let root    = fe.root_cause.clone();
                let sev     = fe.severity.clone();
                let feeder  = fe.feeder_id.clone();
                let factors = fe.contributing_factors.clone();
                let count   = fe.incident_count;

                match serde_json::to_string(&fe) {
                    Ok(payload) => {
                        if let Err(e) = client
                            .publish("edge/fault_events", QoS::AtLeastOnce, false, payload)
                            .await
                        {
                            error!("[AGG] Failed to publish fault_event: {}", e);
                        } else {
                            info!(
                                "→ [FAULT_EVENT] {} | {} | feeder={} | incidents={} | factors={:?}",
                                root, sev, feeder, count, factors
                            );
                        }
                    }
                    Err(e) => error!("[AGG] Failed to serialise fault_event: {}", e),
                }
            }
        }
    });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
