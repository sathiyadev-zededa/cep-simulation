// types.rs — shared data structures for the CEP engine

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// ─── INBOUND: raw event from simulator ───────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct EdgeEvent {
    pub id:         Option<String>,
    pub event_type: String,
    pub ts_ms:      u64,
    pub site_id:    Option<String>,
    pub feeder_id:  Option<String>,
    pub payload:    HashMap<String, Value>,
}

impl EdgeEvent {
    /// Convenience: get a string field from payload
    pub fn str_field(&self, key: &str) -> Option<&str> {
        self.payload.get(key)?.as_str()
    }

    /// Convenience: get an f64 field from payload
    pub fn f64_field(&self, key: &str) -> Option<f64> {
        self.payload.get(key)?.as_f64()
    }

    /// Convenience: get a bool field from payload
    pub fn bool_field(&self, key: &str) -> Option<bool> {
        self.payload.get(key)?.as_bool()
    }

    pub fn feeder(&self) -> String {
        self.feeder_id.clone().unwrap_or_else(|| "—".to_string())
    }

    pub fn site(&self) -> String {
        self.site_id.clone().unwrap_or_else(|| "SUBSTATION-ABU-01".to_string())
    }
}

// ─── OUTBOUND: incident published to edge/incidents ──────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct Incident {
    pub id:         String,
    pub event_type: String,   // always "incident"
    pub ts_ms:      u64,
    pub site_id:    String,
    pub payload:    IncidentPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct IncidentPayload {
    pub incident_type:  String,
    pub severity:       String,
    pub confidence:     f64,
    pub summary:        String,
    pub feeder_id:      String,
    pub fault_scenario: Option<String>,
    pub relay_codes:    Option<Vec<String>>,
    pub details:        HashMap<String, Value>,
}

// ─── OUTBOUND: reclose command published to edge/commands ────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct Command {
    pub id:         String,
    pub event_type: String,   // "reclose_command" | "zone_isolation"
    pub ts_ms:      u64,
    pub site_id:    String,
    pub feeder_id:  String,
    pub payload:    HashMap<String, Value>,
}
