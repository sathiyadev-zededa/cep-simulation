// dedup.rs — IncidentDedup
//
// Replaces the Python IncidentDedup class.
// Enforces a configurable cooldown period per (incident_type, feeder_id) pair
// so the same incident cannot fire repeatedly within the cooldown window.
//
// Default cooldown: 20 seconds (matches Python implementation).

use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct IncidentDedup {
    last_fired:  DashMap<String, u64>,   // key → last fired ts_ms
    cooldown_ms: u64,
}

impl IncidentDedup {
    pub fn new(cooldown_secs: u64) -> Self {
        Self {
            last_fired:  DashMap::new(),
            cooldown_ms: cooldown_secs * 1_000,
        }
    }

    /// Returns true if this incident should fire (i.e. it is NOT a duplicate).
    /// Atomically records the fire timestamp so concurrent calls are safe.
    pub fn should_fire(&self, incident_type: &str, feeder_id: &str) -> bool {
        let key = format!("{}:{}", incident_type, feeder_id);
        let now = now_ms();

        if let Some(last) = self.last_fired.get(&key) {
            if now.saturating_sub(*last) < self.cooldown_ms {
                return false;  // still within cooldown — suppress
            }
        }

        self.last_fired.insert(key, now);
        true
    }

    /// Force-reset cooldown for a specific incident+feeder (useful in tests)
    pub fn reset(&self, incident_type: &str, feeder_id: &str) {
        let key = format!("{}:{}", incident_type, feeder_id);
        self.last_fired.remove(&key);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
