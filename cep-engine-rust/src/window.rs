// window.rs — WindowBuffer
//
// Replaces the Python WindowBuffer class.
// Tracks a sliding time window of events per feeder so multi-event rules
// (e.g. "3 failed reclosures within 30 seconds") can be evaluated.
//
// Uses DashMap for lock-free concurrent access from the async MQTT handler.

use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// One entry in the window: timestamp + tag (event sub-type)
#[derive(Clone)]
struct Entry {
    ts_ms: u64,
    tag:   String,
}

pub struct WindowBuffer {
    // feeder_id → list of recent entries
    store:     DashMap<String, Vec<Entry>>,
    window_ms: u64,
}

impl WindowBuffer {
    pub fn new(window_secs: u64) -> Self {
        Self {
            store:     DashMap::new(),
            window_ms: window_secs * 1_000,
        }
    }

    /// Add an event entry for a feeder
    pub fn add(&self, feeder_id: &str, tag: &str) {
        let now = now_ms();
        let mut entries = self.store.entry(feeder_id.to_string()).or_default();
        // prune expired entries first
        let cutoff = now.saturating_sub(self.window_ms);
        entries.retain(|e| e.ts_ms >= cutoff);
        entries.push(Entry { ts_ms: now, tag: tag.to_string() });
    }

    /// Count entries matching a tag within the window for a feeder
    pub fn count(&self, feeder_id: &str, tag: &str) -> usize {
        let now = now_ms();
        let cutoff = now.saturating_sub(self.window_ms);
        self.store
            .get(feeder_id)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| e.ts_ms >= cutoff && e.tag == tag)
                    .count()
            })
            .unwrap_or(0)
    }

    /// Count all entries within the window for a feeder (any tag)
    pub fn count_any(&self, feeder_id: &str) -> usize {
        let now = now_ms();
        let cutoff = now.saturating_sub(self.window_ms);
        self.store
            .get(feeder_id)
            .map(|entries| entries.iter().filter(|e| e.ts_ms >= cutoff).count())
            .unwrap_or(0)
    }

    /// Clear all entries for a feeder (call after lockout fires)
    pub fn clear(&self, feeder_id: &str) {
        if let Some(mut entries) = self.store.get_mut(feeder_id) {
            entries.clear();
        }
    }

    /// Return timestamp of last entry for a feeder (any tag), or None
    pub fn last_seen_ms(&self, feeder_id: &str) -> Option<u64> {
        self.store
            .get(feeder_id)
            .and_then(|entries| entries.iter().map(|e| e.ts_ms).max())
    }

    /// Return (feeder_id, last_seen_ms) for all known feeders.
    /// Used by CommsLossTracker to scan for stale feeders.
    pub fn all_last_seen(&self) -> Vec<(String, u64)> {
        self.store
            .iter()
            .filter_map(|entry| {
                let last = entry.value().iter().map(|e| e.ts_ms).max()?;
                Some((entry.key().clone(), last))
            })
            .collect()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
