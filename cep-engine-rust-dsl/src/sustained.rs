// sustained.rs — Pattern A: Sustained Condition Tracker
//
// Tracks how long a boolean condition has been continuously true
// per (feeder_id, key) pair.
//
// Used by CEP rules that require a condition to persist for N seconds
// before firing — e.g. "transformer temperature elevated for 3s (demo) /
// 15 min (production)".
//
// Design:
//   - observe(feeder, key, is_true):  records a "condition is currently true"
//     observation. Resets the start time if the condition was previously false.
//     Calling with is_true=false removes the entry, breaking continuity.
//   - has_persisted(feeder, key, secs): returns true if the condition has been
//     continuously true for at least `secs` seconds AND was seen recently
//     (within 2 s — prevents stale entries from firing after events stop).
//   - clear(feeder, key): removes the entry so the timer resets after firing,
//     preventing the rule from immediately re-firing.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

struct SustainedEntry {
    /// When the condition first became true (continuous start)
    started_at: Instant,
    /// Last time we observed the condition as true
    last_seen:  Instant,
}

pub struct SustainedTracker {
    inner: Mutex<HashMap<(String, String), SustainedEntry>>,
}

impl SustainedTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Record that the condition for (feeder, key) is currently `is_true`.
    ///
    /// - If `is_true`: upsert the entry — preserve `started_at` if already
    ///   present (continuity), update `last_seen` to now.
    /// - If `!is_true`: remove the entry, breaking the continuity streak.
    pub fn observe(&self, feeder: &str, key: &str, is_true: bool) {
        let mut map = self.inner.lock().unwrap();
        let k = (feeder.to_string(), key.to_string());
        if is_true {
            let now = Instant::now();
            let entry = map.entry(k).or_insert_with(|| SustainedEntry {
                started_at: now,
                last_seen:  now,
            });
            entry.last_seen = now;
        } else {
            map.remove(&k);
        }
    }

    /// Returns true if the condition has been continuously true for at least
    /// `for_secs` seconds AND was observed within the last 2 seconds.
    ///
    /// The 2-second recency check prevents stale entries from firing if
    /// the event stream stops (e.g. scenario ends but entry was never cleared).
    pub fn has_persisted(&self, feeder: &str, key: &str, for_secs: u64) -> bool {
        let map = self.inner.lock().unwrap();
        let k = (feeder.to_string(), key.to_string());
        if let Some(entry) = map.get(&k) {
            let duration_ok = entry.started_at.elapsed().as_secs() >= for_secs;
            let recently_seen = entry.last_seen.elapsed().as_secs() <= 2;
            duration_ok && recently_seen
        } else {
            false
        }
    }

    /// How long (seconds) the condition has been continuously true, or 0.
    pub fn duration_secs(&self, feeder: &str, key: &str) -> u64 {
        let map = self.inner.lock().unwrap();
        let k = (feeder.to_string(), key.to_string());
        map.get(&k)
            .map(|e| e.started_at.elapsed().as_secs())
            .unwrap_or(0)
    }

    /// Clear the entry after the rule fires so the timer resets.
    /// Without this the rule would fire again on the very next matching event.
    pub fn clear(&self, feeder: &str, key: &str) {
        let mut map = self.inner.lock().unwrap();
        map.remove(&(feeder.to_string(), key.to_string()));
    }
}
