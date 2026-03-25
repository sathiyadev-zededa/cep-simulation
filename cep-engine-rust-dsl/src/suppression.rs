use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// When a CRITICAL incident fires for a feeder, suppress all
/// lower-severity incidents for that feeder for `duration` seconds.
/// This ensures one root-cause incident per fault event, not a cascade.
pub struct FaultSuppression {
    suppressed: Mutex<HashMap<String, Instant>>,
    duration: Duration,
}

impl FaultSuppression {
    pub fn new(secs: u64) -> Self {
        Self {
            suppressed: Mutex::new(HashMap::new()),
            duration: Duration::from_secs(secs),
        }
    }

    /// Mark a feeder as suppressed starting now.
    pub fn suppress(&self, feeder_id: &str) {
        let mut map = self.suppressed.lock().unwrap();
        map.insert(feeder_id.to_string(), Instant::now());
        tracing::info!("Suppressing feeder {} for {}s after CRITICAL fault", feeder_id, self.duration.as_secs());
    }

    /// Returns true if this feeder is currently within the suppression window.
    pub fn is_suppressed(&self, feeder_id: &str) -> bool {
        let map = self.suppressed.lock().unwrap();
        if let Some(t) = map.get(feeder_id) {
            t.elapsed() < self.duration
        } else {
            false
        }
    }

    /// Clear suppression early (e.g. when fault is cleared/resolved).
    pub fn clear(&self, feeder_id: &str) {
        self.suppressed.lock().unwrap().remove(feeder_id);
    }
}
