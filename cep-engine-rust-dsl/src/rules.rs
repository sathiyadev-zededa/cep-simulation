// rules.rs — DSL loader and evaluation entry point
//
// Rules are defined in rules.yaml (path set via RULES_FILE env var).
// This module:
//   1. Loads and parses the YAML file at startup via load_rules()
//   2. Exposes evaluate() which iterates rules in priority order,
//      delegates to config::Rule::evaluate(), and handles early_return.
//
// To add or change a rule: edit rules.yaml and restart.  No recompile needed.

use crate::config::{Rule, RulesFile};
use crate::dedup::IncidentDedup;
use crate::types::{EdgeEvent, Incident};
use crate::window::WindowBuffer;
use std::sync::Arc;
use tracing::{debug, info};

/// Load and sort rules from a YAML file.
/// Panics with a clear message if the file is missing or malformed.
pub fn load_rules(path: &str) -> Vec<Rule> {
    let src = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("[CEP] Cannot read rules file '{}': {}", path, e));

    let mut cfg: RulesFile = serde_yaml::from_str(&src)
        .unwrap_or_else(|e| panic!("[CEP] Cannot parse rules file '{}': {}", path, e));

    // Sort ascending — priority 1 evaluates first
    cfg.rules.sort_by_key(|r| r.priority);

    info!("[CEP] Loaded {} rules from {}", cfg.rules.len(), path);
    for r in &cfg.rules {
        let win = r.window.as_ref()
            .map(|w| format!(" window={}s≥{}", w.seconds, w.count_gte))
            .unwrap_or_default();
        let early = if r.early_return { " [early_return]" } else { "" };
        info!(
            "[CEP]   [{:02}] {:<22} on:{:<22}{}{}",
            r.priority, r.id, r.on, win, early
        );
    }

    cfg.rules
}

/// Evaluate all loaded rules against one incoming event.
/// Returns a Vec of incidents to publish (may be empty).
pub fn evaluate(
    event:   &EdgeEvent,
    rules:   &[Rule],
    window:  &Arc<WindowBuffer>,
    dedup:   &Arc<IncidentDedup>,
    site_id: &str,
) -> Vec<Incident> {
    let mut out = Vec::new();

    // Track telemetry heartbeat per feeder (used by CommsLossTracker)
    if event.event_type == "telemetry" {
        window.add(&event.feeder(), "telemetry");
    }

    for rule in rules {
        debug!("[CEP] checking [{:02}] {} on {}", rule.priority, rule.id, event.event_type);

        if let Some(incident) = rule.evaluate(event, window, dedup, site_id) {
            let early = rule.early_return;
            out.push(incident);
            if early {
                debug!("[CEP] early_return after rule {}", rule.id);
                break;
            }
        }
    }

    out
}
