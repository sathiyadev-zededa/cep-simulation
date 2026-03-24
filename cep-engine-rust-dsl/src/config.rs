// config.rs — YAML rule DSL: structs + per-rule evaluation
//
// Rules are defined in rules.yaml.  This module owns:
//   - Deserialisation structs that mirror the YAML schema
//   - Field access helpers (top-level event fields + payload HashMap)
//   - Condition matching (in / gt / gte / lt / lte / eq / bool / any)
//   - Template interpolation for `message` strings
//   - Rule::evaluate() which runs one rule against one event

use crate::dedup::IncidentDedup;
use crate::types::{EdgeEvent, Incident, IncidentPayload};
use crate::window::WindowBuffer;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

// ─── YAML schema ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RulesFile {
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub id:          String,
    pub priority:    u8,
    pub description: Option<String>,
    /// event_type that must match for this rule to be evaluated
    pub on:          String,
    /// WHERE conditions — HashMap captures all keys including `any`
    #[serde(rename = "where", default)]
    pub conditions:  HashMap<String, serde_yaml::Value>,
    pub window:      Option<WindowConfig>,
    /// Reset the window bucket after firing (e.g. breaker lockout)
    #[serde(default)]
    pub clear_window_on_fire: bool,
    pub fire:        FireConfig,
    /// Stop evaluating lower-priority rules after this one fires
    #[serde(default)]
    pub early_return: bool,
}

#[derive(Debug, Deserialize)]
pub struct WindowConfig {
    /// Bucket name inside WindowBuffer — scoped per feeder automatically
    pub bucket:    String,
    /// Look-back period in seconds
    pub seconds:   u64,
    /// Minimum event count within the window to fire
    pub count_gte: usize,
}

#[derive(Debug, Deserialize)]
pub struct FireConfig {
    #[serde(rename = "type")]
    pub incident_type:    String,
    pub severity:         String,
    pub confidence:       f64,
    pub message:          Option<String>,
    pub fault_scenario:   Option<String>,
    /// Name of the payload field whose value populates relay_codes[]
    pub relay_codes_from: Option<String>,
    /// Severity escalation overrides (checked in order; first match wins)
    pub escalate:         Option<Vec<Escalation>>,
}

#[derive(Debug, Deserialize)]
pub struct Escalation {
    pub when:       HashMap<String, serde_yaml::Value>,
    pub severity:   String,
    pub confidence: f64,
}

// ─── Rule evaluation ─────────────────────────────────────────────────────────

impl Rule {
    /// Evaluate this rule against one incoming event.
    /// Returns Some(Incident) if the rule fires, None otherwise.
    pub fn evaluate(
        &self,
        event:   &EdgeEvent,
        window:  &Arc<WindowBuffer>,
        dedup:   &Arc<IncidentDedup>,
        site_id: &str,
    ) -> Option<Incident> {

        // 1. event_type gate
        if event.event_type != self.on {
            return None;
        }

        // 2. WHERE conditions
        if !self.conditions.is_empty() && !matches_where(&self.conditions, event) {
            return None;
        }

        // 3. Window accumulation + threshold check
        let feeder        = event.feeder();
        let window_count  = if let Some(win) = &self.window {
            window.add(&feeder, &win.bucket);
            let n = window.count_within(&feeder, &win.bucket, win.seconds);
            debug!("[{}] window {}/{} feeder={}", self.id, n, win.count_gte, feeder);
            if n < win.count_gte {
                return None;
            }
            n
        } else {
            0
        };

        // 4. Dedup / cooldown
        if !dedup.should_fire(&self.fire.incident_type, &feeder) {
            debug!("[{}] dedup suppressed feeder={}", self.id, feeder);
            return None;
        }

        // 5. Clear window bucket if configured
        if self.clear_window_on_fire {
            if let Some(win) = &self.window {
                window.clear_bucket(&feeder, &win.bucket);
            }
        }

        // 6. Resolve severity (escalation overrides base)
        let (severity, confidence) = self.resolve_severity(event);

        // 7. Build description message from template
        let message = self.fire.message.as_deref()
            .map(|tmpl| interpolate(tmpl, event, window_count))
            .unwrap_or_else(|| {
                format!("{} detected on {}", self.fire.incident_type, feeder)
            });

        // 8. Extract relay codes from payload if configured
        let relay_codes = self.fire.relay_codes_from.as_deref()
            .and_then(|field| get_str_field(event, field))
            .map(|v| vec![v]);

        Some(Incident {
            id:         format!("inc-{}", Uuid::new_v4().simple()),
            event_type: "incident".to_string(),
            ts_ms:      event.ts_ms,
            site_id:    site_id.to_string(),
            payload:    IncidentPayload {
                incident_type:  self.fire.incident_type.clone(),
                severity,
                confidence,
                summary:        message,
                feeder_id:      feeder,
                fault_scenario: self.fire.fault_scenario.clone(),
                relay_codes,
                details:        event.payload.clone(),
            },
        })
    }

    /// Check escalation conditions in order; return base if none match.
    fn resolve_severity(&self, event: &EdgeEvent) -> (String, f64) {
        if let Some(escalations) = &self.fire.escalate {
            for esc in escalations {
                if matches_where(&esc.when, event) {
                    return (esc.severity.clone(), esc.confidence);
                }
            }
        }
        (self.fire.severity.clone(), self.fire.confidence)
    }
}

// ─── WHERE clause evaluation ─────────────────────────────────────────────────

/// Evaluate a map of field conditions against an event.
/// All keys must match (AND).  The special `any` key is an OR list.
fn matches_where(
    conditions: &HashMap<String, serde_yaml::Value>,
    event:      &EdgeEvent,
) -> bool {
    for (key, cond_val) in conditions {
        if key == "any" {
            // OR — at least one sub-map must match entirely
            if let serde_yaml::Value::Sequence(or_list) = cond_val {
                let any_ok = or_list.iter().any(|item| {
                    if let serde_yaml::Value::Mapping(sub_map) = item {
                        sub_map.iter().all(|(k, v)| {
                            let field = k.as_str().unwrap_or("");
                            get_json_field(event, field)
                                .map(|fv| matches_condition(&fv, v))
                                .unwrap_or(false)
                        })
                    } else {
                        false
                    }
                });
                if !any_ok {
                    return false;
                }
            }
        } else {
            // Regular AND condition
            match get_json_field(event, key) {
                Some(field_val) => {
                    if !matches_condition(&field_val, cond_val) {
                        return false;
                    }
                }
                None => return false,
            }
        }
    }
    true
}

/// Compare a serde_json field value against a serde_yaml condition.
fn matches_condition(
    field: &serde_json::Value,
    cond:  &serde_yaml::Value,
) -> bool {
    match cond {
        // Simple bool: `success: false`
        serde_yaml::Value::Bool(b) => {
            field.as_bool().map(|v| v == *b).unwrap_or(false)
        }

        // Simple string: `event_type: "relay_trip"`
        serde_yaml::Value::String(s) => {
            field.as_str().map(|v| v == s.as_str()).unwrap_or(false)
        }

        // Numeric equality: `load_pct: 105`
        serde_yaml::Value::Number(n) => {
            n.as_f64()
                .and_then(|y| field.as_f64().map(|f| (f - y).abs() < f64::EPSILON))
                .unwrap_or(false)
        }

        // Operator map: `{in: [...]}`, `{gt: 10}`, `{gte: 3}`, `{eq: "x"}`
        serde_yaml::Value::Mapping(ops) => {
            ops.iter().all(|(op_key, op_val)| {
                match op_key.as_str().unwrap_or("") {
                    "in" => {
                        if let serde_yaml::Value::Sequence(list) = op_val {
                            let s = field.as_str().unwrap_or("");
                            list.iter().any(|x| x.as_str().unwrap_or("") == s)
                        } else {
                            false
                        }
                    }
                    "gt"  => op_val.as_f64()
                        .and_then(|t| field.as_f64().map(|v| v > t))
                        .unwrap_or(false),
                    "gte" => op_val.as_f64()
                        .and_then(|t| field.as_f64().map(|v| v >= t))
                        .unwrap_or(false),
                    "lt"  => op_val.as_f64()
                        .and_then(|t| field.as_f64().map(|v| v < t))
                        .unwrap_or(false),
                    "lte" => op_val.as_f64()
                        .and_then(|t| field.as_f64().map(|v| v <= t))
                        .unwrap_or(false),
                    "eq"  => match op_val {
                        serde_yaml::Value::String(s) => {
                            field.as_str().map(|v| v == s.as_str()).unwrap_or(false)
                        }
                        serde_yaml::Value::Number(n) => {
                            n.as_f64()
                                .and_then(|y| field.as_f64().map(|f| (f - y).abs() < f64::EPSILON))
                                .unwrap_or(false)
                        }
                        serde_yaml::Value::Bool(b) => {
                            field.as_bool().map(|v| v == *b).unwrap_or(false)
                        }
                        _ => false,
                    },
                    _ => true,  // unknown operator — skip
                }
            })
        }

        _ => true,  // null / sequence at top level — ignore
    }
}

// ─── Field access ─────────────────────────────────────────────────────────────

/// Retrieve a field as serde_json::Value.
/// Checks top-level event fields first, then falls through to payload.
fn get_json_field(event: &EdgeEvent, field: &str) -> Option<serde_json::Value> {
    match field {
        "event_type" => Some(serde_json::Value::String(event.event_type.clone())),
        "feeder_id"  => event.feeder_id.as_ref()
                            .map(|f| serde_json::Value::String(f.clone())),
        "site_id"    => event.site_id.as_ref().map(|s| serde_json::Value::String(s.clone())),
        _            => event.payload.get(field).cloned(),
    }
}

/// Retrieve a payload field as a plain String (for relay_codes_from).
fn get_str_field(event: &EdgeEvent, field: &str) -> Option<String> {
    get_json_field(event, field)
        .and_then(|v| v.as_str().map(|s| s.to_string()))
}

// ─── Template interpolation ───────────────────────────────────────────────────

/// Replace {field_name} placeholders in a message template.
/// Supports: {feeder_id}, {site_id}, {event_type}, {window_count}
/// and any key present in event.payload (e.g. {relay_code}, {load_pct}).
fn interpolate(template: &str, event: &EdgeEvent, window_count: usize) -> String {
    let mut out = template.to_string();

    out = out.replace("{feeder_id}",    &event.feeder());
    out = out.replace("{site_id}",      event.site_id.as_deref().unwrap_or(""));
    out = out.replace("{event_type}",   &event.event_type);
    out = out.replace("{window_count}", &window_count.to_string());

    for (key, value) in &event.payload {
        let placeholder = format!("{{{}}}", key);
        let display = match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    // Remove trailing zeros for clean display
                    if f.fract() == 0.0 { format!("{}", f as i64) }
                    else                { format!("{:.1}", f) }
                } else {
                    n.to_string()
                }
            }
            serde_json::Value::Bool(b) => b.to_string(),
            _ => value.to_string(),
        };
        out = out.replace(&placeholder, &display);
    }

    out
}
