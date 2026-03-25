// main.rs — CEP Engine entry point
//
// Wires together:
//   MQTT subscription → edge/events
//   DSL rule loader   → rules::load_rules()  (reads rules.yaml)
//   Rules evaluation  → rules::evaluate()
//   MQTT publish      → edge/incidents        (all incidents, dashboard)
//   MQTT publish      → edge/fault_events     (aggregated, LLM service)
//   Background tasks  → CommsLossTracker, FaultEventAggregator flusher
//
// Environment variables:
//   MQTT_HOST    default: mqtt-broker
//   MQTT_PORT    default: 1883
//   SITE_ID      default: SUBSTATION-ABU-01
//   RULES_FILE   default: rules.yaml          ← path to the DSL config
//   RUST_LOG     default: info  (set to debug for rule traces)

mod aggregator;
mod comms_loss;
mod config;
mod dedup;
mod rules;
mod suppression;
mod sustained;
mod types;
mod window;

use aggregator::FaultEventAggregator;
use dedup::IncidentDedup;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::Arc;
use suppression::FaultSuppression;
use sustained::SustainedTracker;
use tokio::time::Duration;
use tracing::{error, info, warn};
use types::EdgeEvent;
use window::WindowBuffer;

#[tokio::main]
async fn main() {
    // ── Logging ──────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string())
        )
        .init();

    // ── Config from environment ───────────────────────────────────────────────
    let mqtt_host  = std::env::var("MQTT_HOST")
        .unwrap_or_else(|_| "mqtt-broker".to_string());
    let mqtt_port: u16 = std::env::var("MQTT_PORT")
        .unwrap_or_else(|_| "1883".to_string())
        .parse().unwrap_or(1883);
    let site_id    = std::env::var("SITE_ID")
        .unwrap_or_else(|_| "SUBSTATION-ABU-01".to_string());
    let rules_file = std::env::var("RULES_FILE")
        .unwrap_or_else(|_| "rules.yaml".to_string());

    info!("╔══════════════════════════════════════════════╗");
    info!("║       CEP Engine  —  Rust Edition  (DSL)     ║");
    info!("╚══════════════════════════════════════════════╝");
    info!("MQTT:       {}:{}", mqtt_host, mqtt_port);
    info!("Site:       {}", site_id);
    info!("Rules file: {}", rules_file);

    // ── Load DSL rules ────────────────────────────────────────────────────────
    let rules = Arc::new(rules::load_rules(&rules_file));

    // ── Shared state ──────────────────────────────────────────────────────────
    let window: Arc<WindowBuffer> = Arc::new(WindowBuffer::new(120));
    let dedup:  Arc<IncidentDedup> = Arc::new(IncidentDedup::new(20));

    // FaultSuppression: when a CRITICAL incident fires for a feeder,
    // suppress secondary incidents (voltage, temp, comms) for 60 s.
    let suppression: Arc<FaultSuppression> = Arc::new(FaultSuppression::new(60));

    // SustainedTracker: tracks how long a condition has been continuously true
    // per feeder — used by Pattern A (sustained condition) rules.
    let sustained: Arc<SustainedTracker> = Arc::new(SustainedTracker::new());

    // FaultEventAggregator: collects HIGH/CRITICAL incidents within a 5 s
    // window per feeder and publishes a single FaultEvent to edge/fault_events.
    let aggregator: Arc<FaultEventAggregator> = Arc::new(FaultEventAggregator::new(5));

    // ── MQTT client ───────────────────────────────────────────────────────────
    let mut mqttopts = MqttOptions::new("cep-engine-rust", &mqtt_host, mqtt_port);
    mqttopts.set_keep_alive(Duration::from_secs(30));
    mqttopts.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(mqttopts, 256);

    client
        .subscribe("edge/events", QoS::AtLeastOnce)
        .await
        .expect("Failed to subscribe to edge/events");

    info!("[CEP] Connected → subscribed to edge/events");
    info!("[CEP] Publishes → edge/incidents  (all incidents, dashboard)");
    info!("[CEP] Publishes → edge/fault_events  (aggregated, LLM service)");

    // ── Background tasks ──────────────────────────────────────────────────────
    // CommsLoss monitor — fires COMMS_LOSS if no telemetry in 15 s
    comms_loss::spawn(
        client.clone(),
        Arc::clone(&window),
        Arc::clone(&suppression),
        site_id.clone(),
    );

    // Aggregator flusher — drains 5 s windows and publishes FaultEvents
    aggregator::spawn_flusher(Arc::clone(&aggregator), client.clone());

    // ── Main event loop ───────────────────────────────────────────────────────
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(msg))) => {
                if msg.topic != "edge/events" {
                    continue;
                }

                let event: EdgeEvent = match serde_json::from_slice(&msg.payload) {
                    Ok(e)  => e,
                    Err(e) => { warn!("[CEP] Failed to parse event: {}", e); continue; }
                };

                let incidents = rules::evaluate(
                    &event, &rules, &window, &dedup, &site_id, &suppression, &sustained,
                );

                for incident in incidents {
                    // Determine aggregation eligibility before any borrows
                    let is_high_or_critical = matches!(
                        incident.payload.severity.as_str(),
                        "HIGH" | "CRITICAL"
                    );

                    // Publish to edge/incidents — dashboard receives all incidents
                    match serde_json::to_string(&incident) {
                        Ok(payload) => {
                            if let Err(e) = client
                                .publish("edge/incidents", QoS::AtLeastOnce, false, payload)
                                .await
                            {
                                error!("[CEP] Failed to publish incident: {}", e);
                            } else {
                                info!(
                                    "→ {} | {} | conf={:.0}% | feeder={}",
                                    incident.payload.incident_type,
                                    incident.payload.severity,
                                    incident.payload.confidence * 100.0,
                                    incident.payload.feeder_id,
                                );
                            }
                        }
                        Err(e) => error!("[CEP] Failed to serialise incident: {}", e),
                    }

                    // Feed HIGH/CRITICAL incidents into the 5 s aggregation window.
                    // The flusher task drains expired windows into a single FaultEvent
                    // published to edge/fault_events for the LLM service.
                    if is_high_or_critical {
                        aggregator.add(incident);
                    }
                }
            }

            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("[CEP] MQTT broker acknowledged connection");
            }

            Ok(Event::Incoming(Packet::PingResp)) => { /* keepalive */ }
            Ok(_) => { /* other MQTT events — ignore */ }

            Err(e) => {
                error!("[CEP] MQTT error: {} — retrying in 3s", e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
