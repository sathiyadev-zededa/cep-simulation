// main.rs — CEP Engine entry point
//
// Wires together:
//   MQTT subscription → edge/events
//   DSL rule loader   → rules::load_rules()  (reads rules.yaml)
//   Rules evaluation  → rules::evaluate()
//   MQTT publish      → edge/incidents
//   Background task   → CommsLossTracker
//
// Environment variables:
//   MQTT_HOST    default: mqtt-broker
//   MQTT_PORT    default: 1883
//   SITE_ID      default: SUBSTATION-ABU-01
//   RULES_FILE   default: rules.yaml          ← path to the DSL config
//   RUST_LOG     default: info  (set to debug for rule traces)

mod comms_loss;
mod config;
mod dedup;
mod rules;
mod types;
mod window;

use dedup::IncidentDedup;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::Arc;
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
    // Panics with a clear error if rules.yaml is missing or malformed.
    // After startup, rules are immutable — hot-reload is not supported yet.
    let rules = Arc::new(rules::load_rules(&rules_file));

    // ── Shared state ──────────────────────────────────────────────────────────
    // Single window large enough for the widest rule (60s freq/harmonics).
    // Rules call count_within(secs) so each uses its own look-back period.
    let window: Arc<WindowBuffer> = Arc::new(WindowBuffer::new(120));
    let dedup:  Arc<IncidentDedup> = Arc::new(IncidentDedup::new(20));

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
    info!("[CEP] Publishes → edge/incidents");

    // ── CommsLoss background task ─────────────────────────────────────────────
    comms_loss::spawn(client.clone(), Arc::clone(&window), site_id.clone());

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
                    &event, &rules, &window, &dedup, &site_id,
                );

                for incident in &incidents {
                    match serde_json::to_string(incident) {
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
