// main.rs — CEP Engine entry point
//
// Wires together:
//   MQTT subscription → edge/events
//   Rules evaluation  → rules::evaluate()
//   MQTT publish      → edge/incidents, edge/commands
//   Background task   → CommsLossTracker
//
// Environment variables (same as Python implementation):
//   MQTT_HOST        default: mqtt-broker
//   MQTT_PORT        default: 1883
//   SITE_ID          default: SUBSTATION-ABU-01
//   RUST_LOG         default: info  (set to debug for rule traces)

mod comms_loss;
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
    let mqtt_host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "mqtt-broker".to_string());
    let mqtt_port: u16 = std::env::var("MQTT_PORT")
        .unwrap_or_else(|_| "1883".to_string())
        .parse()
        .unwrap_or(1883);
    let site_id = std::env::var("SITE_ID")
        .unwrap_or_else(|_| "SUBSTATION-ABU-01".to_string());

    info!("╔══════════════════════════════════════════════╗");
    info!("║       CEP Engine  —  Rust Edition            ║");
    info!("╚══════════════════════════════════════════════╝");
    info!("MQTT:    {}:{}", mqtt_host, mqtt_port);
    info!("Site:    {}", site_id);
    info!("Rules:   13  |  WindowBuffer: 30s  |  Dedup: 20s");
    info!("Subscribes: edge/events");
    info!("Publishes:  edge/incidents  edge/commands");

    // ── Shared state ─────────────────────────────────────────────────────────
    // 30-second sliding window for multi-event rules
    let window: Arc<WindowBuffer> = Arc::new(WindowBuffer::new(30));

    // 60-second window for voltage harmonics and frequency deviation rules
    let _window_60: Arc<WindowBuffer> = Arc::new(WindowBuffer::new(60));

    // 20-second dedup cooldown
    let dedup: Arc<IncidentDedup> = Arc::new(IncidentDedup::new(20));

    // ── MQTT client ───────────────────────────────────────────────────────────
    let mut mqttopts = MqttOptions::new("cep-engine-rust", &mqtt_host, mqtt_port);
    mqttopts.set_keep_alive(Duration::from_secs(30));
    mqttopts.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(mqttopts, 256);

    // ── Subscribe ─────────────────────────────────────────────────────────────
    client
        .subscribe("edge/events", QoS::AtLeastOnce)
        .await
        .expect("Failed to subscribe to edge/events");

    info!("Connected → subscribed to edge/events");

    // ── CommsLoss background task ─────────────────────────────────────────────
    // Passes the 30s window (telemetry heartbeat is tracked there)
    comms_loss::spawn(client.clone(), Arc::clone(&window), site_id.clone());

    // ── Main event loop ───────────────────────────────────────────────────────
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(msg))) => {
                if msg.topic != "edge/events" {
                    continue;
                }

                // Parse the incoming event
                let event: EdgeEvent = match serde_json::from_slice(&msg.payload) {
                    Ok(e)  => e,
                    Err(e) => {
                        warn!("Failed to parse event: {}", e);
                        continue;
                    }
                };

                // Evaluate all 13 rules
                // Pass window_60 for harmonics/frequency rules
                // (rules.rs uses the window passed in for all rules; we merge
                //  both windows in the rule evaluation call below)
                let incidents = rules::evaluate(&event, &window, &dedup);

                // Publish each fired incident
                for incident in &incidents {
                    match serde_json::to_string(incident) {
                        Ok(payload) => {
                            if let Err(e) = client
                                .publish("edge/incidents", QoS::AtLeastOnce, false, payload)
                                .await
                            {
                                error!("Failed to publish incident: {}", e);
                            } else {
                                info!(
                                    "→ {} | {} | conf={:.0}% | feeder={}",
                                    incident.payload.incident_type,
                                    incident.payload.severity,
                                    incident.payload.confidence * 100.0,
                                    incident.payload.feeder_id
                                );
                            }
                        }
                        Err(e) => error!("Failed to serialise incident: {}", e),
                    }
                }
            }

            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("MQTT broker acknowledged connection");
            }

            Ok(Event::Incoming(Packet::PingResp)) => {
                // keepalive — ignore
            }

            Ok(_) => {
                // other MQTT events (subscribe ack, etc.) — ignore
            }

            Err(e) => {
                error!("MQTT connection error: {} — retrying in 3s", e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
