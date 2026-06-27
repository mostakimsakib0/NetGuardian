use std::path::Path;
use tokio::net::UnixListener;

use crate::event_bus::bus::EventBus;
use crate::event_bus::events::NetGuardianEvent;
use crate::metrics::MetricsEngine;
use crate::monitor::{connectivity::check_connectivity, dns::check_dns_health, gateway::check_gateway, interface::list_interfaces, ConnectionStatus, NetworkStatus};
use crate::storage::AppConfig;

use chrono::Utc;

pub struct IpcServer {
    socket_path: String,
}

impl IpcServer {
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
        }
    }

    async fn collect_status() -> anyhow::Result<NetworkStatus> {
        let (latency_ms, packet_loss_pct) = check_connectivity().await?;
        let dns = check_dns_health().await?;
        let gateway = check_gateway().await?;
        let interfaces = list_interfaces().await?;
        let status = if packet_loss_pct >= 80.0 {
            ConnectionStatus::Offline
        } else if packet_loss_pct > 10.0 || latency_ms > 500.0 {
            ConnectionStatus::Degraded
        } else {
            ConnectionStatus::Online
        };
        Ok(NetworkStatus { status, latency_ms, packet_loss_pct, dns, gateway, interfaces, timestamp: Utc::now().to_rfc3339() })
    }

    pub async fn serve(
        &self,
        event_bus: &EventBus,
        metrics: &MetricsEngine,
        config: &AppConfig,
    ) -> anyhow::Result<()> {
        let _ = std::fs::remove_file(&self.socket_path);
        if let Some(parent) = Path::new(&self.socket_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let listener = UnixListener::bind(&self.socket_path)?;
        let mut rx = event_bus.subscribe();
        let bus = event_bus.clone();

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    let metrics = metrics.clone();
                    let config = config.clone();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        loop {
                            line.clear();
                            match reader.read_line(&mut line).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {
                                    let cmd = line.trim();
                                    let response = match cmd {
                                        "status" => {
                                            match Self::collect_status().await {
                                                Ok(s) => serde_json::to_string_pretty(&s).unwrap_or_default(),
                                                Err(e) => format!("{{\"error\": \"{}\"}}", e),
                                            }
                                        }
                                        "metrics" | "metrics/json" => {
                                            let snap = metrics.snapshot();
                                            serde_json::to_string_pretty(&snap).unwrap_or_default()
                                        }
                                        "metrics/prometheus" => {
                                            metrics.format_prometheus()
                                        }
                                        "config" => {
                                            serde_json::to_string_pretty(&config).unwrap_or_default()
                                        }
                                        "ping" => "pong".into(),
                                        cmd if cmd.starts_with("pause_job ") => {
                                            let id: u64 = cmd.trim_start_matches("pause_job ").trim().parse().unwrap_or(0);
                                            bus.publish(NetGuardianEvent::JobControlPause { job_id: id });
                                            "{\"status\":\"pause_requested\"}".into()
                                        }
                                        cmd if cmd.starts_with("resume_job ") => {
                                            let id: u64 = cmd.trim_start_matches("resume_job ").trim().parse().unwrap_or(0);
                                            bus.publish(NetGuardianEvent::JobControlResume { job_id: id });
                                            "{\"status\":\"resume_requested\"}".into()
                                        }
                                        cmd if cmd.starts_with("cancel_job ") => {
                                            let id: u64 = cmd.trim_start_matches("cancel_job ").trim().parse().unwrap_or(0);
                                            bus.publish(NetGuardianEvent::JobControlCancel { job_id: id });
                                            "{\"status\":\"cancel_requested\"}".into()
                                        }
                                        "health" => {
                                            serde_json::json!({
                                                "status": "ok",
                                                "version": env!("CARGO_PKG_VERSION"),
                                                "uptime_secs": metrics.snapshot().uptime_secs,
                                                "healthy": true,
                                            }).to_string()
                                        }
                                        _ => format!("{{\"error\": \"unknown command: {}\"}}", cmd),
                                    };
                                    let _ = reader.get_mut().write_all(format!("{}\n", response).as_bytes()).await;
                                    let _ = reader.get_mut().flush().await;
                                }
                            }
                        }
                    });
                }
                Ok(event) = rx.recv() => {
                    let _ = event;
                }
                else => break,
            }
        }

        Ok(())
    }
}
