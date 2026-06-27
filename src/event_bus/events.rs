use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetGuardianEvent {
    InternetLost,
    InternetRecovered(NetworkMetrics),
    DownloadStarted { url: String },
    DownloadPaused { url: String },
    DownloadResumed { url: String },
    DownloadFailed { url: String, reason: String },
    DNSFailure { domain: String },
    PacketLossHigh { packet_loss_pct: f64 },
    LatencyHigh { latency_ms: f64 },
    VPNConnected,
    VPNDisconnected,
    InterfaceChanged { name: String, is_up: bool },
    RouteChanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    pub latency_ms: f64,
    pub packet_loss_pct: f64,
    pub dns_healthy: bool,
}
