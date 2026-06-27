pub mod connectivity;
pub mod dns;
pub mod gateway;
pub mod interface;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStatus {
    pub status: ConnectionStatus,
    pub latency_ms: f64,
    pub packet_loss_pct: f64,
    pub dns: DnsStatus,
    pub gateway: GatewayStatus,
    pub interfaces: Vec<InterfaceInfo>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConnectionStatus {
    Online,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsStatus {
    pub healthy: bool,
    pub resolution_ms: f64,
    pub nameservers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayStatus {
    pub reachable: bool,
    pub gateway_ip: Option<String>,
    pub interface: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceInfo {
    pub name: String,
    pub ip: Option<String>,
    pub mac: Option<String>,
    pub is_up: bool,
    pub is_loopback: bool,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}
