use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    pub latency_ms: f64,
    pub packet_loss_pct: f64,
    pub dns_healthy: bool,
    pub jitter_ms: Option<f64>,
    pub bandwidth_kbps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEventPayload {
    pub job_id: u64,
    pub plugin: String,
    pub description: String,
    pub priority: u8,
    pub retry_count: u32,
    pub owner: Option<String>,
    pub error: Option<String>,
    pub progress: f64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryEventPayload {
    pub job_id: u64,
    pub attempt: u32,
    pub delay_ms: u64,
    pub failure_class: String,
    pub error: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginEventPayload {
    pub plugin: String,
    pub version: String,
    pub state: String,
    pub healthy: bool,
    pub error: Option<String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEventPayload {
    pub cache_type: String,
    pub key: String,
    pub size_bytes: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEventPayload {
    pub rule: String,
    pub condition: String,
    pub action: String,
    pub network_status: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerEventPayload {
    pub quality: String,
    pub concurrency: usize,
    pub latency_ms: f64,
    pub loss_pct: f64,
    pub active_jobs: usize,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadEventPayload {
    pub job_id: u64,
    pub url: String,
    pub plugin: String,
    pub progress: f64,
    pub speed_bytes_per_sec: f64,
    pub error: Option<String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorEventPayload {
    pub name: String,
    pub url: String,
    pub latency_ms: f64,
    pub healthy: bool,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetGuardianEvent {
    // ── Network ──
    InternetLost,
    InternetRecovered(NetworkMetrics),
    PacketLossHigh { packet_loss_pct: f64, latency_ms: f64, interface: Option<String> },
    LatencyHigh { latency_ms: f64, packet_loss_pct: f64, interface: Option<String> },
    DNSFailure { domain: String, resolver: Option<String>, error: String },
    InterfaceChanged { name: String, is_up: bool, ip: Option<String> },
    RouteChanged { gateway: Option<String>, interface: Option<String> },
    BandwidthLow { current_kbps: f64, threshold_kbps: f64 },

    // ── Job Lifecycle ──
    JobCreated(JobEventPayload),
    JobStarted(JobEventPayload),
    JobPaused(JobEventPayload),
    JobResumed(JobEventPayload),
    JobCompleted(JobEventPayload),
    JobFailed(JobEventPayload),
    JobCancelled(JobEventPayload),
    JobProgress { job_id: u64, progress: f64, plugin: String },

    // ── Retry ──
    RetryStarted(RetryEventPayload),
    RetrySkipped { job_id: u64, reason: String },
    RetryExhausted { job_id: u64, attempts: u32, last_error: String },
    CircuitBreakerOpened { policy: String, failures: u32, timeout_secs: u64 },
    CircuitBreakerClosed { policy: String },
    CircuitBreakerHalfOpen { policy: String },

    // ── Plugin ──
    PluginLoaded(PluginEventPayload),
    PluginStarted(PluginEventPayload),
    PluginPaused(PluginEventPayload),
    PluginFailed(PluginEventPayload),
    PluginHealthChanged { plugin: String, healthy: bool, message: String },

    // ── Policy ──
    PolicyTriggered(PolicyEventPayload),
    PolicyCooldown { rule: String, remaining_secs: u64 },

    // ── Queue ──
    QueueChanged { total: usize, pending: usize, running: usize, failed: usize },
    QueueDrained,

    // ── Cache ──
    CacheHit(CacheEventPayload),
    CacheMiss(CacheEventPayload),
    CacheEvicted { cache_type: String, key: String, reason: String },

    // ── Scheduler ──
    SchedulerDecision(SchedulerEventPayload),
    QualityChanged { from: String, to: String, reason: String },

    // ── Download ──
    DownloadStarted(DownloadEventPayload),
    DownloadPaused(DownloadEventPayload),
    DownloadResumed(DownloadEventPayload),
    DownloadFailed(DownloadEventPayload),
    DownloadCompleted(DownloadEventPayload),

    // ── Mirror ──
    MirrorChanged(MirrorEventPayload),
    MirrorUnreachable { name: String, url: String, attempts: u32 },

    // ── VPN ──
    VPNConnected { interface: Option<String> },
    VPNDisconnected,

    // ── Control (from IPC / CLI) ──
    JobControlPause { job_id: u64 },
    JobControlResume { job_id: u64 },
    JobControlCancel { job_id: u64 },
    HealthCheckRequest { request_id: String },
    HealthCheckResponse { request_id: String, status: String },
}
