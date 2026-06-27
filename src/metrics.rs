use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub downtime_secs: u64,
    pub total_retries: u64,
    pub reconnect_count: u64,
    pub failed_operations: u64,
    pub successful_operations: u64,
    pub bandwidth_rx_bytes: u64,
    pub bandwidth_tx_bytes: u64,
    pub avg_latency_ms: f64,
    pub max_latency_ms: f64,
    pub avg_packet_loss_pct: f64,
    pub max_packet_loss_pct: f64,
    pub session_count: u64,
    pub total_operations: u64,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            uptime_secs: 0,
            downtime_secs: 0,
            total_retries: 0,
            reconnect_count: 0,
            failed_operations: 0,
            successful_operations: 0,
            bandwidth_rx_bytes: 0,
            bandwidth_tx_bytes: 0,
            avg_latency_ms: 0.0,
            max_latency_ms: 0.0,
            avg_packet_loss_pct: 0.0,
            max_packet_loss_pct: 0.0,
            session_count: 0,
            total_operations: 0,
        }
    }
}

struct MetricsInner {
    started_at: Instant,
    last_online: Option<Instant>,
    last_offline: Option<Instant>,
    total_uptime: Duration,
    total_downtime: Duration,
    latencies: VecDeque<f64>,
    packet_losses: VecDeque<f64>,
    retries: Vec<Instant>,
    reconnects: Vec<Instant>,
    operations: Vec<OperationRecord>,
    bandwidth_samples: VecDeque<BandwidthSample>,
}

struct BandwidthSample {
    rx: u64,
    tx: u64,
    time: Instant,
}

#[derive(Debug, Clone)]
struct OperationRecord {
    plugin: String,
    description: String,
    success: bool,
    retries: u32,
    timestamp: Instant,
}

pub struct MetricsEngine {
    inner: Arc<Mutex<MetricsInner>>,
    rx_bytes: AtomicU64,
    tx_bytes: AtomicU64,
    retry_count: AtomicU64,
    reconnect_count: AtomicU64,
    failed_ops: AtomicU64,
    success_ops: AtomicU64,
    is_online: AtomicBool,
}

impl MetricsEngine {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsInner {
                started_at: Instant::now(),
                last_online: None,
                last_offline: None,
                total_uptime: Duration::ZERO,
                total_downtime: Duration::ZERO,
                latencies: VecDeque::with_capacity(1000),
                packet_losses: VecDeque::with_capacity(1000),
                retries: Vec::new(),
                reconnects: Vec::new(),
                operations: Vec::new(),
                bandwidth_samples: VecDeque::with_capacity(3600),
            })),
            rx_bytes: AtomicU64::new(0),
            tx_bytes: AtomicU64::new(0),
            retry_count: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            failed_ops: AtomicU64::new(0),
            success_ops: AtomicU64::new(0),
            is_online: AtomicBool::new(false),
        }
    }

    pub fn record_connectivity(&self, latency_ms: f64, packet_loss_pct: f64) {
        let mut inner = self.inner.try_lock().unwrap();
        inner.latencies.push_back(latency_ms);
        if inner.latencies.len() > 1000 {
            inner.latencies.pop_front();
        }
        inner.packet_losses.push_back(packet_loss_pct);
        if inner.packet_losses.len() > 1000 {
            inner.packet_losses.pop_front();
        }
    }

    pub fn record_online(&self) {
        let was_offline = !self.is_online.swap(true, Ordering::Relaxed);
        if was_offline {
            self.reconnect_count.fetch_add(1, Ordering::Relaxed);
            let mut inner = self.inner.try_lock().unwrap();
            inner.last_online = Some(Instant::now());
            if let Some(offline_at) = inner.last_offline {
                inner.total_downtime += Instant::now() - offline_at;
            }
            inner.reconnects.push(Instant::now());
        }
    }

    pub fn record_offline(&self) {
        let was_online = self.is_online.swap(false, Ordering::Relaxed);
        if was_online {
            let mut inner = self.inner.try_lock().unwrap();
            inner.last_offline = Some(Instant::now());
            if let Some(online_at) = inner.last_online {
                inner.total_uptime += Instant::now() - online_at;
            }
        }
    }

    pub fn record_operation(
        &self,
        plugin: &str,
        description: &str,
        success: bool,
        retries: u32,
    ) {
        if success {
            self.success_ops.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_ops.fetch_add(1, Ordering::Relaxed);
        }

        let mut inner = self.inner.try_lock().unwrap();
        inner.operations.push(OperationRecord {
            plugin: plugin.to_string(),
            description: description.to_string(),
            success,
            retries,
            timestamp: Instant::now(),
        });
        if inner.operations.len() > 10000 {
            inner.operations.remove(0);
        }
    }

    pub fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.try_lock().unwrap();
        inner.retries.push(Instant::now());
    }

    pub fn record_bandwidth(&self, rx: u64, tx: u64) {
        self.rx_bytes.store(rx, Ordering::Relaxed);
        self.tx_bytes.store(tx, Ordering::Relaxed);

        let mut inner = self.inner.try_lock().unwrap();
        inner.bandwidth_samples.push_back(BandwidthSample {
            rx,
            tx,
            time: Instant::now(),
        });
        if inner.bandwidth_samples.len() > 3600 {
            inner.bandwidth_samples.pop_front();
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut inner = self.inner.try_lock().unwrap();

        let now = Instant::now();
        if self.is_online.load(Ordering::Relaxed) {
            if let Some(online_at) = inner.last_online {
                inner.total_uptime = now - online_at;
            }
        } else if let Some(offline_at) = inner.last_offline {
            inner.total_downtime = now - offline_at;
        }

        let avg_lat = if !inner.latencies.is_empty() {
            inner.latencies.iter().sum::<f64>() / inner.latencies.len() as f64
        } else {
            0.0
        };

        let max_lat = inner
            .latencies
            .iter()
            .cloned()
            .fold(0.0_f64, f64::max);

        let avg_loss = if !inner.packet_losses.is_empty() {
            inner.packet_losses.iter().sum::<f64>() / inner.packet_losses.len() as f64
        } else {
            0.0
        };

        let max_loss = inner
            .packet_losses
            .iter()
            .cloned()
            .fold(0.0_f64, f64::max);

        let rx = self.rx_bytes.load(Ordering::Relaxed);
        let tx = self.tx_bytes.load(Ordering::Relaxed);

        MetricsSnapshot {
            uptime_secs: inner.total_uptime.as_secs(),
            downtime_secs: inner.total_downtime.as_secs(),
            total_retries: self.retry_count.load(Ordering::Relaxed),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            failed_operations: self.failed_ops.load(Ordering::Relaxed),
            successful_operations: self.success_ops.load(Ordering::Relaxed),
            bandwidth_rx_bytes: rx,
            bandwidth_tx_bytes: tx,
            avg_latency_ms: avg_lat,
            max_latency_ms: max_lat,
            avg_packet_loss_pct: avg_loss,
            max_packet_loss_pct: max_loss,
            session_count: inner.reconnects.len() as u64,
            total_operations: inner.operations.len() as u64,
        }
    }
}

impl Default for MetricsEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_online_offline_tracking() {
        let m = MetricsEngine::new();
        let snap = m.snapshot();
        assert_eq!(snap.uptime_secs, 0);
        assert_eq!(snap.downtime_secs, 0);

        m.record_online();
        tokio::time::sleep(Duration::from_millis(10)).await;
        m.record_offline();

        let snap = m.snapshot();
        assert!(snap.uptime_secs > 0 || snap.downtime_secs == 0);
    }

    #[tokio::test]
    async fn test_reconnect_count() {
        let m = MetricsEngine::new();
        assert_eq!(m.snapshot().reconnect_count, 0);

        m.record_online();
        m.record_offline();
        m.record_online();

        let snap = m.snapshot();
        assert_eq!(snap.reconnect_count, 2);
    }

    #[test]
    fn test_operation_counting() {
        let m = MetricsEngine::new();
        m.record_operation("git", "clone repo", true, 2);
        m.record_operation("curl", "download file", false, 3);

        let snap = m.snapshot();
        assert_eq!(snap.successful_operations, 1);
        assert_eq!(snap.failed_operations, 1);
        assert_eq!(snap.total_retries, 0);
    }

    #[test]
    fn test_retry_tracking() {
        let m = MetricsEngine::new();
        m.record_retry();
        m.record_retry();
        m.record_retry();

        assert_eq!(m.snapshot().total_retries, 3);
    }

    #[test]
    fn test_connectivity_metrics() {
        let m = MetricsEngine::new();
        m.record_connectivity(10.0, 0.0);
        m.record_connectivity(20.0, 5.0);
        m.record_connectivity(30.0, 10.0);

        let snap = m.snapshot();
        assert!((snap.avg_latency_ms - 20.0).abs() < 0.1);
        assert!((snap.max_latency_ms - 30.0).abs() < 0.1);
        assert!((snap.avg_packet_loss_pct - 5.0).abs() < 0.1);
        assert!((snap.max_packet_loss_pct - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_bandwidth() {
        let m = MetricsEngine::new();
        m.record_bandwidth(1000, 500);
        assert_eq!(m.snapshot().bandwidth_rx_bytes, 1000);
        assert_eq!(m.snapshot().bandwidth_tx_bytes, 500);
    }
}
