use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::monitor::ConnectionStatus;

#[derive(Debug, Clone)]
pub struct ScheduleWindow {
    pub start_hour: u8,
    pub end_hour: u8,
    pub max_concurrency: usize,
    pub priority_boost: u8,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScheduleConfig {
    pub max_concurrency: usize,
    pub min_concurrency: usize,
    pub concurrency_per_good_network: usize,
    pub concurrency_per_degraded_network: usize,
    pub concurrency_per_bad_network: usize,
    pub batch_size: usize,
    pub cooldown_after_failure_secs: u64,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 8,
            min_concurrency: 1,
            concurrency_per_good_network: 5,
            concurrency_per_degraded_network: 2,
            concurrency_per_bad_network: 1,
            batch_size: 3,
            cooldown_after_failure_secs: 10,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScheduleState {
    pub current_concurrency: usize,
    pub active_count: usize,
    pub queue_depth: usize,
    pub suggested_concurrency: usize,
    pub network_quality: NetworkQuality,
    pub recent_failures: u32,
    pub cooldown_remaining_secs: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum NetworkQuality {
    Good,
    Degraded,
    Bad,
}

pub struct AdaptiveScheduler {
    config: ScheduleConfig,
    state: Arc<Mutex<ScheduleInner>>,
}

struct ScheduleInner {
    active_count: usize,
    queue_depth: usize,
    recent_failures: u32,
    last_failure_time: Option<std::time::Instant>,
    windows: Vec<ScheduleWindow>,
}

impl AdaptiveScheduler {
    pub fn new(config: ScheduleConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(ScheduleInner {
                active_count: 0,
                queue_depth: 0,
                recent_failures: 0,
                last_failure_time: None,
                windows: Vec::new(),
            })),
        }
    }

    pub async fn suggest_concurrency(&self, status: &ConnectionStatus, latency_ms: f64, loss_pct: f64) -> usize {
        let mut inner = self.state.lock().await;
        let quality = classify_quality(status, latency_ms, loss_pct);

        let base = match quality {
            NetworkQuality::Good => self.config.concurrency_per_good_network,
            NetworkQuality::Degraded => self.config.concurrency_per_degraded_network,
            NetworkQuality::Bad => self.config.concurrency_per_bad_network,
        };

        let failure_penalty = if inner.recent_failures > 0 {
            (inner.recent_failures as f64 * 0.5).min(2.0) as usize
        } else {
            0
        };

        let in_cooldown = inner.last_failure_time.map_or(false, |t| {
            t.elapsed() < Duration::from_secs(self.config.cooldown_after_failure_secs)
        });

        let suggested = if in_cooldown {
            self.config.min_concurrency
        } else {
            base.saturating_sub(failure_penalty).max(self.config.min_concurrency)
        };

        suggested.min(self.config.max_concurrency)
    }

    pub async fn record_active(&self, delta: i32) {
        let mut inner = self.state.lock().await;
        if delta > 0 {
            inner.active_count = inner.active_count.saturating_add(delta as usize);
        } else {
            inner.active_count = inner.active_count.saturating_sub((-delta) as usize);
        }
    }

    pub async fn record_queue_depth(&self, depth: usize) {
        let mut inner = self.state.lock().await;
        inner.queue_depth = depth;
    }

    pub async fn record_failure(&self) {
        let mut inner = self.state.lock().await;
        inner.recent_failures += 1;
        inner.last_failure_time = Some(std::time::Instant::now());
    }

    pub async fn record_success(&self) {
        let mut inner = self.state.lock().await;
        inner.recent_failures = 0;
    }

    pub async fn add_window(&self, window: ScheduleWindow) {
        let mut inner = self.state.lock().await;
        inner.windows.push(window);
    }

    pub async fn state(&self) -> ScheduleState {
        let inner = self.state.lock().await;
        let quality = NetworkQuality::Good;

        ScheduleState {
            current_concurrency: self.config.max_concurrency,
            active_count: inner.active_count,
            queue_depth: inner.queue_depth,
            suggested_concurrency: 0,
            network_quality: quality,
            recent_failures: inner.recent_failures,
            cooldown_remaining_secs: inner
                .last_failure_time
                .map(|t| {
                    let elapsed = t.elapsed().as_secs();
                    self.config.cooldown_after_failure_secs.saturating_sub(elapsed)
                })
                .unwrap_or(0),
        }
    }

    pub fn config(&self) -> &ScheduleConfig {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut ScheduleConfig {
        &mut self.config
    }
}

impl Default for AdaptiveScheduler {
    fn default() -> Self {
        Self::new(ScheduleConfig::default())
    }
}

fn classify_quality(status: &ConnectionStatus, latency_ms: f64, loss_pct: f64) -> NetworkQuality {
    if *status == ConnectionStatus::Offline || loss_pct >= 50.0 {
        NetworkQuality::Bad
    } else if *status == ConnectionStatus::Degraded || latency_ms > 200.0 || loss_pct > 10.0 {
        NetworkQuality::Degraded
    } else {
        NetworkQuality::Good
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::ConnectionStatus;

    #[tokio::test]
    async fn test_quality_classification() {
        assert_eq!(classify_quality(&ConnectionStatus::Online, 10.0, 0.0), NetworkQuality::Good);
        assert_eq!(classify_quality(&ConnectionStatus::Degraded, 50.0, 5.0), NetworkQuality::Degraded);
        assert_eq!(classify_quality(&ConnectionStatus::Online, 300.0, 0.0), NetworkQuality::Degraded);
        assert_eq!(classify_quality(&ConnectionStatus::Online, 10.0, 30.0), NetworkQuality::Degraded);
        assert_eq!(classify_quality(&ConnectionStatus::Offline, 0.0, 100.0), NetworkQuality::Bad);
        assert_eq!(classify_quality(&ConnectionStatus::Online, 10.0, 60.0), NetworkQuality::Bad);
    }

    #[tokio::test]
    async fn test_suggest_concurrency_good() {
        let scheduler = AdaptiveScheduler::default();
        let c = scheduler.suggest_concurrency(&ConnectionStatus::Online, 10.0, 0.0).await;
        assert_eq!(c, 5);
    }

    #[tokio::test]
    async fn test_suggest_concurrency_degraded() {
        let scheduler = AdaptiveScheduler::default();
        let c = scheduler.suggest_concurrency(&ConnectionStatus::Degraded, 300.0, 5.0).await;
        assert_eq!(c, 2);
    }

    #[tokio::test]
    async fn test_suggest_concurrency_bad() {
        let scheduler = AdaptiveScheduler::default();
        let c = scheduler.suggest_concurrency(&ConnectionStatus::Offline, 0.0, 100.0).await;
        assert_eq!(c, 1);
    }

    #[tokio::test]
    async fn test_failure_penalty() {
        let scheduler = AdaptiveScheduler::new(ScheduleConfig {
            cooldown_after_failure_secs: 0,
            ..Default::default()
        });
        scheduler.record_failure().await;
        scheduler.record_failure().await;

        let c = scheduler.suggest_concurrency(&ConnectionStatus::Online, 10.0, 0.0).await;
        // 5 - 1 (2 failures * 0.5) = 4
        assert_eq!(c, 4);
    }

    #[tokio::test]
    async fn test_success_resets_penalty() {
        let scheduler = AdaptiveScheduler::new(ScheduleConfig {
            cooldown_after_failure_secs: 0,
            ..Default::default()
        });
        scheduler.record_failure().await;
        scheduler.record_failure().await;
        scheduler.record_success().await;

        let c = scheduler.suggest_concurrency(&ConnectionStatus::Online, 10.0, 0.0).await;
        assert_eq!(c, 5);
    }

    #[tokio::test]
    async fn test_min_concurrency() {
        let scheduler = AdaptiveScheduler::new(ScheduleConfig {
            min_concurrency: 1,
            max_concurrency: 8,
            concurrency_per_good_network: 1,
            concurrency_per_degraded_network: 1,
            concurrency_per_bad_network: 1,
            batch_size: 3,
            cooldown_after_failure_secs: 10,
        });

        let c = scheduler.suggest_concurrency(&ConnectionStatus::Online, 10.0, 0.0).await;
        assert_eq!(c, 1);
    }

    #[tokio::test]
    async fn test_record_active() {
        let scheduler = AdaptiveScheduler::default();
        scheduler.record_active(3).await;
        let state = scheduler.state().await;
        assert_eq!(state.active_count, 3);

        scheduler.record_active(-1).await;
        let state = scheduler.state().await;
        assert_eq!(state.active_count, 2);
    }

    #[tokio::test]
    async fn test_schedule_state() {
        let scheduler = AdaptiveScheduler::default();
        scheduler.record_queue_depth(10).await;
        scheduler.record_active(2).await;

        let state = scheduler.state().await;
        assert_eq!(state.queue_depth, 10);
        assert_eq!(state.active_count, 2);
    }

    #[tokio::test]
    async fn test_cooldown() {
        let scheduler = AdaptiveScheduler::default();
        scheduler.record_failure().await;

        let state = scheduler.state().await;
        assert_eq!(state.recent_failures, 1);
        assert!(state.cooldown_remaining_secs > 0);
    }
}
