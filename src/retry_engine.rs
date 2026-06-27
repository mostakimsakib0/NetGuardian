use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum RetryPolicy {
    Immediate,
    Fixed { delay: Duration },
    ExponentialBackoff {
        initial: Duration,
        multiplier: f64,
        max_delay: Duration,
    },
    Adaptive {
        initial: Duration,
        multiplier: f64,
        max_delay: Duration,
        latency_weight: f64,
        loss_weight: f64,
    },
    Smart {
        initial: Duration,
        multiplier: f64,
        max_delay: Duration,
        circuit_breaker_threshold: u32,
        circuit_breaker_cooldown: Duration,
    },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::ExponentialBackoff {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FailureClass {
    Transient,
    Permanent,
    Timeout,
    DnsFailure,
    NetworkUnreachable,
    RateLimited,
    Unknown,
}

impl FailureClass {
    pub fn classify(error_msg: &str) -> Self {
        let lower = error_msg.to_lowercase();
        if lower.contains("timeout") || lower.contains("timed out") {
            FailureClass::Timeout
        } else if lower.contains("dns") || lower.contains("name resolution") {
            FailureClass::DnsFailure
        } else if lower.contains("unreachable") || lower.contains("network is") {
            FailureClass::NetworkUnreachable
        } else if lower.contains("429") || lower.contains("rate limit") || lower.contains("too many") {
            FailureClass::RateLimited
        } else if lower.contains("404") || lower.contains("not found")
            || lower.contains("403") || lower.contains("forbidden")
            || lower.contains("401") || lower.contains("unauthorized")
        {
            FailureClass::Permanent
        } else {
            FailureClass::Transient
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, FailureClass::Transient | FailureClass::Timeout
            | FailureClass::DnsFailure | FailureClass::NetworkUnreachable
            | FailureClass::RateLimited | FailureClass::Unknown)
    }
}

#[derive(Debug, Clone)]
pub enum CircuitState {
    Closed,
    Open(Instant),
    HalfOpen,
}

#[derive(Debug, Clone)]
pub struct RetryState {
    pub attempt: u32,
    pub max_retries: u32,
    pub policy: RetryPolicy,
    pub last_latency_ms: f64,
    pub last_packet_loss_pct: f64,
    consecutive_failures: u32,
    circuit_state: CircuitState,
    failure_history: VecDeque<FailureRecord>,
    last_failure_class: Option<FailureClass>,
}

#[derive(Debug, Clone)]
struct FailureRecord {
    class: FailureClass,
    latency_ms: f64,
    loss_pct: f64,
    timestamp: Instant,
}

impl RetryState {
    pub fn new(max_retries: u32, policy: RetryPolicy) -> Self {
        Self {
            attempt: 0,
            max_retries,
            policy,
            last_latency_ms: 0.0,
            last_packet_loss_pct: 0.0,
            consecutive_failures: 0,
            circuit_state: CircuitState::Closed,
            failure_history: VecDeque::with_capacity(100),
            last_failure_class: None,
        }
    }

    pub fn next_delay(&mut self) -> Option<Duration> {
        if self.attempt >= self.max_retries {
            return None;
        }

        if !self.circuit_allows_retry() {
            return None;
        }

        self.attempt += 1;
        Some(self.calculate_delay())
    }

    pub fn record_failure(&mut self, error_msg: &str) {
        let class = FailureClass::classify(error_msg);
        self.last_failure_class = Some(class.clone());

        if class.is_retryable() {
            self.consecutive_failures += 1;
        }

        self.failure_history.push_back(FailureRecord {
            class,
            latency_ms: self.last_latency_ms,
            loss_pct: self.last_packet_loss_pct,
            timestamp: Instant::now(),
        });
        if self.failure_history.len() > 100 {
            self.failure_history.pop_front();
        }

        self.update_circuit_state();
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.circuit_state = CircuitState::Closed;
        self.last_failure_class = None;
    }

    pub fn circuit_state(&self) -> &CircuitState {
        &self.circuit_state
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    pub fn last_failure_class(&self) -> Option<&FailureClass> {
        self.last_failure_class.as_ref()
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn remaining(&self) -> u32 {
        self.max_retries.saturating_sub(self.attempt)
    }

    fn circuit_allows_retry(&self) -> bool {
        match &self.circuit_state {
            CircuitState::Closed => true,
            CircuitState::Open(opened_at) => {
                let cooldown = match &self.policy {
                    RetryPolicy::Smart { circuit_breaker_cooldown, .. } => *circuit_breaker_cooldown,
                    _ => Duration::from_secs(30),
                };
                opened_at.elapsed() >= cooldown
            }
            CircuitState::HalfOpen => true,
        }
    }

    fn update_circuit_state(&mut self) {
        let threshold = match &self.policy {
            RetryPolicy::Smart { circuit_breaker_threshold, .. } => *circuit_breaker_threshold,
            _ => 5,
        };

        match &self.circuit_state {
            CircuitState::Closed => {
                if self.consecutive_failures >= threshold {
                    self.circuit_state = CircuitState::Open(Instant::now());
                }
            }
            CircuitState::Open(opened_at) => {
                let cooldown = match &self.policy {
                    RetryPolicy::Smart { circuit_breaker_cooldown, .. } => *circuit_breaker_cooldown,
                    _ => Duration::from_secs(30),
                };
                if opened_at.elapsed() >= cooldown {
                    self.circuit_state = CircuitState::HalfOpen;
                }
            }
            CircuitState::HalfOpen => {
                if self.consecutive_failures >= 1 {
                    self.circuit_state = CircuitState::Open(Instant::now());
                }
            }
        }
    }

    fn calculate_delay(&self) -> Duration {
        let base = match &self.policy {
            RetryPolicy::Immediate => return Duration::ZERO,
            RetryPolicy::Fixed { delay } => return *delay,
            RetryPolicy::ExponentialBackoff { initial, multiplier, max_delay } => {
                initial.as_secs_f64() * multiplier.powf(self.attempt as f64 - 1.0)
            }
            RetryPolicy::Adaptive { initial, multiplier, max_delay, latency_weight, loss_weight } => {
                let b = initial.as_secs_f64() * multiplier.powf(self.attempt as f64 - 1.0);
                let lf = (self.last_latency_ms / 100.0).min(5.0) * latency_weight;
                let lsf = (self.last_packet_loss_pct / 20.0).min(5.0) * loss_weight;
                b * (1.0 + lf + lsf)
            }
            RetryPolicy::Smart { initial, multiplier, max_delay, .. } => {
                let b = initial.as_secs_f64() * multiplier.powf(self.attempt as f64 - 1.0);
                let circuit_penalty = match &self.circuit_state {
                    CircuitState::HalfOpen => 2.0,
                    _ => 1.0,
                };
                let failure_penalty = (self.consecutive_failures as f64).min(10.0) * 0.5;
                let network_penalty = (self.last_packet_loss_pct / 10.0).min(3.0);
                b * circuit_penalty * (1.0 + failure_penalty + network_penalty)
            }
        };

        let max = match &self.policy {
            RetryPolicy::ExponentialBackoff { max_delay, .. } => max_delay.as_secs_f64(),
            RetryPolicy::Adaptive { max_delay, .. } => max_delay.as_secs_f64(),
            RetryPolicy::Smart { max_delay, .. } => max_delay.as_secs_f64(),
            _ => 60.0,
        };

        Duration::from_secs_f64(base.min(max) + jitter())
    }
}

fn jitter() -> f64 {
    fastrand::f64() * 0.5
}

#[derive(Debug, Clone)]
pub struct RetryEngine {
    pub state: RetryState,
    pub policy: RetryPolicy,
}

impl RetryEngine {
    pub fn new(max_retries: u32, policy: RetryPolicy) -> Self {
        Self {
            state: RetryState::new(max_retries, policy.clone()),
            policy,
        }
    }

    pub fn with_network_conditions(
        &mut self,
        latency_ms: f64,
        packet_loss_pct: f64,
    ) {
        self.state.last_latency_ms = latency_ms;
        self.state.last_packet_loss_pct = packet_loss_pct;
    }

    pub fn should_retry(&self) -> bool {
        self.state.attempt < self.state.max_retries && self.state.circuit_allows_retry()
    }

    pub fn next_retry(&mut self) -> Option<Duration> {
        self.state.next_delay()
    }

    pub fn record_failure(&mut self, error_msg: &str) {
        self.state.record_failure(error_msg);
    }

    pub fn record_success(&mut self) {
        self.state.record_success();
    }

    pub fn reset(&mut self) {
        self.state.reset();
    }

    pub fn circuit_state(&self) -> &CircuitState {
        self.state.circuit_state()
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.state.consecutive_failures()
    }

    pub fn failure_class(&self) -> Option<&FailureClass> {
        self.state.last_failure_class()
    }
}

impl Default for RetryEngine {
    fn default() -> Self {
        Self::new(5, RetryPolicy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failure_classification() {
        assert_eq!(FailureClass::classify("Connection timed out"), FailureClass::Timeout);
        assert_eq!(FailureClass::classify("DNS resolution failed"), FailureClass::DnsFailure);
        assert_eq!(FailureClass::classify("Network is unreachable"), FailureClass::NetworkUnreachable);
        assert_eq!(FailureClass::classify("HTTP 429 Too Many Requests"), FailureClass::RateLimited);
        assert_eq!(FailureClass::classify("404 Not Found"), FailureClass::Permanent);
        assert_eq!(FailureClass::classify("Connection reset by peer"), FailureClass::Transient);
    }

    #[test]
    fn test_retryable_classification() {
        assert!(FailureClass::Transient.is_retryable());
        assert!(FailureClass::Timeout.is_retryable());
        assert!(FailureClass::DnsFailure.is_retryable());
        assert!(!FailureClass::Permanent.is_retryable());
    }

    #[test]
    fn test_circuit_breaker_opens() {
        let mut engine = RetryEngine::new(10, RetryPolicy::Smart {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            circuit_breaker_threshold: 3,
            circuit_breaker_cooldown: Duration::from_secs(60),
        });

        assert!(matches!(engine.circuit_state(), CircuitState::Closed));

        engine.record_failure("timeout");
        engine.record_failure("timeout");
        engine.record_failure("timeout");

        assert!(matches!(engine.circuit_state(), CircuitState::Open(_)));
    }

    #[test]
    fn test_success_resets_circuit() {
        let mut engine = RetryEngine::new(10, RetryPolicy::Smart {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            circuit_breaker_threshold: 2,
            circuit_breaker_cooldown: Duration::from_secs(60),
        });

        engine.record_failure("timeout");
        engine.record_failure("timeout");
        assert!(matches!(engine.circuit_state(), CircuitState::Open(_)));

        engine.record_success();
        assert!(matches!(engine.circuit_state(), CircuitState::Closed));
    }

    #[test]
    fn test_permanent_failure_does_not_increment_consecutive() {
        let mut engine = RetryEngine::new(10, RetryPolicy::Smart {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            circuit_breaker_threshold: 3,
            circuit_breaker_cooldown: Duration::from_secs(60),
        });

        engine.record_failure("404 Not Found");
        assert_eq!(engine.consecutive_failures(), 0);
    }

    #[test]
    fn test_smart_retry_computes_delay() {
        let mut engine = RetryEngine::new(5, RetryPolicy::Smart {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            circuit_breaker_threshold: 5,
            circuit_breaker_cooldown: Duration::from_secs(30),
        });

        let delay = engine.next_retry();
        assert!(delay.is_some());
        assert!(delay.unwrap().as_secs_f64() >= 0.0);
    }

    #[test]
    fn test_should_retry_with_open_circuit() {
        let mut engine = RetryEngine::new(10, RetryPolicy::Smart {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            circuit_breaker_threshold: 2,
            circuit_breaker_cooldown: Duration::from_secs(3600),
        });

        engine.record_failure("timeout");
        engine.record_failure("timeout");
        assert!(!engine.should_retry());
    }

    #[test]
    fn test_consecutive_failures_tracking() {
        let mut engine = RetryEngine::new(10, RetryPolicy::Smart {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            circuit_breaker_threshold: 5,
            circuit_breaker_cooldown: Duration::from_secs(30),
        });

        assert_eq!(engine.consecutive_failures(), 0);
        engine.record_failure("timeout");
        assert_eq!(engine.consecutive_failures(), 1);
        engine.record_failure("dns error");
        assert_eq!(engine.consecutive_failures(), 2);
    }

    #[test]
    fn test_immediate_retry() {
        let mut engine = RetryEngine::new(3, RetryPolicy::Immediate);
        assert_eq!(engine.next_retry(), Some(Duration::ZERO));
        assert_eq!(engine.next_retry(), Some(Duration::ZERO));
        assert_eq!(engine.next_retry(), Some(Duration::ZERO));
        assert_eq!(engine.next_retry(), None);
    }

    #[test]
    fn test_max_retries() {
        let mut engine = RetryEngine::new(5, RetryPolicy::Immediate);
        for _ in 0..5 {
            assert!(engine.next_retry().is_some());
        }
        assert!(engine.next_retry().is_none());
    }

    #[test]
    fn test_exponential_backoff_increases() {
        let mut engine = RetryEngine::new(5, RetryPolicy::ExponentialBackoff {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
        });

        let d1 = engine.next_retry().unwrap();
        let d2 = engine.next_retry().unwrap();
        assert!(d2 > d1 || (d2 - d1).as_secs_f64().abs() < 1.0);
    }

    #[test]
    fn test_reset() {
        let mut engine = RetryEngine::new(3, RetryPolicy::Immediate);
        engine.next_retry();
        engine.next_retry();
        assert_eq!(engine.state.attempt, 2);
        engine.reset();
        assert_eq!(engine.state.attempt, 0);
    }

    #[test]
    fn test_should_retry() {
        let mut engine = RetryEngine::new(2, RetryPolicy::Immediate);
        assert!(engine.should_retry());
        engine.next_retry();
        assert!(engine.should_retry());
        engine.next_retry();
        assert!(!engine.should_retry());
    }

    #[test]
    fn test_fixed_delay() {
        let mut engine = RetryEngine::new(2, RetryPolicy::Fixed {
            delay: Duration::from_secs(5),
        });
        let d1 = engine.next_retry().unwrap();
        let d2 = engine.next_retry().unwrap();
        assert!((d1.as_secs_f64() - 5.0).abs() < 1.0);
        assert!((d2.as_secs_f64() - 5.0).abs() < 1.0);
    }

    #[test]
    fn test_failure_class_set_on_record() {
        let mut engine = RetryEngine::new(5, RetryPolicy::default());
        assert!(engine.failure_class().is_none());
        engine.record_failure("Connection timed out");
        assert_eq!(engine.failure_class(), Some(&FailureClass::Timeout));
    }
}
