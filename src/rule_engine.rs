use std::sync::Arc;
use tokio::sync::Mutex;

use crate::monitor::{ConnectionStatus, NetworkStatus};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Condition {
    StatusIs(ConnectionStatus),
    LatencyAbove(f64),
    LatencyBelow(f64),
    PacketLossAbove(f64),
    PacketLossBelow(f64),
    DnsHealthy(bool),
    GatewayReachable(bool),
    InterfaceUp(String),
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Action {
    LogWarning(String),
    PauseDownloads,
    ResumeDownloads,
    ReduceConcurrency(u32),
    IncreaseConcurrency(u32),
    Notify(String),
    ReconnectVPN,
    RunCommand(String),
    SetRetryPolicy(RetryPolicyConfig),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryPolicyConfig {
    pub max_retries: u32,
    pub base_delay_secs: u64,
    pub use_exponential_backoff: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Rule {
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub condition: Condition,
    pub actions: Vec<Action>,
    pub cooldown_secs: u64,
}

impl Rule {
    pub fn new(name: &str, description: &str, condition: Condition, actions: Vec<Action>) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            enabled: true,
            condition,
            actions,
            cooldown_secs: 30,
        }
    }

    pub fn with_cooldown(mut self, secs: u64) -> Self {
        self.cooldown_secs = secs;
        self
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuleEvaluation {
    pub rule_name: String,
    pub triggered: bool,
    pub matched_at: String,
    pub actions_taken: Vec<String>,
}

impl Condition {
    pub fn evaluate(&self, status: &NetworkStatus) -> bool {
        match self {
            Condition::StatusIs(expected) => status.status == *expected,
            Condition::LatencyAbove(threshold) => status.latency_ms > *threshold,
            Condition::LatencyBelow(threshold) => status.latency_ms < *threshold,
            Condition::PacketLossAbove(threshold) => status.packet_loss_pct > *threshold,
            Condition::PacketLossBelow(threshold) => status.packet_loss_pct < *threshold,
            Condition::DnsHealthy(expected) => status.dns.healthy == *expected,
            Condition::GatewayReachable(expected) => status.gateway.reachable == *expected,
            Condition::InterfaceUp(name) => {
                status.interfaces.iter().any(|i| i.name == *name && i.is_up)
            }
            Condition::And(conditions) => conditions.iter().all(|c| c.evaluate(status)),
            Condition::Or(conditions) => conditions.iter().any(|c| c.evaluate(status)),
            Condition::Not(condition) => !condition.evaluate(status),
        }
    }
}

pub struct RuleEngine {
    rules: Vec<Rule>,
    last_triggered: Arc<Mutex<Vec<(String, std::time::Instant)>>>,
}

impl RuleEngine {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            last_triggered: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    pub fn remove_rule(&mut self, name: &str) -> bool {
        let len_before = self.rules.len();
        self.rules.retain(|r| r.name != name);
        self.rules.len() < len_before
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    pub async fn evaluate(&self, status: &NetworkStatus) -> Vec<RuleEvaluation> {
        let mut evaluations = Vec::new();
        let mut last_triggered = self.last_triggered.lock().await;
        let now = std::time::Instant::now();

        for rule in &self.rules {
            if !rule.enabled {
                continue;
            }

            let cooldown_remaining = last_triggered
                .iter()
                .find(|(name, _)| name == &rule.name)
                .map(|(_, time)| now.duration_since(*time).as_secs())
                .unwrap_or(u64::MAX);

            if cooldown_remaining < rule.cooldown_secs {
                continue;
            }

            let triggered = rule.condition.evaluate(status);

            if triggered {
                last_triggered.push((rule.name.clone(), now));
                if last_triggered.len() > 1000 {
                    last_triggered.remove(0);
                }

                let mut actions_taken = Vec::new();
                for action in &rule.actions {
                    actions_taken.push(format!("{:?}", action));
                }

                evaluations.push(RuleEvaluation {
                    rule_name: rule.name.clone(),
                    triggered: true,
                    matched_at: chrono::Utc::now().to_rfc3339(),
                    actions_taken,
                });
            }
        }

        evaluations
    }

    pub fn load_defaults(&mut self) {
        self.add_rule(Rule::new(
            "high-latency",
            "Reduce concurrency when latency exceeds 500ms",
            Condition::LatencyAbove(500.0),
            vec![
                Action::LogWarning("High latency detected, reducing concurrency".into()),
                Action::ReduceConcurrency(2),
            ],
        ));

        self.add_rule(Rule::new(
            "high-packet-loss",
            "Pause downloads when packet loss exceeds 30%",
            Condition::PacketLossAbove(30.0),
            vec![
                Action::LogWarning("High packet loss, pausing downloads".into()),
                Action::PauseDownloads,
            ],
        ));

        self.add_rule(Rule::new(
            "offline-queue",
            "Queue operations when offline",
            Condition::StatusIs(ConnectionStatus::Offline),
            vec![
                Action::LogWarning("Network offline, queuing operations".into()),
                Action::PauseDownloads,
            ],
        ));

        self.add_rule(Rule::new(
            "recovered",
            "Resume downloads when back online with low loss",
            Condition::And(vec![
                Condition::StatusIs(ConnectionStatus::Online),
                Condition::PacketLossBelow(10.0),
            ]),
            vec![
                Action::LogWarning("Network recovered, resuming operations".into()),
                Action::ResumeDownloads,
            ],
        ));

        self.add_rule(Rule::new(
            "dns-failure",
            "Alert when DNS is unhealthy",
            Condition::DnsHealthy(false),
            vec![
                Action::Notify("DNS resolution failed".into()),
            ],
        ));

        self.add_rule(Rule::new(
            "degraded-notify",
            "Notify when network is degraded",
            Condition::StatusIs(ConnectionStatus::Degraded),
            vec![
                Action::Notify("Network quality is degraded".into()),
            ],
        ));
    }
}

impl Default for RuleEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::{ConnectionStatus, DnsStatus, GatewayStatus, InterfaceInfo};
    use chrono::Utc;

    fn make_status(
        status: ConnectionStatus,
        latency: f64,
        loss: f64,
        dns_healthy: bool,
    ) -> NetworkStatus {
        NetworkStatus {
            status,
            latency_ms: latency,
            packet_loss_pct: loss,
            dns: DnsStatus {
                healthy: dns_healthy,
                resolution_ms: 10.0,
                nameservers: vec!["1.1.1.1".into()],
            },
            gateway: GatewayStatus {
                reachable: true,
                gateway_ip: Some("192.168.1.1".into()),
                interface: Some("eth0".into()),
            },
            interfaces: vec![InterfaceInfo {
                name: "eth0".into(),
                ip: Some("192.168.1.100".into()),
                mac: Some("aa:bb:cc:dd:ee:ff".into()),
                is_up: true,
                is_loopback: false,
                rx_bytes: 1000,
                tx_bytes: 500,
            }],
            timestamp: Utc::now().to_rfc3339(),
        }
    }

    #[test]
    fn test_condition_latency_above() {
        let c = Condition::LatencyAbove(100.0);
        let s = make_status(ConnectionStatus::Online, 200.0, 0.0, true);
        assert!(c.evaluate(&s));

        let s2 = make_status(ConnectionStatus::Online, 50.0, 0.0, true);
        assert!(!c.evaluate(&s2));
    }

    #[test]
    fn test_condition_packet_loss_above() {
        let c = Condition::PacketLossAbove(20.0);
        let s = make_status(ConnectionStatus::Degraded, 50.0, 30.0, true);
        assert!(c.evaluate(&s));

        let s2 = make_status(ConnectionStatus::Online, 50.0, 5.0, true);
        assert!(!c.evaluate(&s2));
    }

    #[test]
    fn test_condition_status_is() {
        let c = Condition::StatusIs(ConnectionStatus::Offline);
        let s = make_status(ConnectionStatus::Offline, 0.0, 100.0, false);
        assert!(c.evaluate(&s));

        let s2 = make_status(ConnectionStatus::Online, 10.0, 0.0, true);
        assert!(!c.evaluate(&s2));
    }

    #[test]
    fn test_condition_dns_healthy() {
        let c = Condition::DnsHealthy(false);
        let s = make_status(ConnectionStatus::Degraded, 50.0, 10.0, false);
        assert!(c.evaluate(&s));
    }

    #[test]
    fn test_condition_and_or_not() {
        let and = Condition::And(vec![
            Condition::LatencyAbove(50.0),
            Condition::PacketLossBelow(20.0),
        ]);
        let s = make_status(ConnectionStatus::Online, 100.0, 5.0, true);
        assert!(and.evaluate(&s));

        let s2 = make_status(ConnectionStatus::Degraded, 100.0, 30.0, true);
        assert!(!and.evaluate(&s2));

        let or = Condition::Or(vec![
            Condition::LatencyAbove(500.0),
            Condition::PacketLossAbove(30.0),
        ]);
        let s3 = make_status(ConnectionStatus::Degraded, 50.0, 40.0, true);
        assert!(or.evaluate(&s3));

        let not = Condition::Not(Box::new(Condition::StatusIs(ConnectionStatus::Online)));
        let s4 = make_status(ConnectionStatus::Degraded, 50.0, 5.0, true);
        assert!(not.evaluate(&s4));
    }

    #[tokio::test]
    async fn test_rule_engine_defaults() {
        let mut engine = RuleEngine::new();
        engine.load_defaults();
        assert!(engine.rules().len() >= 4);
    }

    #[tokio::test]
    async fn test_rule_evaluation() {
        let mut engine = RuleEngine::new();
        engine.add_rule(Rule::new(
            "test-loss",
            "Test",
            Condition::PacketLossAbove(20.0),
            vec![Action::LogWarning("loss".into())],
        ));

        let s = make_status(ConnectionStatus::Degraded, 50.0, 30.0, true);
        let results = engine.evaluate(&s).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rule_name, "test-loss");
        assert!(results[0].triggered);

        let s2 = make_status(ConnectionStatus::Online, 10.0, 0.0, true);
        let results2 = engine.evaluate(&s2).await;
        assert_eq!(results2.len(), 0);
    }

    #[tokio::test]
    async fn test_cooldown() {
        let mut engine = RuleEngine::new();
        engine.add_rule(
            Rule::new("cooldown-test", "Test", Condition::PacketLossAbove(10.0), vec![])
                .with_cooldown(3600),
        );

        let s = make_status(ConnectionStatus::Degraded, 50.0, 30.0, true);
        let r1 = engine.evaluate(&s).await;
        assert_eq!(r1.len(), 1);

        let r2 = engine.evaluate(&s).await;
        assert_eq!(r2.len(), 0);
    }
}
