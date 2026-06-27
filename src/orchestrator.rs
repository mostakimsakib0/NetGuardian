use std::time::Duration;

use crate::event_bus::bus::EventBus;
use crate::event_bus::events::{
    NetGuardianEvent, RetryEventPayload, JobEventPayload,
};
use crate::job::{Job, JobStatus};
use crate::queue_manager::QueueManager;
use crate::retry_engine::{FailureClass, RetryEngine, RetryPolicy};
use crate::plugin_manager::PluginManager;

use chrono::Utc;

pub struct RetryOrchestrator {
    engine: RetryEngine,
    event_bus: EventBus,
}

impl RetryOrchestrator {
    pub fn new(max_retries: u32, policy: RetryPolicy) -> Self {
        Self {
            engine: RetryEngine::new(max_retries, policy),
            event_bus: EventBus::new(),
        }
    }

    pub fn with_event_bus(mut self, event_bus: EventBus) -> Self {
        self.event_bus = event_bus;
        self
    }

    pub fn engine(&self) -> &RetryEngine {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut RetryEngine {
        &mut self.engine
    }

    pub fn update_network(&mut self, latency_ms: f64, packet_loss_pct: f64) {
        self.engine.with_network_conditions(latency_ms, packet_loss_pct);
    }

    pub async fn handle_failure(
        &mut self,
        job: &mut Job,
        error: &str,
        pm: &PluginManager,
        qm: &QueueManager,
    ) {
        let class = FailureClass::classify(error);

        self.engine.record_failure(error);
        job.retry_count += 1;

        self.event_bus.publish(NetGuardianEvent::RetryStarted(RetryEventPayload {
            job_id: job.id,
            attempt: job.retry_count,
            delay_ms: self.engine.next_retry().map(|d| d.as_millis() as u64).unwrap_or(0),
            failure_class: format!("{:?}", class),
            error: error.to_string(),
            timestamp: Utc::now().to_rfc3339(),
        }));

        if !class.is_retryable() {
            job.status = JobStatus::Failed(error.to_string());
            self.event_bus.publish(NetGuardianEvent::JobFailed(JobEventPayload {
                job_id: job.id,
                plugin: job.plugin.clone(),
                description: job.description.clone(),
                priority: job.priority.clone() as u8,
                retry_count: job.retry_count,
                owner: job.owner.clone(),
                error: Some(error.to_string()),
                progress: job.progress,
                timestamp: Utc::now().to_rfc3339(),
            }));
            self.event_bus.publish(NetGuardianEvent::RetryExhausted {
                job_id: job.id,
                attempts: job.retry_count,
                last_error: error.to_string(),
            });
            return;
        }

        if !self.engine.should_retry() || job.retry_count >= job.max_retries {
            job.status = JobStatus::Failed(error.to_string());
            self.event_bus.publish(NetGuardianEvent::RetryExhausted {
                job_id: job.id,
                attempts: job.retry_count,
                last_error: error.to_string(),
            });
            self.event_bus.publish(NetGuardianEvent::JobFailed(JobEventPayload {
                job_id: job.id,
                plugin: job.plugin.clone(),
                description: job.description.clone(),
                priority: job.priority.clone() as u8,
                retry_count: job.retry_count,
                owner: job.owner.clone(),
                error: Some(error.to_string()),
                progress: job.progress,
                timestamp: Utc::now().to_rfc3339(),
            }));
            return;
        }

        // Re-enqueue for retry
        job.status = JobStatus::Pending;
    }

    pub async fn handle_success(&mut self, job: &mut Job) {
        self.engine.record_success();
        job.status = JobStatus::Completed;

        self.event_bus.publish(NetGuardianEvent::JobCompleted(JobEventPayload {
            job_id: job.id,
            plugin: job.plugin.clone(),
            description: job.description.clone(),
            priority: job.priority.clone() as u8,
            retry_count: job.retry_count,
            owner: job.owner.clone(),
            error: None,
            progress: 100.0,
            timestamp: Utc::now().to_rfc3339(),
        }));
    }

    pub fn next_retry_delay(&mut self) -> Option<Duration> {
        self.engine.next_retry()
    }

    pub fn circuit_state(&self) -> String {
        format!("{:?}", self.engine.circuit_state())
    }
}
