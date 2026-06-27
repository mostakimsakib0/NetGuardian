use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::event_bus::bus::EventBus;
use crate::event_bus::events::{NetGuardianEvent, JobEventPayload, DownloadEventPayload};
use crate::job::{Job, JobStatus};
use crate::orchestrator::RetryOrchestrator;
use crate::plugin_manager::PluginManager;
use crate::process_supervisor::ProcessSupervisor;
use crate::queue_manager::QueueManager;
use crate::download_manager::DownloadManager;
use crate::metrics::MetricsEngine;

use chrono::Utc;

pub struct JobExecutor {
    pm: Arc<Mutex<PluginManager>>,
    qm: QueueManager,
    dm: DownloadManager,
    supervisor: Arc<ProcessSupervisor>,
    orchestrator: Arc<Mutex<RetryOrchestrator>>,
    event_bus: EventBus,
    metrics: Arc<MetricsEngine>,
    max_concurrent: usize,
}

impl JobExecutor {
    pub fn new(
        pm: Arc<Mutex<PluginManager>>,
        qm: QueueManager,
        dm: DownloadManager,
        supervisor: Arc<ProcessSupervisor>,
        orchestrator: Arc<Mutex<RetryOrchestrator>>,
        event_bus: EventBus,
        metrics: Arc<MetricsEngine>,
    ) -> Self {
        Self {
            pm,
            qm,
            dm,
            supervisor,
            orchestrator,
            event_bus,
            metrics,
            max_concurrent: 3,
        }
    }

    pub fn with_max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = n;
        self
    }

    pub async fn execute_next(&self) {
        let Some(mut job) = self.qm.dequeue().await else {
            return;
        };

        // Publish JobStarted
        self.event_bus.publish(NetGuardianEvent::JobStarted(JobEventPayload {
            job_id: job.id,
            plugin: job.plugin.clone(),
            description: job.description.clone(),
            priority: job.priority as u8,
            retry_count: job.retry_count,
            owner: job.owner.clone(),
            error: None,
            progress: 0.0,
            timestamp: Utc::now().to_rfc3339(),
        }));

        let plugin_name = job.plugin.clone();
        let pm = self.pm.lock().await;

        // Check if the plugin exists and has the right capabilities
        let plugin = match pm.get(&plugin_name).await {
            Some(p) => p,
            None => {
                job.status = JobStatus::Failed(format!("plugin '{}' not found", plugin_name));
                self.qm.update_status(job.id, JobStatus::Failed(format!("plugin '{}' not found", plugin_name))).await;
                return;
            }
        };

        // Build args from job command + args
        let mut cmd_args = vec![job.command.clone()];
        cmd_args.extend(job.args.clone());

        // Spawn the process
        let proc = match self.supervisor.spawn(&plugin_name, plugin_name.as_str(), &cmd_args).await {
            Ok(p) => p,
            Err(e) => {
                let err = format!("failed to spawn: {}", e);
                job.status = JobStatus::Failed(err.clone());
                self.qm.update_status(job.id, JobStatus::Failed(err.clone())).await;
                self.event_bus.publish(NetGuardianEvent::JobFailed(JobEventPayload {
                    job_id: job.id,
                    plugin: job.plugin.clone(),
                    description: job.description.clone(),
                    priority: job.priority as u8,
                    retry_count: job.retry_count,
                    owner: job.owner.clone(),
                    error: Some(err),
                    progress: 0.0,
                    timestamp: Utc::now().to_rfc3339(),
                }));
                self.metrics.record_operation(&plugin_name, &job.description, false, job.retry_count);
                return;
            }
        };

        // Track as download if applicable
        if job.command == "curl" || job.command == "wget" {
            let url = job.args.first().cloned().unwrap_or_default();
            self.dm.track(&job, &url).await;
        }

        // Wait for completion with periodic progress polling
        let poll_interval = Duration::from_millis(500);
        let timeout = Duration::from_secs(3600); // 1 hour max
        let start = std::time::Instant::now();
        let mut last_progress_pct = 0.0;

        loop {
            if start.elapsed() > timeout {
                let _ = proc.terminate(Duration::from_secs(3));
                job.status = JobStatus::Failed("timeout".into());
                self.qm.update_status(job.id, JobStatus::Failed("timeout".into())).await;
                self.event_bus.publish(NetGuardianEvent::JobFailed(JobEventPayload {
                    job_id: job.id,
                    plugin: job.plugin.clone(),
                    description: job.description.clone(),
                    priority: job.priority as u8,
                    retry_count: job.retry_count,
                    owner: job.owner.clone(),
                    error: Some("timeout".into()),
                    progress: job.progress,
                    timestamp: Utc::now().to_rfc3339(),
                }));
                self.metrics.record_operation(&plugin_name, &job.description, false, job.retry_count);
                break;
            }

            match proc.try_wait() {
                Ok(Some(exit_code)) => {
                    if exit_code == 0 {
                        // Success
                        let mut orch = self.orchestrator.lock().await;
                        orch.handle_success(&mut job).await;
                        self.qm.update_status(job.id, JobStatus::Completed).await;
                        self.dm.complete(job.id, 0).await;
                        self.event_bus.publish(NetGuardianEvent::DownloadCompleted(DownloadEventPayload {
                            job_id: job.id,
                            url: job.args.first().cloned().unwrap_or_default(),
                            plugin: job.plugin.clone(),
                            progress: 100.0,
                            speed_bytes_per_sec: 0.0,
                            error: None,
                            timestamp: Utc::now().to_rfc3339(),
                        }));
                        self.metrics.record_operation(&plugin_name, &job.description, true, job.retry_count);
                    } else {
                        // Failure — hand to retry orchestrator
                        let err = format!("exit code {}", exit_code);
                        let mut orch = self.orchestrator.lock().await;
                        orch.handle_failure(&mut job, &err, &self.pm.lock().await, &self.qm).await;
                        self.qm.update_status(job.id, job.status.clone()).await;
                        self.dm.fail(job.id, &err).await;
                        self.metrics.record_operation(&plugin_name, &job.description, false, job.retry_count);
                        self.metrics.record_retry();
                    }
                    break;
                }
                Ok(None) => {
                    // Still running — emit progress event periodically
                    let elapsed = start.elapsed().as_secs_f64();
                    let progress = (elapsed / 60.0).min(99.0); // crude progress estimate
                    if (progress - last_progress_pct).abs() > 5.0 {
                        last_progress_pct = progress;
                        job.set_progress(progress);
                        self.event_bus.publish(NetGuardianEvent::JobProgress {
                            job_id: job.id,
                            progress,
                            plugin: job.plugin.clone(),
                        });
                        self.dm.update_progress(job.id, elapsed as u64, 3600, 0.0).await;
                    }
                    tokio::time::sleep(poll_interval).await;
                }
                Err(e) => {
                    let err = format!("process error: {}", e);
                    job.status = JobStatus::Failed(err.clone());
                    self.qm.update_status(job.id, JobStatus::Failed(err.clone())).await;
                    self.metrics.record_operation(&plugin_name, &job.description, false, job.retry_count);
                    break;
                }
            }
        }

        self.supervisor.cleanup(&plugin_name).await;
    }

    pub async fn execute_loop(&self) {
        let mut rx = self.event_bus.subscribe();
        loop {
            tokio::select! {
                Ok(event) = rx.recv() => {
                    match event {
                        NetGuardianEvent::JobControlPause { job_id } => {
                            self.pause_job(job_id).await;
                        }
                        NetGuardianEvent::JobControlResume { job_id } => {
                            self.resume_job(job_id).await;
                        }
                        NetGuardianEvent::JobControlCancel { job_id } => {
                            self.cancel_job(job_id).await;
                        }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    let active = self.supervisor.all_running().await.len();
                    if active < self.max_concurrent {
                        self.execute_next().await;
                    }
                }
            }
        }
    }

    pub async fn pause_job(&self, job_id: u64) -> bool {
        let jobs = self.qm.list().await;
        let job = match jobs.iter().find(|j| j.id == job_id) {
            Some(j) => j,
            None => return false,
        };
        let results = self.supervisor.stop_all(&job.plugin).await;
        self.qm.update_status(job_id, JobStatus::Paused).await;
        results.iter().any(|r| r.is_ok())
    }

    pub async fn resume_job(&self, job_id: u64) -> bool {
        let jobs = self.qm.list().await;
        let job = match jobs.iter().find(|j| j.id == job_id) {
            Some(j) => j,
            None => return false,
        };
        let results = self.supervisor.cont_all(&job.plugin).await;
        self.qm.update_status(job_id, JobStatus::Running).await;
        results.iter().any(|r| r.is_ok())
    }

    pub async fn cancel_job(&self, job_id: u64) -> bool {
        let jobs = self.qm.list().await;
        let job = match jobs.iter().find(|j| j.id == job_id) {
            Some(j) => j,
            None => return false,
        };
        let results = self.supervisor.terminate_all(&job.plugin, Duration::from_secs(3)).await;
        self.qm.update_status(job_id, JobStatus::Cancelled).await;
        self.supervisor.cleanup(&job.plugin).await;
        results.iter().any(|r| r.is_ok())
    }
}
