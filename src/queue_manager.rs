use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::job::{Job, JobPriority, JobStatus};

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub total: usize,
    pub pending: usize,
    pub running: usize,
    pub paused: usize,
    pub completed: usize,
    pub failed: usize,
}

#[derive(Clone)]
pub struct QueueManager {
    inner: Arc<Mutex<QueueInner>>,
}

struct QueueInner {
    jobs: VecDeque<Job>,
    auto_resume: bool,
}

impl QueueManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(QueueInner {
                jobs: VecDeque::new(),
                auto_resume: true,
            })),
        }
    }

    pub async fn enqueue(
        &self,
        plugin: &str,
        description: &str,
        command: &str,
        args: Vec<String>,
        priority: JobPriority,
    ) -> Job {
        let job = Job::new(plugin, description, command, args, priority);

        let mut inner = self.inner.lock().await;
        inner.jobs.push_back(job.clone());
        job
    }

    pub async fn dequeue(&self) -> Option<Job> {
        let mut inner = self.inner.lock().await;
        let pos = inner
            .jobs
            .iter()
            .enumerate()
            .filter(|(_, j)| matches!(j.status, JobStatus::Pending))
            .max_by_key(|(_, j)| j.priority.clone())
            .map(|(i, _)| i);

        if let Some(idx) = pos {
            let mut job = inner.jobs.remove(idx).expect("index verified");
            job.status = JobStatus::Running;
            Some(job)
        } else {
            None
        }
    }

    pub async fn update_status(&self, id: u64, status: JobStatus) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(job) = inner.jobs.iter_mut().find(|j| j.id == id) {
            let _ = job.set_status(status);
            true
        } else {
            false
        }
    }

    pub async fn remove(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        let len_before = inner.jobs.len();
        inner.jobs.retain(|j| j.id != id);
        inner.jobs.len() < len_before
    }

    pub async fn list(&self) -> Vec<Job> {
        let inner = self.inner.lock().await;
        inner.jobs.iter().cloned().collect()
    }

    pub async fn get(&self, id: u64) -> Option<Job> {
        let inner = self.inner.lock().await;
        inner.jobs.iter().find(|j| j.id == id).cloned()
    }

    pub async fn stats(&self) -> QueueStats {
        let inner = self.inner.lock().await;
        let mut stats = QueueStats {
            total: 0,
            pending: 0,
            running: 0,
            paused: 0,
            completed: 0,
            failed: 0,
        };

        for job in &inner.jobs {
            stats.total += 1;
            match job.status {
                JobStatus::Pending => stats.pending += 1,
                JobStatus::Running => stats.running += 1,
                JobStatus::Paused => stats.paused += 1,
                JobStatus::Completed => stats.completed += 1,
                JobStatus::Failed(_) => stats.failed += 1,
                _ => {}
            }
        }

        stats
    }

    pub async fn retry_failed(&self) -> Vec<u64> {
        let mut inner = self.inner.lock().await;
        let mut retried = Vec::new();

        for job in inner.jobs.iter_mut() {
            if matches!(job.status, JobStatus::Failed(_))
                && job.retry_count < job.max_retries
            {
                job.retry_count += 1;
                job.status = JobStatus::Pending;
                retried.push(job.id);
            }
        }

        retried
    }

    pub async fn resume_all(&self) -> Vec<u64> {
        let mut inner = self.inner.lock().await;
        let mut resumed = Vec::new();

        for job in inner.jobs.iter_mut() {
            if matches!(job.status, JobStatus::Paused) {
                job.status = JobStatus::Pending;
                resumed.push(job.id);
            }
        }

        resumed
    }

    pub async fn clear_completed(&self) -> usize {
        let mut inner = self.inner.lock().await;
        let len_before = inner.jobs.len();
        inner
            .jobs
            .retain(|j| !matches!(j.status, JobStatus::Completed));
        len_before - inner.jobs.len()
    }

    pub async fn set_auto_resume(&self, enabled: bool) {
        let mut inner = self.inner.lock().await;
        inner.auto_resume = enabled;
    }

    pub async fn auto_resume_enabled(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.auto_resume
    }
}

impl Default for QueueManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_enqueue_dequeue() {
        let qm = QueueManager::new();
        let job = qm
            .enqueue("curl", "Download file", "curl", vec!["-O".into(), "file".into()], JobPriority::Normal)
            .await;

        let dequeued = qm.dequeue().await;
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().id, job.id);

        let op2 = qm.dequeue().await;
        assert!(op2.is_none());
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let qm = QueueManager::new();
        qm.enqueue("git", "low priority", "git", vec!["clone".into()], JobPriority::Low).await;
        qm.enqueue("git", "high priority", "git", vec!["pull".into()], JobPriority::Critical).await;

        let first = qm.dequeue().await.unwrap();
        assert_eq!(first.description, "high priority");
    }

    #[tokio::test]
    async fn test_update_status() {
        let qm = QueueManager::new();
        let job = qm.enqueue("test", "test op", "echo", vec![], JobPriority::Normal).await;

        assert!(qm.update_status(job.id, JobStatus::Running).await);
        assert!(qm.update_status(job.id, JobStatus::Completed).await);

        let jobs = qm.list().await;
        assert_eq!(jobs[0].status, JobStatus::Completed);
    }

    #[tokio::test]
    async fn test_remove() {
        let qm = QueueManager::new();
        let job = qm.enqueue("test", "to remove", "rm", vec![], JobPriority::Normal).await;
        assert!(qm.remove(job.id).await);
        assert!(qm.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_stats() {
        let qm = QueueManager::new();
        qm.enqueue("a", "op1", "cmd", vec![], JobPriority::Normal).await;
        let j2 = qm.enqueue("b", "op2", "cmd", vec![], JobPriority::Normal).await;
        qm.update_status(j2.id, JobStatus::Running).await;

        let stats = qm.stats().await;
        assert_eq!(stats.total, 2);
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.running, 1);
    }

    #[tokio::test]
    async fn test_retry_failed() {
        let qm = QueueManager::new();
        let job = qm.enqueue("test", "failing", "cmd", vec![], JobPriority::Normal).await;
        qm.update_status(job.id, JobStatus::Failed("error".into())).await;

        let retried = qm.retry_failed().await;
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0], job.id);

        let jobs = qm.list().await;
        assert_eq!(jobs[0].status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn test_resume_all() {
        let qm = QueueManager::new();
        let job = qm.enqueue("test", "paused", "cmd", vec![], JobPriority::Normal).await;
        qm.update_status(job.id, JobStatus::Paused).await;

        let resumed = qm.resume_all().await;
        assert_eq!(resumed.len(), 1);
        assert_eq!(qm.list().await[0].status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn test_auto_resume() {
        let qm = QueueManager::new();
        assert!(qm.auto_resume_enabled().await);
        qm.set_auto_resume(false).await;
        assert!(!qm.auto_resume_enabled().await);
    }
}
