use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq)]
pub enum OperationStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct QueuedOperation {
    pub id: u64,
    pub plugin: String,
    pub description: String,
    pub command: String,
    pub args: Vec<String>,
    pub priority: u8,
    pub status: OperationStatus,
    pub created_at: Instant,
    pub retry_count: u32,
    pub max_retries: u32,
}

impl QueuedOperation {
    pub fn new(
        id: u64,
        plugin: &str,
        description: &str,
        command: &str,
        args: Vec<String>,
        priority: u8,
    ) -> Self {
        Self {
            id,
            plugin: plugin.to_string(),
            description: description.to_string(),
            command: command.to_string(),
            args,
            priority,
            status: OperationStatus::Pending,
            created_at: Instant::now(),
            retry_count: 0,
            max_retries: 3,
        }
    }
}

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
    next_id: Arc<Mutex<u64>>,
}

struct QueueInner {
    queue: VecDeque<QueuedOperation>,
    auto_resume: bool,
}

impl QueueManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(QueueInner {
                queue: VecDeque::new(),
                auto_resume: true,
            })),
            next_id: Arc::new(Mutex::new(1)),
        }
    }

    pub async fn enqueue(
        &self,
        plugin: &str,
        description: &str,
        command: &str,
        args: Vec<String>,
        priority: u8,
    ) -> u64 {
        let mut id_lock = self.next_id.lock().await;
        let id = *id_lock;
        *id_lock += 1;

        let op = QueuedOperation::new(id, plugin, description, command, args, priority);

        let mut inner = self.inner.lock().await;
        inner.queue.push_back(op);
        id
    }

    pub async fn dequeue(&self) -> Option<QueuedOperation> {
        let mut inner = self.inner.lock().await;
        let pos = inner
            .queue
            .iter()
            .enumerate()
            .filter(|(_, op)| matches!(op.status, OperationStatus::Pending))
            .max_by_key(|(_, op)| op.priority)
            .map(|(i, _)| i);

        if let Some(idx) = pos {
            let mut op = inner.queue.remove(idx).expect("index verified");
            op.status = OperationStatus::Running;
            Some(op)
        } else {
            None
        }
    }

    pub async fn update_status(&self, id: u64, status: OperationStatus) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(op) = inner.queue.iter_mut().find(|op| op.id == id) {
            op.status = status;
            true
        } else {
            false
        }
    }

    pub async fn remove(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        let len_before = inner.queue.len();
        inner.queue.retain(|op| op.id != id);
        inner.queue.len() < len_before
    }

    pub async fn list(&self) -> Vec<QueuedOperation> {
        let inner = self.inner.lock().await;
        inner.queue.iter().cloned().collect()
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

        for op in &inner.queue {
            stats.total += 1;
            match op.status {
                OperationStatus::Pending => stats.pending += 1,
                OperationStatus::Running => stats.running += 1,
                OperationStatus::Paused => stats.paused += 1,
                OperationStatus::Completed => stats.completed += 1,
                OperationStatus::Failed(_) => stats.failed += 1,
            }
        }

        stats
    }

    pub async fn retry_failed(&self) -> Vec<u64> {
        let mut inner = self.inner.lock().await;
        let mut retried = Vec::new();

        for op in inner.queue.iter_mut() {
            if matches!(op.status, OperationStatus::Failed(_))
                && op.retry_count < op.max_retries
            {
                op.retry_count += 1;
                op.status = OperationStatus::Pending;
                retried.push(op.id);
            }
        }

        retried
    }

    pub async fn resume_all(&self) -> Vec<u64> {
        let mut inner = self.inner.lock().await;
        let mut resumed = Vec::new();

        for op in inner.queue.iter_mut() {
            if matches!(op.status, OperationStatus::Paused) {
                op.status = OperationStatus::Pending;
                resumed.push(op.id);
            }
        }

        resumed
    }

    pub async fn clear_completed(&self) -> usize {
        let mut inner = self.inner.lock().await;
        let len_before = inner.queue.len();
        inner
            .queue
            .retain(|op| !matches!(op.status, OperationStatus::Completed));
        len_before - inner.queue.len()
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
        let id = qm
            .enqueue("curl", "Download file", "curl", vec!["-O".into(), "file".into()], 5)
            .await;
        assert_eq!(id, 1);

        let op = qm.dequeue().await;
        assert!(op.is_some());
        assert_eq!(op.unwrap().id, 1);

        let op2 = qm.dequeue().await;
        assert!(op2.is_none());
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let qm = QueueManager::new();
        qm.enqueue("git", "low priority", "git", vec!["clone".into()], 1).await;
        qm.enqueue("git", "high priority", "git", vec!["pull".into()], 10).await;

        let first = qm.dequeue().await.unwrap();
        assert_eq!(first.description, "high priority");
    }

    #[tokio::test]
    async fn test_update_status() {
        let qm = QueueManager::new();
        let id = qm.enqueue("test", "test op", "echo", vec![], 5).await;

        assert!(qm.update_status(id, OperationStatus::Running).await);
        assert!(qm.update_status(id, OperationStatus::Completed).await);

        let ops = qm.list().await;
        assert_eq!(ops[0].status, OperationStatus::Completed);
    }

    #[tokio::test]
    async fn test_remove() {
        let qm = QueueManager::new();
        let id = qm.enqueue("test", "to remove", "rm", vec![], 5).await;
        assert!(qm.remove(id).await);
        assert!(qm.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_stats() {
        let qm = QueueManager::new();
        qm.enqueue("a", "op1", "cmd", vec![], 5).await;
        let id2 = qm.enqueue("b", "op2", "cmd", vec![], 5).await;
        qm.update_status(id2, OperationStatus::Running).await;

        let stats = qm.stats().await;
        assert_eq!(stats.total, 2);
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.running, 1);
    }

    #[tokio::test]
    async fn test_retry_failed() {
        let qm = QueueManager::new();
        let id = qm.enqueue("test", "failing", "cmd", vec![], 5).await;
        qm.update_status(id, OperationStatus::Failed("error".into())).await;

        let retried = qm.retry_failed().await;
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0], id);

        let op = qm.list().await;
        assert_eq!(op[0].status, OperationStatus::Pending);
    }

    #[tokio::test]
    async fn test_resume_all() {
        let qm = QueueManager::new();
        let id = qm.enqueue("test", "paused", "cmd", vec![], 5).await;
        qm.update_status(id, OperationStatus::Paused).await;

        let resumed = qm.resume_all().await;
        assert_eq!(resumed.len(), 1);
        assert_eq!(qm.list().await[0].status, OperationStatus::Pending);
    }

    #[tokio::test]
    async fn test_auto_resume() {
        let qm = QueueManager::new();
        assert!(qm.auto_resume_enabled().await);
        qm.set_auto_resume(false).await;
        assert!(!qm.auto_resume_enabled().await);
    }
}
