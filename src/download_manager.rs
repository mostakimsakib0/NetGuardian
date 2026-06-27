use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::job::Job;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadProgress {
    pub job_id: u64,
    pub url: String,
    pub plugin: String,
    pub destination: PathBuf,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub speed_bytes_per_sec: f64,
    pub error: Option<String>,
}

impl DownloadProgress {
    pub fn progress_pct(&self) -> f64 {
        if self.total_bytes > 0 {
            (self.downloaded_bytes as f64 / self.total_bytes as f64) * 100.0
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadStats {
    pub total: usize,
    pub active: usize,
    pub completed: usize,
    pub failed: usize,
    pub total_bytes_downloaded: u64,
    pub active_speed: f64,
}

struct DownloadInner {
    jobs: HashMap<u64, DownloadProgress>,
    active_limit: usize,
    base_dir: PathBuf,
}

#[derive(Clone)]
pub struct DownloadManager {
    inner: Arc<Mutex<DownloadInner>>,
}

impl DownloadManager {
    pub fn new(download_dir: &Path) -> Self {
        let _ = std::fs::create_dir_all(download_dir);

        Self {
            inner: Arc::new(Mutex::new(DownloadInner {
                jobs: HashMap::new(),
                active_limit: 3,
                base_dir: download_dir.to_path_buf(),
            })),
        }
    }

    pub async fn track(&self, job: &Job, url: &str) -> DownloadProgress {
        let mut inner = self.inner.lock().await;
        let progress = DownloadProgress {
            job_id: job.id,
            url: url.to_string(),
            plugin: job.plugin.clone(),
            destination: inner.base_dir.join(sanitize_filename(url)),
            total_bytes: 0,
            downloaded_bytes: 0,
            speed_bytes_per_sec: 0.0,
            error: None,
        };
        inner.jobs.insert(job.id, progress.clone());
        progress
    }

    pub async fn update_progress(&self, job_id: u64, downloaded: u64, total: u64, speed: f64) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.jobs.get_mut(&job_id) {
            dl.downloaded_bytes = downloaded;
            dl.total_bytes = total;
            dl.speed_bytes_per_sec = speed;
            true
        } else {
            false
        }
    }

    pub async fn complete(&self, job_id: u64, total_bytes: u64) {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.jobs.get_mut(&job_id) {
            dl.downloaded_bytes = total_bytes;
            dl.total_bytes = total_bytes;
        }
    }

    pub async fn fail(&self, job_id: u64, error: &str) {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.jobs.get_mut(&job_id) {
            dl.error = Some(error.to_string());
        }
    }

    pub async fn get(&self, job_id: u64) -> Option<DownloadProgress> {
        let inner = self.inner.lock().await;
        inner.jobs.get(&job_id).cloned()
    }

    pub async fn list(&self) -> Vec<DownloadProgress> {
        let inner = self.inner.lock().await;
        inner.jobs.values().cloned().collect()
    }

    pub async fn remove(&self, job_id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        inner.jobs.remove(&job_id).is_some()
    }

    pub async fn stats(&self) -> DownloadStats {
        let inner = self.inner.lock().await;
        let mut stats = DownloadStats {
            total: 0,
            active: 0,
            completed: 0,
            failed: 0,
            total_bytes_downloaded: 0,
            active_speed: 0.0,
        };

        for dl in inner.jobs.values() {
            stats.total += 1;
            stats.total_bytes_downloaded += dl.downloaded_bytes;
            if dl.error.is_some() {
                stats.failed += 1;
            } else if dl.total_bytes > 0 && dl.downloaded_bytes >= dl.total_bytes {
                stats.completed += 1;
            } else {
                stats.active += 1;
                stats.active_speed += dl.speed_bytes_per_sec;
            }
        }

        stats
    }

    pub async fn active_limit(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.active_limit
    }

    pub async fn set_active_limit(&self, limit: usize) {
        let mut inner = self.inner.lock().await;
        inner.active_limit = limit;
    }
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new(&PathBuf::from("/tmp/netguardian-downloads"))
    }
}

fn sanitize_filename(url: &str) -> String {
    url.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{Job, JobPriority};
    use tempfile::tempdir;

    fn make_job() -> Job {
        Job::new("curl", "download test", "curl", vec![], JobPriority::Normal)
    }

    #[tokio::test]
    async fn test_track_and_list() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());
        let job = make_job();

        let dl = dm.track(&job, "https://example.com/file").await;
        assert_eq!(dl.job_id, job.id);
        assert_eq!(dl.url, "https://example.com/file");

        let list = dm.list().await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn test_progress() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());
        let job = make_job();

        dm.track(&job, "https://example.com/file").await;
        dm.update_progress(job.id, 500, 1000, 50.0).await;

        let dl = dm.get(job.id).await.unwrap();
        assert_eq!(dl.downloaded_bytes, 500);
        assert_eq!(dl.total_bytes, 1000);
        assert!((dl.progress_pct() - 50.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_fail() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());
        let job = make_job();

        dm.track(&job, "https://example.com/file").await;
        dm.fail(job.id, "Connection timeout").await;

        let dl = dm.get(job.id).await.unwrap();
        assert_eq!(dl.error, Some("Connection timeout".into()));
    }

    #[tokio::test]
    async fn test_remove() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());
        let job = make_job();

        dm.track(&job, "url").await;
        assert!(dm.remove(job.id).await);
        assert!(dm.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_stats() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());
        let j1 = make_job();
        let j2 = Job::new("wget", "dl2", "wget", vec![], JobPriority::Normal);
        let j3 = Job::new("curl", "dl3", "curl", vec![], JobPriority::Normal);

        dm.track(&j1, "u1").await;
        dm.track(&j2, "u2").await;
        dm.track(&j3, "u3").await;
        dm.complete(j3.id, 2048).await;

        let stats = dm.stats().await;
        assert_eq!(stats.total, 3);
        assert_eq!(stats.active, 2);
        assert_eq!(stats.completed, 1);
    }

    #[tokio::test]
    async fn test_active_limit() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        assert_eq!(dm.active_limit().await, 3);
        dm.set_active_limit(5).await;
        assert_eq!(dm.active_limit().await, 5);
    }
}
