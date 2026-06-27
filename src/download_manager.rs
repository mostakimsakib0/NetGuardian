use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Completed,
    Failed(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Download {
    pub id: u64,
    pub url: String,
    pub destination: PathBuf,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub status: DownloadStatus,
    pub plugin: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub speed_bytes_per_sec: f64,
    pub error: Option<String>,
}

impl Download {
    pub fn progress_pct(&self) -> f64 {
        if self.total_bytes > 0 {
            (self.downloaded_bytes as f64 / self.total_bytes as f64) * 100.0
        } else {
            0.0
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, DownloadStatus::Downloading)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadStats {
    pub total: usize,
    pub active: usize,
    pub queued: usize,
    pub paused: usize,
    pub completed: usize,
    pub failed: usize,
    pub total_bytes_downloaded: u64,
    pub active_speed: f64,
}

struct DownloadInner {
    downloads: Vec<Download>,
    next_id: u64,
    active_limit: usize,
    base_dir: PathBuf,
}

pub struct DownloadManager {
    inner: Arc<Mutex<DownloadInner>>,
}

impl DownloadManager {
    pub fn new(download_dir: &Path) -> Self {
        let _ = std::fs::create_dir_all(download_dir);

        Self {
            inner: Arc::new(Mutex::new(DownloadInner {
                downloads: Vec::new(),
                next_id: 1,
                active_limit: 3,
                base_dir: download_dir.to_path_buf(),
            })),
        }
    }

    pub async fn add(
        &self,
        url: &str,
        destination: &Path,
        plugin: &str,
    ) -> Download {
        let mut inner = self.inner.lock().await;
        let id = inner.next_id;
        inner.next_id += 1;

        let download = Download {
            id,
            url: url.to_string(),
            destination: destination.to_path_buf(),
            total_bytes: 0,
            downloaded_bytes: 0,
            status: DownloadStatus::Queued,
            plugin: plugin.to_string(),
            started_at: None,
            completed_at: None,
            speed_bytes_per_sec: 0.0,
            error: None,
        };

        inner.downloads.push(download.clone());
        download
    }

    pub async fn start(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.downloads.iter_mut().find(|d| d.id == id) {
            if dl.status == DownloadStatus::Queued {
                dl.status = DownloadStatus::Downloading;
                dl.started_at = Some(chrono::Utc::now().to_rfc3339());
                return true;
            }
        }
        false
    }

    pub async fn pause(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.downloads.iter_mut().find(|d| d.id == id) {
            if dl.status == DownloadStatus::Downloading {
                dl.status = DownloadStatus::Paused;
                return true;
            }
        }
        false
    }

    pub async fn resume(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.downloads.iter_mut().find(|d| d.id == id) {
            if dl.status == DownloadStatus::Paused {
                dl.status = DownloadStatus::Downloading;
                return true;
            }
        }
        false
    }

    pub async fn complete(&self, id: u64, total_bytes: u64) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.downloads.iter_mut().find(|d| d.id == id) {
            dl.status = DownloadStatus::Completed;
            dl.total_bytes = total_bytes;
            dl.downloaded_bytes = total_bytes;
            dl.completed_at = Some(chrono::Utc::now().to_rfc3339());
            true
        } else {
            false
        }
    }

    pub async fn fail(&self, id: u64, error: &str) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.downloads.iter_mut().find(|d| d.id == id) {
            dl.status = DownloadStatus::Failed(error.to_string());
            dl.error = Some(error.to_string());
            true
        } else {
            false
        }
    }

    pub async fn update_progress(&self, id: u64, downloaded: u64, total: u64) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(dl) = inner.downloads.iter_mut().find(|d| d.id == id) {
            dl.downloaded_bytes = downloaded;
            dl.total_bytes = total;
            true
        } else {
            false
        }
    }

    pub async fn get(&self, id: u64) -> Option<Download> {
        let inner = self.inner.lock().await;
        inner.downloads.iter().find(|d| d.id == id).cloned()
    }

    pub async fn list(&self) -> Vec<Download> {
        let inner = self.inner.lock().await;
        inner.downloads.clone()
    }

    pub async fn list_active(&self) -> Vec<Download> {
        let inner = self.inner.lock().await;
        inner
            .downloads
            .iter()
            .filter(|d| d.is_active())
            .cloned()
            .collect()
    }

    pub async fn remove(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        let len_before = inner.downloads.len();
        inner.downloads.retain(|d| d.id != id);
        inner.downloads.len() < len_before
    }

    pub async fn clear_completed(&self) -> usize {
        let mut inner = self.inner.lock().await;
        let len_before = inner.downloads.len();
        inner
            .downloads
            .retain(|d| !matches!(d.status, DownloadStatus::Completed));
        len_before - inner.downloads.len()
    }

    pub async fn stats(&self) -> DownloadStats {
        let inner = self.inner.lock().await;
        let mut stats = DownloadStats {
            total: 0,
            active: 0,
            queued: 0,
            paused: 0,
            completed: 0,
            failed: 0,
            total_bytes_downloaded: 0,
            active_speed: 0.0,
        };

        for dl in &inner.downloads {
            stats.total += 1;
            stats.total_bytes_downloaded += dl.downloaded_bytes;

            match dl.status {
                DownloadStatus::Queued => stats.queued += 1,
                DownloadStatus::Downloading => {
                    stats.active += 1;
                    stats.active_speed += dl.speed_bytes_per_sec;
                }
                DownloadStatus::Paused => stats.paused += 1,
                DownloadStatus::Completed => stats.completed += 1,
                DownloadStatus::Failed(_) => stats.failed += 1,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_add_and_list() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        let dl = dm
            .add("https://example.com/file", &dir.path().join("file"), "curl")
            .await;

        assert_eq!(dl.id, 1);
        assert_eq!(dl.url, "https://example.com/file");
        assert_eq!(dl.status, DownloadStatus::Queued);

        let list = dm.list().await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn test_lifecycle() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        let dl = dm
            .add("https://example.com/file", &dir.path().join("file"), "curl")
            .await;

        assert!(dm.start(dl.id).await);
        assert_eq!(dm.get(dl.id).await.unwrap().status, DownloadStatus::Downloading);

        assert!(dm.pause(dl.id).await);
        assert_eq!(dm.get(dl.id).await.unwrap().status, DownloadStatus::Paused);

        assert!(dm.resume(dl.id).await);
        assert_eq!(dm.get(dl.id).await.unwrap().status, DownloadStatus::Downloading);

        assert!(dm.complete(dl.id, 1024).await);
        assert_eq!(dm.get(dl.id).await.unwrap().status, DownloadStatus::Completed);
    }

    #[tokio::test]
    async fn test_fail() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        let dl = dm
            .add("https://example.com/file", &dir.path().join("file"), "curl")
            .await;

        dm.start(dl.id).await;
        assert!(dm.fail(dl.id, "Connection timeout").await);

        let failed = dm.get(dl.id).await.unwrap();
        assert_eq!(failed.status, DownloadStatus::Failed("Connection timeout".into()));
        assert_eq!(failed.error, Some("Connection timeout".into()));
    }

    #[tokio::test]
    async fn test_remove() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        let dl = dm
            .add("url", &dir.path().join("f"), "curl")
            .await;

        assert!(dm.remove(dl.id).await);
        assert!(dm.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_progress() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        let dl = dm
            .add("url", &dir.path().join("f"), "curl")
            .await;

        dm.start(dl.id).await;
        dm.update_progress(dl.id, 500, 1000).await;

        let d = dm.get(dl.id).await.unwrap();
        assert_eq!(d.downloaded_bytes, 500);
        assert_eq!(d.total_bytes, 1000);
        assert!((d.progress_pct() - 50.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_stats() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        let d1 = dm.add("u1", &dir.path().join("f1"), "curl").await;
        let d2 = dm.add("u2", &dir.path().join("f2"), "wget").await;
        let d3 = dm.add("u3", &dir.path().join("f3"), "curl").await;

        dm.start(d1.id).await;
        dm.start(d2.id).await;
        dm.complete(d3.id, 2048).await;

        let stats = dm.stats().await;
        assert_eq!(stats.total, 3);
        assert_eq!(stats.active, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.total_bytes_downloaded, 2048);
    }

    #[tokio::test]
    async fn test_active_limit() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        assert_eq!(dm.active_limit().await, 3);
        dm.set_active_limit(5).await;
        assert_eq!(dm.active_limit().await, 5);
    }

    #[tokio::test]
    async fn test_clear_completed() {
        let dir = tempdir().unwrap();
        let dm = DownloadManager::new(dir.path());

        let d1 = dm.add("u1", &dir.path().join("f1"), "curl").await;
        let d2 = dm.add("u2", &dir.path().join("f2"), "curl").await;

        dm.complete(d1.id, 100).await;
        dm.start(d2.id).await;

        assert_eq!(dm.clear_completed().await, 1);
        assert_eq!(dm.list().await.len(), 1);
    }
}
