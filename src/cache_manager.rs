use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CacheType {
    GitObjects,
    OciLayers,
    PipWheels,
    AptPackages,
    NpmPackages,
    Generic,
}

impl CacheType {
    pub fn as_str(&self) -> &str {
        match self {
            CacheType::GitObjects => "git",
            CacheType::OciLayers => "oci",
            CacheType::PipWheels => "pip",
            CacheType::AptPackages => "apt",
            CacheType::NpmPackages => "npm",
            CacheType::Generic => "generic",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEntry {
    pub key: String,
    pub path: PathBuf,
    pub size_bytes: u64,
    pub cache_type: CacheType,
    pub created_at: String,
    pub last_access: String,
    pub access_count: u64,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheStats {
    pub total_entries: usize,
    pub total_size_bytes: u64,
    pub max_size_bytes: u64,
    pub entries_by_type: HashMap<String, usize>,
    pub size_by_type: HashMap<String, u64>,
    pub hit_count: u64,
    pub miss_count: u64,
}

struct CacheInner {
    entries: Vec<CacheEntry>,
    base_path: PathBuf,
    max_size: u64,
    hits: u64,
    misses: u64,
}

pub struct CacheManager {
    inner: Arc<Mutex<CacheInner>>,
}

impl CacheManager {
    pub fn new(base_path: &Path, max_size_bytes: u64) -> Self {
        let _ = std::fs::create_dir_all(base_path);

        Self {
            inner: Arc::new(Mutex::new(CacheInner {
                entries: Vec::new(),
                base_path: base_path.to_path_buf(),
                max_size: max_size_bytes,
                hits: 0,
                misses: 0,
            })),
        }
    }

    pub async fn get(&self, key: &str, cache_type: &CacheType) -> Option<CacheEntry> {
        let mut inner = self.inner.lock().await;
        let idx = inner
            .entries
            .iter()
            .position(|e| e.key == key && e.cache_type == *cache_type && e.path.exists());

        if let Some(idx) = idx {
            let entry = inner.entries[idx].clone();
            inner.entries[idx].access_count += 1;
            inner.entries[idx].last_access = chrono::Utc::now().to_rfc3339();
            inner.hits += 1;
            Some(entry)
        } else {
            inner.misses += 1;
            None
        }
    }

    pub async fn put(
        &self,
        key: &str,
        cache_type: &CacheType,
        data: &[u8],
        metadata: HashMap<String, String>,
    ) -> std::io::Result<CacheEntry> {
        let mut inner = self.inner.lock().await;

        let type_dir = inner.base_path.join(cache_type.as_str());
        let _ = std::fs::create_dir_all(&type_dir);

        let filename = sanitize_filename(key);
        let file_path = type_dir.join(&filename);

        std::fs::write(&file_path, data)?;
        let file_size = file_path.metadata()?.len();

        let now = chrono::Utc::now().to_rfc3339();
        let entry = CacheEntry {
            key: key.to_string(),
            path: file_path,
            size_bytes: file_size,
            cache_type: cache_type.clone(),
            created_at: now.clone(),
            last_access: now,
            access_count: 0,
            metadata,
        };

        inner.entries.push(entry.clone());

        while inner.total_size() > inner.max_size {
            if !self.evict_one(&mut inner).await {
                break;
            }
        }

        Ok(entry)
    }

    pub async fn put_path(
        &self,
        key: &str,
        cache_type: &CacheType,
        file_path: &Path,
        metadata: HashMap<String, String>,
    ) -> std::io::Result<CacheEntry> {
        let mut inner = self.inner.lock().await;

        let data = std::fs::read(file_path)?;
        let file_size = data.len() as u64;

        let type_dir = inner.base_path.join(cache_type.as_str());
        let _ = std::fs::create_dir_all(&type_dir);

        let filename = sanitize_filename(key);
        let dest_path = type_dir.join(&filename);

        std::fs::copy(file_path, &dest_path)?;

        let now = chrono::Utc::now().to_rfc3339();
        let entry = CacheEntry {
            key: key.to_string(),
            path: dest_path,
            size_bytes: file_size,
            cache_type: cache_type.clone(),
            created_at: now.clone(),
            last_access: now,
            access_count: 0,
            metadata,
        };

        inner.entries.push(entry.clone());

        while inner.total_size() > inner.max_size {
            if !self.evict_one(&mut inner).await {
                break;
            }
        }

        Ok(entry)
    }

    pub async fn remove(&self, key: &str, cache_type: &CacheType) -> bool {
        let mut inner = self.inner.lock().await;
        let len_before = inner.entries.len();
        inner.entries.retain(|e| {
            if e.key == key && e.cache_type == *cache_type {
                let _ = std::fs::remove_file(&e.path);
                false
            } else {
                true
            }
        });
        inner.entries.len() < len_before
    }

    pub async fn contains(&self, key: &str, cache_type: &CacheType) -> bool {
        let inner = self.inner.lock().await;
        inner
            .entries
            .iter()
            .any(|e| e.key == key && e.cache_type == *cache_type && e.path.exists())
    }

    pub async fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().await;
        let mut entries_by_type = HashMap::new();
        let mut size_by_type = HashMap::new();

        for entry in &inner.entries {
            *entries_by_type
                .entry(entry.cache_type.as_str().to_string())
                .or_insert(0) += 1;
            *size_by_type
                .entry(entry.cache_type.as_str().to_string())
                .or_insert(0) += entry.size_bytes;
        }

        CacheStats {
            total_entries: inner.entries.len(),
            total_size_bytes: inner.total_size(),
            max_size_bytes: inner.max_size,
            entries_by_type,
            size_by_type,
            hit_count: inner.hits,
            miss_count: inner.misses,
        }
    }

    pub async fn clear(&self) {
        let mut inner = self.inner.lock().await;
        for entry in &inner.entries {
            let _ = std::fs::remove_file(&entry.path);
        }
        inner.entries.clear();
    }

    pub async fn list_by_type(&self, cache_type: &CacheType) -> Vec<CacheEntry> {
        let inner = self.inner.lock().await;
        inner
            .entries
            .iter()
            .filter(|e| e.cache_type == *cache_type)
            .cloned()
            .collect()
    }

    async fn evict_one(&self, inner: &mut CacheInner) -> bool {
        let idx = inner
            .entries
            .iter()
            .enumerate()
            .min_by_key(|(_, e)| (e.access_count, e.last_access.clone()))
            .map(|(i, _)| i);

        if let Some(idx) = idx {
            let entry = &inner.entries[idx];
            let _ = std::fs::remove_file(&entry.path);
            inner.entries.remove(idx);
            true
        } else {
            false
        }
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new(
            &PathBuf::from("/var/cache/netguardian"),
            20 * 1024 * 1024 * 1024,
        )
    }
}

impl CacheInner {
    fn total_size(&self) -> u64 {
        self.entries.iter().map(|e| e.size_bytes).sum()
    }
}

fn sanitize_filename(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        + ".cache"
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_put_and_get() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 1024 * 1024);

        let mut meta = HashMap::new();
        meta.insert("url".into(), "https://example.com/file".into());

        let entry = cm
            .put("test-key", &CacheType::Generic, b"hello world", meta)
            .await
            .unwrap();

        assert_eq!(entry.size_bytes, 11);
        assert!(entry.path.exists());

        let cached = cm.get("test-key", &CacheType::Generic).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().key, "test-key");
    }

    #[tokio::test]
    async fn test_get_miss() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 1024 * 1024);

        let cached = cm.get("nonexistent", &CacheType::Generic).await;
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_contains() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 1024 * 1024);

        cm.put("key1", &CacheType::Generic, b"data", HashMap::new())
            .await
            .unwrap();

        assert!(cm.contains("key1", &CacheType::Generic).await);
        assert!(!cm.contains("key2", &CacheType::Generic).await);
    }

    #[tokio::test]
    async fn test_remove() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 1024 * 1024);

        cm.put("to-remove", &CacheType::Generic, b"data", HashMap::new())
            .await
            .unwrap();

        assert!(cm.remove("to-remove", &CacheType::Generic).await);
        assert!(!cm.contains("to-remove", &CacheType::Generic).await);
    }

    #[tokio::test]
    async fn test_stats() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 1024 * 1024);

        cm.put("a", &CacheType::GitObjects, b"aaa", HashMap::new())
            .await
            .unwrap();
        cm.put("b", &CacheType::OciLayers, b"bb", HashMap::new())
            .await
            .unwrap();
        cm.put("c", &CacheType::PipWheels, b"c", HashMap::new())
            .await
            .unwrap();

        let stats = cm.stats().await;
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.total_size_bytes, 6);
        assert_eq!(*stats.entries_by_type.get("git").unwrap(), 1);
        assert_eq!(*stats.entries_by_type.get("oci").unwrap(), 1);
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 10);

        cm.put("big1", &CacheType::Generic, b"1234567890", HashMap::new())
            .await
            .unwrap();
        cm.put("big2", &CacheType::Generic, b"1234567890", HashMap::new())
            .await
            .unwrap();

        let stats = cm.stats().await;
        assert!(stats.total_size_bytes <= stats.max_size_bytes);
    }

    #[tokio::test]
    async fn test_list_by_type() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 1024 * 1024);

        cm.put("g1", &CacheType::GitObjects, b"data", HashMap::new())
            .await
            .unwrap();
        cm.put("g2", &CacheType::GitObjects, b"data", HashMap::new())
            .await
            .unwrap();
        cm.put("o1", &CacheType::OciLayers, b"data", HashMap::new())
            .await
            .unwrap();

        let git_entries = cm.list_by_type(&CacheType::GitObjects).await;
        assert_eq!(git_entries.len(), 2);

        let oci_entries = cm.list_by_type(&CacheType::OciLayers).await;
        assert_eq!(oci_entries.len(), 1);
    }

    #[tokio::test]
    async fn test_hit_miss_tracking() {
        let dir = tempdir().unwrap();
        let cm = CacheManager::new(dir.path(), 1024 * 1024);

        cm.put("hit", &CacheType::Generic, b"data", HashMap::new())
            .await
            .unwrap();

        let _ = cm.get("hit", &CacheType::Generic).await;
        let _ = cm.get("miss", &CacheType::Generic).await;

        let stats = cm.stats().await;
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
    }
}
