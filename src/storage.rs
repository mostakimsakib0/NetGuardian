use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::job::{Job, JobPriority, JobStatus};

pub struct JobStore {
    path: PathBuf,
    jobs: HashMap<u64, Job>,
    dirty: bool,
}

impl JobStore {
    pub fn new(path: &Path) -> Self {
        let jobs = if path.exists() {
            Self::load_from_disk(path).unwrap_or_default()
        } else {
            HashMap::new()
        };

        Self {
            path: path.to_path_buf(),
            jobs,
            dirty: false,
        }
    }

    pub fn save(&mut self) -> std::io::Result<()> {
        let entries: Vec<&Job> = self.jobs.values().collect();
        let json = serde_json::to_string_pretty(&entries)?;
        std::fs::write(&self.path, json)?;
        self.dirty = false;
        Ok(())
    }

    pub fn save_if_dirty(&mut self) -> std::io::Result<()> {
        if self.dirty {
            self.save()
        } else {
            Ok(())
        }
    }

    pub fn insert(&mut self, job: Job) {
        self.jobs.insert(job.id, job);
        self.dirty = true;
    }

    pub fn get(&self, id: u64) -> Option<&Job> {
        self.jobs.get(&id)
    }

    pub fn get_mut(&mut self, id: u64) -> Option<&mut Job> {
        self.dirty = true;
        self.jobs.get_mut(&id)
    }

    pub fn remove(&mut self, id: u64) -> bool {
        self.dirty = true;
        self.jobs.remove(&id).is_some()
    }

    pub fn all(&self) -> Vec<&Job> {
        self.jobs.values().collect()
    }

    pub fn by_status(&self, status: &JobStatus) -> Vec<&Job> {
        self.jobs.values().filter(|j| j.status == *status).collect()
    }

    pub fn by_plugin(&self, plugin: &str) -> Vec<&Job> {
        self.jobs.values().filter(|j| j.plugin == plugin).collect()
    }

    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    fn load_from_disk(path: &Path) -> std::io::Result<HashMap<u64, Job>> {
        let content = std::fs::read_to_string(path)?;
        let jobs: Vec<Job> = serde_json::from_str(&content)?;
        Ok(jobs.into_iter().map(|j| (j.id, j)).collect())
    }
}

// ── Config store ──

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AppConfig {
    pub daemon: DaemonConfig,
    pub network: NetworkConfig,
    pub retry: RetryConfig,
    pub plugins: Vec<PluginConfig>,
    pub storage: StorageConfig,
    pub default_max_retries: u32,
    pub active_download_limit: usize,
    pub log_level: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            daemon: DaemonConfig::default(),
            network: NetworkConfig::default(),
            retry: RetryConfig::default(),
            plugins: Vec::new(),
            storage: StorageConfig::default(),
            default_max_retries: 3,
            active_download_limit: 3,
            log_level: "info".into(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DaemonConfig {
    pub enabled: bool,
    pub pid_file: String,
    pub socket_path: String,
    pub monitor_interval_secs: u64,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            pid_file: "/var/run/netguardian.pid".into(),
            socket_path: "/var/run/netguardian.sock".into(),
            monitor_interval_secs: 5,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetworkConfig {
    pub ping_targets: Vec<String>,
    pub dns_servers: Vec<String>,
    pub latency_threshold_ms: f64,
    pub packet_loss_threshold_pct: f64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            ping_targets: vec!["1.1.1.1".into(), "8.8.8.8".into()],
            dns_servers: vec!["1.1.1.1".into(), "8.8.8.8".into()],
            latency_threshold_ms: 200.0,
            packet_loss_threshold_pct: 10.0,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryConfig {
    pub policy: String,
    pub max_retries: u32,
    pub base_delay_secs: u64,
    pub multiplier: f64,
    pub max_delay_secs: u64,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_cooldown_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            policy: "exponential".into(),
            max_retries: 5,
            base_delay_secs: 1,
            multiplier: 2.0,
            max_delay_secs: 60,
            circuit_breaker_threshold: 5,
            circuit_breaker_cooldown_secs: 30,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PluginConfig {
    pub name: String,
    pub enabled: bool,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageConfig {
    pub jobs_path: String,
    pub config_path: String,
    pub cache_dir: String,
    pub download_dir: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            jobs_path: "/var/lib/netguardian/jobs.json".into(),
            config_path: "/etc/netguardian/config.json".into(),
            cache_dir: "/var/cache/netguardian".into(),
            download_dir: "/tmp/netguardian-downloads".into(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Self {
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(path) {
                if let Ok(config) = serde_json::from_str(&content) {
                    return config;
                }
            }
        }
        let config = AppConfig::default();
        let _ = config.save(path);
        config
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)
    }
}

// ── Event Log ──

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub level: String,
    pub module: String,
    pub message: String,
}

pub struct EventLog {
    entries: Vec<LogEntry>,
    max_entries: usize,
}

impl EventLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Vec::with_capacity(max_entries),
            max_entries,
        }
    }

    pub fn record(&mut self, level: &str, module: &str, message: &str) {
        let entry = LogEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            level: level.to_string(),
            module: module.to_string(),
            message: message.to_string(),
        };

        if self.entries.len() >= self.max_entries {
            self.entries.remove(0);
        }
        self.entries.push(entry);
    }

    pub fn info(&mut self, module: &str, message: &str) {
        self.record("INFO", module, message);
    }

    pub fn warn(&mut self, module: &str, message: &str) {
        self.record("WARN", module, message);
    }

    pub fn error(&mut self, module: &str, message: &str) {
        self.record("ERROR", module, message);
    }

    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    pub fn recent(&self, count: usize) -> Vec<&LogEntry> {
        let start = self.entries.len().saturating_sub(count);
        self.entries[start..].iter().collect()
    }

    pub fn by_module(&self, module: &str) -> Vec<&LogEntry> {
        self.entries.iter().filter(|e| e.module == module).collect()
    }
}
