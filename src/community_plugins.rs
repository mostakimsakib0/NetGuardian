use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use serde::{Serialize, Deserialize};

use crate::plugin_manager::{
    Plugin, CliPlugin, PluginResult, PluginError, PluginState, PluginHealth,
    CommandOutput, execute_command, PluginManager,
};

const DEFAULT_INDEX_URL: &str = "https://raw.githubusercontent.com/netguardian/plugins/main/index.json";
const STORAGE_FILE: &str = "community_plugins.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityPluginManifest {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub repository: Option<String>,
    pub license: Option<String>,
    pub tags: Vec<String>,
    pub binary: String,
    pub default_args: Vec<String>,
    pub supports_resume: bool,
    pub homepage: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledPluginEntry {
    pub manifest: CommunityPluginManifest,
    pub installed_at: String,
}

pub struct CommunityPlugin {
    manifest: CommunityPluginManifest,
    state: PluginState,
}

impl CommunityPlugin {
    pub fn new(manifest: CommunityPluginManifest) -> Self {
        Self {
            manifest,
            state: PluginState::Loaded,
        }
    }
}

impl Plugin for CommunityPlugin {
    fn name(&self) -> &str {
        &self.manifest.name
    }

    fn version(&self) -> &str {
        &self.manifest.version
    }

    fn description(&self) -> &str {
        &self.manifest.description
    }

    fn initialize(&mut self) -> PluginResult<()> {
        self.state = PluginState::Loaded;
        Ok(())
    }

    fn start(&mut self) -> PluginResult<()> {
        match self.state {
            PluginState::Loaded | PluginState::Paused => {
                self.state = PluginState::Running;
                Ok(())
            }
            PluginState::Running => Err(PluginError::AlreadyRunning),
            _ => Err(PluginError::ExecutionFailed("cannot start".into())),
        }
    }

    fn pause(&mut self) -> PluginResult<()> {
        if self.state == PluginState::Running {
            self.state = PluginState::Paused;
            Ok(())
        } else {
            Err(PluginError::NotRunning)
        }
    }

    fn resume(&mut self) -> PluginResult<()> {
        if self.state == PluginState::Paused {
            self.state = PluginState::Running;
            Ok(())
        } else {
            Err(PluginError::NotRunning)
        }
    }

    fn retry(&mut self) -> PluginResult<()> {
        if self.state == PluginState::Running {
            Ok(())
        } else {
            Err(PluginError::NotRunning)
        }
    }

    fn cancel(&mut self) -> PluginResult<()> {
        self.state = PluginState::Loaded;
        Ok(())
    }

    fn health_check(&self) -> PluginResult<PluginHealth> {
        Ok(PluginHealth {
            healthy: true,
            state: self.state.clone(),
            message: format!("{} plugin operational", self.manifest.name),
        })
    }

    fn shutdown(&mut self) -> PluginResult<()> {
        self.state = PluginState::Shutdown;
        Ok(())
    }
}

impl CliPlugin for CommunityPlugin {
    fn binary(&self) -> &str {
        &self.manifest.binary
    }

    fn default_args(&self) -> &[&str] {
        &[]
    }

    fn supports_resume(&self) -> bool {
        self.manifest.supports_resume
    }

    fn run(&self, args: &[&str], timeout: Duration) -> PluginResult<CommandOutput> {
        let all_args: Vec<&str> = self.manifest.default_args.iter()
            .map(|s| s.as_str())
            .chain(args.iter().copied())
            .collect();
        execute_command(self.binary(), &all_args, timeout)
    }
}

pub struct CommunityPluginRegistry {
    index: Vec<CommunityPluginManifest>,
    installed: HashMap<String, InstalledPluginEntry>,
    index_url: String,
    storage_path: PathBuf,
}

impl CommunityPluginRegistry {
    pub fn new() -> Self {
        Self {
            index: builtin_index(),
            installed: HashMap::new(),
            index_url: DEFAULT_INDEX_URL.into(),
            storage_path: PathBuf::from(STORAGE_FILE),
        }
    }

    pub fn with_storage_path<P: AsRef<Path>>(path: P) -> Self {
        let storage_path = path.as_ref().to_path_buf();
        let installed = Self::load_installed(&storage_path);
        Self {
            index: builtin_index(),
            installed,
            index_url: DEFAULT_INDEX_URL.into(),
            storage_path,
        }
    }

    fn load_installed(path: &Path) -> HashMap<String, InstalledPluginEntry> {
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(path) {
                if let Ok(entries) = serde_json::from_str::<Vec<InstalledPluginEntry>>(&content) {
                    return entries.into_iter().map(|e| (e.manifest.name.clone(), e)).collect();
                }
            }
        }
        HashMap::new()
    }

    fn save_installed(&self) {
        let entries: Vec<&InstalledPluginEntry> = self.installed.values().collect();
        if let Ok(content) = serde_json::to_string_pretty(&entries) {
            let _ = std::fs::write(&self.storage_path, content);
        }
    }

    pub fn search(&self, query: &str) -> Vec<&CommunityPluginManifest> {
        let q = query.to_lowercase();
        self.index.iter()
            .filter(|m| {
                m.name.to_lowercase().contains(&q)
                    || m.description.to_lowercase().contains(&q)
                    || m.tags.iter().any(|t| t.to_lowercase().contains(&q))
                    || m.author.to_lowercase().contains(&q)
            })
            .collect()
    }

    pub fn install(&mut self, name: &str) -> Result<CommunityPluginManifest, String> {
        if self.installed.contains_key(name) {
            return Err(format!("plugin '{}' is already installed", name));
        }
        let manifest = self.index.iter()
            .find(|m| m.name == name)
            .cloned()
            .ok_or_else(|| format!("plugin '{}' not found in community index", name))?;
        let entry = InstalledPluginEntry {
            manifest: manifest.clone(),
            installed_at: chrono::Utc::now().to_rfc3339(),
        };
        self.installed.insert(name.into(), entry);
        self.save_installed();
        Ok(manifest)
    }

    pub fn remove(&mut self, name: &str) -> bool {
        let removed = self.installed.remove(name).is_some();
        if removed {
            self.save_installed();
        }
        removed
    }

    pub fn list_installed(&self) -> Vec<&InstalledPluginEntry> {
        let mut entries: Vec<&InstalledPluginEntry> = self.installed.values().collect();
        entries.sort_by_key(|e| e.manifest.name.clone());
        entries
    }

    pub fn list_available(&self) -> Vec<&CommunityPluginManifest> {
        let mut available: Vec<&CommunityPluginManifest> = self.index.iter()
            .filter(|m| !self.installed.contains_key(&m.name))
            .collect();
        available.sort_by_key(|m| m.name.clone());
        available
    }

    pub fn info(&self, name: &str) -> Option<&CommunityPluginManifest> {
        self.index.iter().find(|m| m.name == name)
    }

    pub fn is_installed(&self, name: &str) -> bool {
        self.installed.contains_key(name)
    }

    pub async fn refresh_index(&mut self) -> Result<usize, String> {
        match Self::fetch_remote_index(&self.index_url).await {
            Ok(plugins) if !plugins.is_empty() => {
                self.index = plugins;
                Ok(self.index.len())
            }
            Ok(_) => Err("remote index is empty, keeping builtin index".into()),
            Err(e) => Err(format!("failed to fetch remote index: {}", e)),
        }
    }

    async fn fetch_remote_index(url: &str) -> Result<Vec<CommunityPluginManifest>, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("failed to create http client: {}", e))?;
        let resp = client.get(url)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        let manifests: Vec<CommunityPluginManifest> = resp
            .json()
            .await
            .map_err(|e| format!("failed to parse index: {}", e))?;
        Ok(manifests)
    }

    pub fn register_with(&self, pm: &mut PluginManager) {
        for entry in self.installed.values() {
            let plugin = CommunityPlugin::new(entry.manifest.clone());
            pm.register_named(&entry.manifest.name, Arc::new(Mutex::new(plugin)));
        }
    }
}

impl Default for CommunityPluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn builtin_index() -> Vec<CommunityPluginManifest> {
    vec![
        CommunityPluginManifest {
            name: "rsync".into(),
            version: "1.0.0".into(),
            description: "Fast file synchronization over SSH with retry and resume support".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://github.com/rsync/rsync".into()),
            license: Some("GPL-3.0".into()),
            tags: vec!["sync".into(), "backup".into(), "ssh".into(), "transfer".into()],
            binary: "rsync".into(),
            default_args: vec!["-avz".into(), "--progress".into(), "--partial".into()],
            supports_resume: true,
            homepage: Some("https://rsync.samba.org".into()),
        },
        CommunityPluginManifest {
            name: "docker".into(),
            version: "1.0.0".into(),
            description: "Container image pull, push and management with retry support".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://github.com/docker/cli".into()),
            license: Some("Apache-2.0".into()),
            tags: vec!["container".into(), "docker".into(), "images".into(), "deploy".into()],
            binary: "docker".into(),
            default_args: vec!["--log-level".into(), "warn".into()],
            supports_resume: false,
            homepage: Some("https://docker.com".into()),
        },
        CommunityPluginManifest {
            name: "pip".into(),
            version: "1.0.0".into(),
            description: "Python package installer with retry for unreliable networks".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://github.com/pypa/pip".into()),
            license: Some("MIT".into()),
            tags: vec!["python".into(), "package".into(), "pypi".into(), "install".into()],
            binary: "pip3".into(),
            default_args: vec!["--timeout".into(), "30".into()],
            supports_resume: false,
            homepage: Some("https://pip.pypa.io".into()),
        },
        CommunityPluginManifest {
            name: "npm".into(),
            version: "1.0.0".into(),
            description: "Node.js package manager with retry and fetch resilience".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://github.com/npm/cli".into()),
            license: Some("BlueOak-1.0.0".into()),
            tags: vec!["node".into(), "javascript".into(), "package".into(), "install".into()],
            binary: "npm".into(),
            default_args: vec!["--loglevel".into(), "warn".into()],
            supports_resume: false,
            homepage: Some("https://npmjs.com".into()),
        },
        CommunityPluginManifest {
            name: "apt".into(),
            version: "1.0.0".into(),
            description: "Debian package manager with smart retry for apt-get operations".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://salsa.debian.org/apt-team/apt".into()),
            license: Some("GPL-2.0".into()),
            tags: vec!["debian".into(), "package".into(), "apt".into(), "install".into()],
            binary: "apt-get".into(),
            default_args: vec!["--option".into(), "Acquire::Retries=3".into()],
            supports_resume: false,
            homepage: None,
        },
        CommunityPluginManifest {
            name: "ffmpeg".into(),
            version: "1.0.0".into(),
            description: "Media processing with download resume for streaming sources".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://github.com/FFmpeg/FFmpeg".into()),
            license: Some("LGPL-2.1".into()),
            tags: vec!["media".into(), "video".into(), "audio".into(), "convert".into()],
            binary: "ffmpeg".into(),
            default_args: vec!["-y".into(), "-stats".into()],
            supports_resume: true,
            homepage: Some("https://ffmpeg.org".into()),
        },
        CommunityPluginManifest {
            name: "ssh".into(),
            version: "1.0.0".into(),
            description: "Secure Shell connections with automatic retry and fallback".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://github.com/openssh/openssh-portable".into()),
            license: Some("BSD-2-Clause".into()),
            tags: vec!["ssh".into(), "remote".into(), "shell".into(), "tunnel".into()],
            binary: "ssh".into(),
            default_args: vec!["-o".into(), "ConnectTimeout=10".into(), "-o".into(), "ServerAliveInterval=5".into()],
            supports_resume: false,
            homepage: Some("https://openssh.com".into()),
        },
        CommunityPluginManifest {
            name: "ansible".into(),
            version: "1.0.0".into(),
            description: "IT automation with resilient playbook execution and retry".into(),
            author: "NetGuardian Community".into(),
            repository: Some("https://github.com/ansible/ansible".into()),
            license: Some("GPL-3.0".into()),
            tags: vec!["automation".into(), "devops".into(), "config".into(), "deploy".into()],
            binary: "ansible-playbook".into(),
            default_args: vec!["--retry-files-enabled".into(), "no".into()],
            supports_resume: false,
            homepage: Some("https://ansible.com".into()),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_index_non_empty() {
        let idx = builtin_index();
        assert!(!idx.is_empty());
        assert!(idx.len() >= 8);
    }

    #[test]
    fn test_search_by_name() {
        let registry = CommunityPluginRegistry::new();
        let results = registry.search("rsync");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "rsync");
    }

    #[test]
    fn test_search_by_tag() {
        let registry = CommunityPluginRegistry::new();
        let results = registry.search("container");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "docker");
    }

    #[test]
    fn test_search_partial() {
        let registry = CommunityPluginRegistry::new();
        let results = registry.search("python");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "pip");
    }

    #[test]
    fn test_search_case_insensitive() {
        let registry = CommunityPluginRegistry::new();
        let results = registry.search("RSYNC");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "rsync");
    }

    #[test]
    fn test_search_multi_match() {
        let registry = CommunityPluginRegistry::new();
        let results = registry.search("package");
        let names: Vec<&str> = results.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"pip"));
        assert!(names.contains(&"npm"));
        assert!(names.contains(&"apt"));
    }

    #[test]
    fn test_install_and_list_installed() {
        let mut registry = CommunityPluginRegistry::new();
        assert!(!registry.is_installed("rsync"));

        let result = registry.install("rsync");
        assert!(result.is_ok());
        assert!(registry.is_installed("rsync"));

        let installed = registry.list_installed();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].manifest.name, "rsync");
    }

    #[test]
    fn test_install_duplicate_fails() {
        let mut registry = CommunityPluginRegistry::new();
        registry.install("rsync").unwrap();
        let result = registry.install("rsync");
        assert!(result.is_err());
    }

    #[test]
    fn test_install_nonexistent_fails() {
        let mut registry = CommunityPluginRegistry::new();
        let result = registry.install("nonexistent-plugin");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_installed() {
        let mut registry = CommunityPluginRegistry::new();
        registry.install("docker").unwrap();
        assert!(registry.is_installed("docker"));

        assert!(registry.remove("docker"));
        assert!(!registry.is_installed("docker"));
    }

    #[test]
    fn test_remove_not_installed() {
        let mut registry = CommunityPluginRegistry::new();
        assert!(!registry.remove("nonexistent"));
    }

    #[test]
    fn test_list_available_excludes_installed() {
        let mut registry = CommunityPluginRegistry::new();
        let before = registry.list_available().len();

        registry.install("rsync").unwrap();
        let after = registry.list_available().len();

        assert_eq!(after, before - 1);
        let names: Vec<&str> = registry.list_available().iter().map(|m| m.name.as_str()).collect();
        assert!(!names.contains(&"rsync"));
    }

    #[test]
    fn test_info() {
        let registry = CommunityPluginRegistry::new();
        let info = registry.info("ssh").unwrap();
        assert_eq!(info.author, "NetGuardian Community");
        assert_eq!(info.binary, "ssh");
        assert!(!info.supports_resume);
    }

    #[test]
    fn test_info_nonexistent() {
        let registry = CommunityPluginRegistry::new();
        assert!(registry.info("void").is_none());
    }

    #[test]
    fn test_community_plugin_lifecycle() {
        let manifest = CommunityPluginManifest {
            name: "test-plugin".into(),
            version: "1.0.0".into(),
            description: "test".into(),
            author: "test".into(),
            repository: None,
            license: None,
            tags: vec![],
            binary: "echo".into(),
            default_args: vec!["hello".into()],
            supports_resume: false,
            homepage: None,
        };

        let mut plugin = CommunityPlugin::new(manifest);
        assert_eq!(plugin.name(), "test-plugin");
        assert_eq!(plugin.version(), "1.0.0");

        assert!(plugin.initialize().is_ok());
        assert!(plugin.start().is_ok());
        assert!(plugin.health_check().unwrap().healthy);

        assert!(plugin.pause().is_ok());
        assert!(plugin.resume().is_ok());

        assert!(plugin.retry().is_ok());
        assert!(plugin.cancel().is_ok());
        assert!(plugin.shutdown().is_ok());
    }

    #[test]
    fn test_community_plugin_start_twice_fails() {
        let manifest = CommunityPluginManifest {
            name: "test".into(),
            version: "1.0.0".into(),
            description: "test".into(),
            author: "test".into(),
            repository: None,
            license: None,
            tags: vec![],
            binary: "echo".into(),
            default_args: vec![],
            supports_resume: false,
            homepage: None,
        };
        let mut plugin = CommunityPlugin::new(manifest);
        plugin.initialize().unwrap();
        plugin.start().unwrap();
        let result = plugin.start();
        assert!(matches!(result, Err(PluginError::AlreadyRunning)));
    }

    #[test]
    fn test_community_plugin_cli_trait() {
        let manifest = CommunityPluginManifest {
            name: "echo".into(),
            version: "1.0.0".into(),
            description: "test".into(),
            author: "test".into(),
            repository: None,
            license: None,
            tags: vec![],
            binary: "echo".into(),
            default_args: vec!["-n".into(), "hello".into()],
            supports_resume: true,
            homepage: None,
        };
        let plugin = CommunityPlugin::new(manifest);
        assert_eq!(plugin.binary(), "echo");
        assert!(plugin.supports_resume());
    }

    #[tokio::test]
    async fn test_register_community_plugin_with_manager() {
        let manifest = CommunityPluginManifest {
            name: "custom".into(),
            version: "0.1.0".into(),
            description: "custom community plugin".into(),
            author: "dev".into(),
            repository: None,
            license: None,
            tags: vec![],
            binary: "ls".into(),
            default_args: vec![],
            supports_resume: false,
            homepage: None,
        };
        let mut pm = PluginManager::new();
        pm.register_named("custom", Arc::new(Mutex::new(CommunityPlugin::new(manifest))));
        assert_eq!(pm.count(), 1);

        let plugins = pm.list().await;
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].name, "custom");
    }

    #[tokio::test]
    async fn test_registered_community_plugin_lifecycle() {
        let manifest = CommunityPluginManifest {
            name: "test-cp".into(),
            version: "1.0.0".into(),
            description: "lifecycle test".into(),
            author: "test".into(),
            repository: None,
            license: None,
            tags: vec![],
            binary: "true".into(),
            default_args: vec![],
            supports_resume: false,
            homepage: None,
        };
        let mut pm = PluginManager::new();
        pm.register_named("test-cp", Arc::new(Mutex::new(CommunityPlugin::new(manifest))));

        let results = pm.initialize_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let results = pm.start_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let p = pm.get("test-cp").await.unwrap();
        let locked = p.lock().await;
        let health = locked.health_check().unwrap();
        assert_eq!(health.state, PluginState::Running);
    }

    #[tokio::test]
    async fn test_register_installed_with_manager() {
        let dir = tempfile::tempdir().unwrap();
        let storage = dir.path().join("test_installed.json");
        {
            let mut registry = CommunityPluginRegistry::with_storage_path(&storage);
            registry.install("rsync").unwrap();
            registry.install("ssh").unwrap();
        }

        let registry = CommunityPluginRegistry::with_storage_path(&storage);
        assert_eq!(registry.list_installed().len(), 2);

        let mut pm = PluginManager::new();
        registry.register_with(&mut pm);
        assert_eq!(pm.count(), 2);

        let plugins = pm.list().await;
        let names: Vec<&str> = plugins.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"rsync"));
        assert!(names.contains(&"ssh"));
    }

    #[tokio::test]
    async fn test_storage_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let storage = dir.path().join("plugins.json");

        {
            let mut registry = CommunityPluginRegistry::with_storage_path(&storage);
            registry.install("docker").unwrap();
            registry.install("pip").unwrap();
        }

        {
            let registry = CommunityPluginRegistry::with_storage_path(&storage);
            assert_eq!(registry.list_installed().len(), 2);
            assert!(registry.is_installed("docker"));
            assert!(registry.is_installed("pip"));
        }
    }

    #[test]
    fn test_manifest_serialization_roundtrip() {
        let manifest = CommunityPluginManifest {
            name: "test".into(),
            version: "1.0.0".into(),
            description: "serde test".into(),
            author: "author".into(),
            repository: Some("https://example.com/repo".into()),
            license: Some("MIT".into()),
            tags: vec!["tag1".into(), "tag2".into()],
            binary: "test-bin".into(),
            default_args: vec!["--flag".into(), "value".into()],
            supports_resume: true,
            homepage: Some("https://example.com".into()),
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let deserialized: CommunityPluginManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "test");
        assert_eq!(deserialized.version, "1.0.0");
        assert_eq!(deserialized.tags, vec!["tag1", "tag2"]);
        assert!(deserialized.supports_resume);
    }
}
