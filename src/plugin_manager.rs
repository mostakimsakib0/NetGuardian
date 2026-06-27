use std::collections::HashMap;
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PluginState {
    Loaded,
    Running,
    Paused,
    Error(String),
    Shutdown,
}

pub type PluginResult<T> = Result<T, PluginError>;

#[derive(Debug, Clone)]
pub enum PluginError {
    NotInitialized,
    AlreadyRunning,
    NotRunning,
    ExecutionFailed(String),
    UnsupportedOperation(String),
}

pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn description(&self) -> &str;

    fn initialize(&mut self) -> PluginResult<()>;
    fn start(&mut self) -> PluginResult<()>;
    fn pause(&mut self) -> PluginResult<()>;
    fn resume(&mut self) -> PluginResult<()>;
    fn retry(&mut self) -> PluginResult<()>;
    fn cancel(&mut self) -> PluginResult<()>;
    fn health_check(&self) -> PluginResult<PluginHealth>;
    fn shutdown(&mut self) -> PluginResult<()>;
}

pub trait CliPlugin: Plugin {
    fn binary(&self) -> &str;
    fn default_args(&self) -> &[&str];
    fn supports_resume(&self) -> bool;

    fn run(&self, args: &[&str], timeout: Duration) -> PluginResult<CommandOutput>;
}

#[derive(Debug, Clone)]
pub struct PluginHealth {
    pub healthy: bool,
    pub state: PluginState,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct CommandOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

pub fn execute_command(
    binary: &str,
    args: &[&str],
    _timeout: Duration,
) -> PluginResult<CommandOutput> {
    let child = StdCommand::new(binary).args(args).output();

    match child {
        Ok(output) => {
            let exit_code = output.status.code().unwrap_or(-1);
            Ok(CommandOutput {
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                exit_code,
            })
        }
        Err(e) => Err(PluginError::ExecutionFailed(format!(
            "failed to execute {}: {}",
            binary, e
        ))),
    }
}

pub struct GitPlugin {
    name: String,
    state: PluginState,
}

impl GitPlugin {
    pub fn new() -> Self {
        Self {
            name: "git".into(),
            state: PluginState::Loaded,
        }
    }
}

impl Plugin for GitPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn description(&self) -> &str {
        "Git protocol support: clone, pull, fetch with resume capability"
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
            message: "Git plugin operational".into(),
        })
    }

    fn shutdown(&mut self) -> PluginResult<()> {
        self.state = PluginState::Shutdown;
        Ok(())
    }
}

impl CliPlugin for GitPlugin {
    fn binary(&self) -> &str {
        "git"
    }

    fn default_args(&self) -> &[&str] {
        &[]
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn run(&self, args: &[&str], timeout: Duration) -> PluginResult<CommandOutput> {
        execute_command(self.binary(), args, timeout)
    }
}

pub struct CurlPlugin {
    name: String,
    state: PluginState,
}

impl CurlPlugin {
    pub fn new() -> Self {
        Self {
            name: "curl".into(),
            state: PluginState::Loaded,
        }
    }
}

impl Plugin for CurlPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn description(&self) -> &str {
        "cURL downloads with retry and resume support"
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
            message: "Curl plugin operational".into(),
        })
    }

    fn shutdown(&mut self) -> PluginResult<()> {
        self.state = PluginState::Shutdown;
        Ok(())
    }
}

impl CliPlugin for CurlPlugin {
    fn binary(&self) -> &str {
        "curl"
    }

    fn default_args(&self) -> &[&str] {
        &["-L", "--connect-timeout", "10"]
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn run(&self, args: &[&str], timeout: Duration) -> PluginResult<CommandOutput> {
        let all_args: Vec<&str> = self.default_args().iter().chain(args).copied().collect();
        execute_command(self.binary(), &all_args, timeout)
    }
}

pub struct WgetPlugin {
    name: String,
    state: PluginState,
}

impl WgetPlugin {
    pub fn new() -> Self {
        Self {
            name: "wget".into(),
            state: PluginState::Loaded,
        }
    }
}

impl Plugin for WgetPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn description(&self) -> &str {
        "Wget downloads with retry and resume (-c) support"
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
            message: "Wget plugin operational".into(),
        })
    }

    fn shutdown(&mut self) -> PluginResult<()> {
        self.state = PluginState::Shutdown;
        Ok(())
    }
}

impl CliPlugin for WgetPlugin {
    fn binary(&self) -> &str {
        "wget"
    }

    fn default_args(&self) -> &[&str] {
        &["--tries=3", "--timeout=15", "-c"]
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn run(&self, args: &[&str], timeout: Duration) -> PluginResult<CommandOutput> {
        let all_args: Vec<&str> = self.default_args().iter().chain(args).copied().collect();
        execute_command(self.binary(), &all_args, timeout)
    }
}

pub struct PodmanPlugin {
    name: String,
    state: PluginState,
}

impl PodmanPlugin {
    pub fn new() -> Self {
        Self {
            name: "podman".into(),
            state: PluginState::Loaded,
        }
    }
}

impl Plugin for PodmanPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn description(&self) -> &str {
        "Podman container management with pull/push retry support"
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
            message: "Podman plugin operational".into(),
        })
    }

    fn shutdown(&mut self) -> PluginResult<()> {
        self.state = PluginState::Shutdown;
        Ok(())
    }
}

impl CliPlugin for PodmanPlugin {
    fn binary(&self) -> &str {
        "podman"
    }

    fn default_args(&self) -> &[&str] {
        &[]
    }

    fn supports_resume(&self) -> bool {
        false
    }

    fn run(&self, args: &[&str], timeout: Duration) -> PluginResult<CommandOutput> {
        execute_command(self.binary(), args, timeout)
    }
}

pub type BoxedPlugin = Arc<Mutex<dyn Plugin>>;

pub struct PluginManager {
    plugins: HashMap<String, BoxedPlugin>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
        }
    }

    pub async fn register(&mut self, plugin: BoxedPlugin) {
        let name = {
            let locked = plugin.lock().await;
            locked.name().to_string()
        };
        self.plugins.insert(name, plugin);
    }

    pub fn register_named(&mut self, name: &str, plugin: BoxedPlugin) {
        self.plugins.insert(name.to_string(), plugin);
    }

    pub async fn get(&self, name: &str) -> Option<BoxedPlugin> {
        self.plugins.get(name).cloned()
    }

    pub async fn list(&self) -> Vec<PluginInfo> {
        let mut infos = Vec::new();
        for (_, plugin) in &self.plugins {
            let locked = plugin.lock().await;
            let (state, healthy) = locked
                .health_check()
                .ok()
                .map(|h| (h.state, h.healthy))
                .unwrap_or((PluginState::Loaded, false));
            infos.push(PluginInfo {
                name: locked.name().to_string(),
                version: locked.version().to_string(),
                description: locked.description().to_string(),
                state,
                healthy,
            });
        }
        infos
    }

    pub async fn initialize_all(&self) -> Vec<PluginResult<()>> {
        let mut results = Vec::new();
        for (_, plugin) in &self.plugins {
            let mut locked = plugin.lock().await;
            results.push(locked.initialize());
        }
        results
    }

    pub async fn start_all(&self) -> Vec<PluginResult<()>> {
        let mut results = Vec::new();
        for (_, plugin) in &self.plugins {
            let mut locked = plugin.lock().await;
            results.push(locked.start());
        }
        results
    }

    pub async fn pause_all(&self) -> Vec<PluginResult<()>> {
        let mut results = Vec::new();
        for (_, plugin) in &self.plugins {
            let mut locked = plugin.lock().await;
            results.push(locked.pause());
        }
        results
    }

    pub async fn resume_all(&self) -> Vec<PluginResult<()>> {
        let mut results = Vec::new();
        for (_, plugin) in &self.plugins {
            let mut locked = plugin.lock().await;
            results.push(locked.resume());
        }
        results
    }

    pub async fn shutdown_all(&self) -> Vec<PluginResult<()>> {
        let mut results = Vec::new();
        for (_, plugin) in &self.plugins {
            let mut locked = plugin.lock().await;
            results.push(locked.shutdown());
        }
        results
    }

    pub fn count(&self) -> usize {
        self.plugins.len()
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    pub state: PluginState,
    pub healthy: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_list() {
        let mut pm = PluginManager::new();
        pm.register(Arc::new(Mutex::new(GitPlugin::new()))).await;
        pm.register(Arc::new(Mutex::new(CurlPlugin::new()))).await;

        assert_eq!(pm.count(), 2);

        let plugins = pm.list().await;
        assert_eq!(plugins.len(), 2);
        let names: Vec<_> = plugins.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"git"));
        assert!(names.contains(&"curl"));
    }

    #[tokio::test]
    async fn test_plugin_lifecycle() {
        let mut pm = PluginManager::new();
        pm.register(Arc::new(Mutex::new(GitPlugin::new()))).await;

        let results = pm.initialize_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let results = pm.start_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let git = pm.get("git").await.unwrap();
        {
            let locked = git.lock().await;
            let health = locked.health_check().unwrap();
            assert_eq!(health.state, PluginState::Running);
        }

        let results = pm.pause_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        {
            let locked = git.lock().await;
            let health = locked.health_check().unwrap();
            assert_eq!(health.state, PluginState::Paused);
        }

        let results = pm.resume_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let results = pm.shutdown_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        {
            let locked = git.lock().await;
            let health = locked.health_check().unwrap();
            assert_eq!(health.state, PluginState::Shutdown);
        }
    }

    #[tokio::test]
    async fn test_git_plugin_retry() {
        let mut pm = PluginManager::new();
        pm.register(Arc::new(Mutex::new(GitPlugin::new()))).await;

        pm.initialize_all().await;
        pm.start_all().await;

        let git = pm.get("git").await.unwrap();
        let mut locked = git.lock().await;
        assert!(locked.retry().is_ok());
    }

    #[tokio::test]
    async fn test_start_twice_fails() {
        let mut pm = PluginManager::new();
        pm.register(Arc::new(Mutex::new(GitPlugin::new()))).await;
        pm.initialize_all().await;

        let results = pm.start_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let results = pm.start_all().await;
        assert!(results.iter().any(|r| matches!(r, Err(PluginError::AlreadyRunning))));
    }

    #[tokio::test]
    async fn test_register_wget_podman() {
        let mut pm = PluginManager::new();
        pm.register(Arc::new(Mutex::new(WgetPlugin::new()))).await;
        pm.register(Arc::new(Mutex::new(PodmanPlugin::new()))).await;

        assert_eq!(pm.count(), 2);

        let plugins = pm.list().await;
        let names: Vec<_> = plugins.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"wget"));
        assert!(names.contains(&"podman"));
    }

    #[tokio::test]
    async fn test_wget_plugin_lifecycle() {
        let mut pm = PluginManager::new();
        pm.register(Arc::new(Mutex::new(WgetPlugin::new()))).await;

        pm.initialize_all().await;
        let results = pm.start_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let wget = pm.get("wget").await.unwrap();
        let locked = wget.lock().await;
        let health = locked.health_check().unwrap();
        assert_eq!(health.state, PluginState::Running);
    }

    #[tokio::test]
    async fn test_podman_plugin_lifecycle() {
        let mut pm = PluginManager::new();
        pm.register(Arc::new(Mutex::new(PodmanPlugin::new()))).await;

        pm.initialize_all().await;
        let results = pm.start_all().await;
        assert!(results.iter().all(|r| r.is_ok()));

        let podman = pm.get("podman").await.unwrap();
        let locked = podman.lock().await;
        let health = locked.health_check().unwrap();
        assert_eq!(health.state, PluginState::Running);
    }

    #[tokio::test]
    async fn test_cli_plugin_trait() {
        let git = GitPlugin::new();
        assert_eq!(git.binary(), "git");
        assert!(git.supports_resume());

        let curl = CurlPlugin::new();
        assert_eq!(curl.binary(), "curl");
        assert!(curl.supports_resume());

        let wget = WgetPlugin::new();
        assert_eq!(wget.binary(), "wget");
        assert!(wget.supports_resume());

        let podman = PodmanPlugin::new();
        assert_eq!(podman.binary(), "podman");
        assert!(!podman.supports_resume());
    }
}
