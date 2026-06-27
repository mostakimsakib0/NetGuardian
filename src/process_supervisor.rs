use std::collections::HashMap;
use std::fmt;
use std::io;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    Running,
    Stopped,
    Exited(i32),
    Killed,
}

pub struct ManagedProcess {
    pub pid: u32,
    pub binary: String,
    pub args: Vec<String>,
    child: Arc<StdMutex<Option<Child>>>,
    status: Arc<StdMutex<ProcessStatus>>,
}

impl fmt::Debug for ManagedProcess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedProcess")
            .field("pid", &self.pid)
            .field("binary", &self.binary)
            .field("args", &self.args)
            .field("status", &self.status)
            .finish()
    }
}

impl ManagedProcess {
    pub fn spawn(binary: &str, args: &[String]) -> io::Result<Self> {
        let child = Command::new(binary)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null())
            .spawn()?;

        let pid = child.id();
        let child = Arc::new(StdMutex::new(Some(child)));

        Ok(Self {
            pid,
            binary: binary.to_string(),
            args: args.to_vec(),
            child,
            status: Arc::new(StdMutex::new(ProcessStatus::Running)),
        })
    }

    pub fn spawn_with_pgroup(binary: &str, args: &[String]) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let mut cmd = Command::new(binary);
            cmd.args(args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .stdin(Stdio::null());

            unsafe {
                cmd.pre_exec(|| {
                    libc::setpgid(0, 0);
                    Ok(())
                });
            }

            let child = cmd.spawn()?;
            let pid = child.id();
            let child = Arc::new(StdMutex::new(Some(child)));

            return Ok(Self {
                pid,
                binary: binary.to_string(),
                args: args.to_vec(),
                child,
                status: Arc::new(StdMutex::new(ProcessStatus::Running)),
            });
        }

        #[cfg(not(unix))]
        {
            Self::spawn(binary, args)
        }
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn status(&self) -> ProcessStatus {
        *self.status.lock().unwrap()
    }

    pub fn stop(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            let pid = self.pid as i32;
            let result = unsafe { libc::kill(pid, libc::SIGSTOP) };
            if result == 0 {
                *self.status.lock().unwrap() = ProcessStatus::Stopped;
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        #[cfg(not(unix))]
        {
            let _ = self.pid;
            Err(io::Error::new(io::ErrorKind::Unsupported, "SIGSTOP not supported"))
        }
    }

    pub fn cont(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            let pid = self.pid as i32;
            let result = unsafe { libc::kill(pid, libc::SIGCONT) };
            if result == 0 {
                *self.status.lock().unwrap() = ProcessStatus::Running;
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        #[cfg(not(unix))]
        {
            let _ = self.pid;
            Err(io::Error::new(io::ErrorKind::Unsupported, "SIGCONT not supported"))
        }
    }

    pub fn terminate(&self, grace_period: Duration) -> io::Result<()> {
        #[cfg(unix)]
        {
            let pid = self.pid as i32;
            unsafe { libc::kill(pid, libc::SIGTERM) };
            if !grace_period.is_zero() {
                std::thread::sleep(grace_period);
            }
            unsafe { libc::kill(pid, libc::SIGKILL) };
            *self.status.lock().unwrap() = ProcessStatus::Killed;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = self.pid;
            let _ = grace_period;
            Err(io::Error::new(io::ErrorKind::Unsupported, "signals not supported"))
        }
    }

    pub fn try_wait(&self) -> io::Result<Option<i32>> {
        let child_opt = self.child.lock().unwrap();
        if let Some(child) = child_opt.as_ref() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let code = status.code().unwrap_or(-1);
                    *self.status.lock().unwrap() = ProcessStatus::Exited(code);
                    Ok(Some(code))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            Ok(None)
        }
    }

    fn clone_handle(&self) -> Self {
        Self {
            pid: self.pid,
            binary: self.binary.clone(),
            args: self.args.clone(),
            child: self.child.clone(),
            status: self.status.clone(),
        }
    }
}

// ── Process Supervisor ──

pub struct ProcessSupervisor {
    processes: Arc<Mutex<HashMap<String, Vec<ManagedProcess>>>>,
}

impl ProcessSupervisor {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn spawn(&self, plugin: &str, binary: &str, args: &[String]) -> io::Result<ManagedProcess> {
        let proc = ManagedProcess::spawn_with_pgroup(binary, args)?;
        let mut map = self.processes.lock().await;
        map.entry(plugin.to_string()).or_default().push(proc.clone_handle());
        Ok(proc)
    }

    pub async fn stop_all(&self, plugin: &str) -> Vec<io::Result<()>> {
        let map = self.processes.lock().await;
        let mut results = Vec::new();
        if let Some(procs) = map.get(plugin) {
            for proc in procs {
                results.push(proc.stop());
            }
        }
        results
    }

    pub async fn cont_all(&self, plugin: &str) -> Vec<io::Result<()>> {
        let map = self.processes.lock().await;
        let mut results = Vec::new();
        if let Some(procs) = map.get(plugin) {
            for proc in procs {
                results.push(proc.cont());
            }
        }
        results
    }

    pub async fn terminate_all(&self, plugin: &str, grace: Duration) -> Vec<io::Result<()>> {
        let map = self.processes.lock().await;
        let mut results = Vec::new();
        if let Some(procs) = map.get(plugin) {
            for proc in procs {
                results.push(proc.terminate(grace));
            }
        }
        results
    }

    pub async fn cleanup(&self, plugin: &str) {
        let mut map = self.processes.lock().await;
        if let Some(procs) = map.remove(plugin) {
            for proc in &procs {
                let _ = proc.terminate(Duration::from_secs(1));
            }
        }
    }

    pub async fn running_count(&self, plugin: &str) -> usize {
        let map = self.processes.lock().await;
        map.get(plugin).map(|v| v.len()).unwrap_or(0)
    }

    pub async fn all_running(&self) -> Vec<(String, u32)> {
        let map = self.processes.lock().await;
        let mut result = Vec::new();
        for (plugin, procs) in map.iter() {
            for proc in procs {
                if let Ok(Some(_)) = proc.try_wait() {
                    // already exited
                } else {
                    result.push((plugin.clone(), proc.pid));
                }
            }
        }
        result
    }
}

impl Default for ProcessSupervisor {
    fn default() -> Self {
        Self::new()
    }
}
