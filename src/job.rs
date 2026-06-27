use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use chrono::{DateTime, Utc};

static NEXT_JOB_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub enum JobPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl JobPriority {
    pub fn as_u8(&self) -> u8 {
        match self {
            JobPriority::Low => 0,
            JobPriority::Normal => 1,
            JobPriority::High => 2,
            JobPriority::Critical => 3,
        }
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => JobPriority::Low,
            2 => JobPriority::High,
            3 => JobPriority::Critical,
            _ => JobPriority::Normal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JobStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Job {
    pub id: u64,
    pub plugin: String,
    pub description: String,
    pub command: String,
    pub args: Vec<String>,
    pub priority: JobPriority,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub progress: f64,
    pub error: Option<String>,
    pub owner: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl Job {
    pub fn new(
        plugin: &str,
        description: &str,
        command: &str,
        args: Vec<String>,
        priority: JobPriority,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: NEXT_JOB_ID.fetch_add(1, Ordering::SeqCst),
            plugin: plugin.to_string(),
            description: description.to_string(),
            command: command.to_string(),
            args,
            priority,
            status: JobStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: 3,
            progress: 0.0,
            error: None,
            owner: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_owner(mut self, owner: &str) -> Self {
        self.owner = Some(owner.to_string());
        self
    }

    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = max;
        self
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.status, JobStatus::Completed | JobStatus::Failed(_) | JobStatus::Cancelled)
    }

    pub fn can_transition(&self, new: &JobStatus) -> bool {
        use JobStatus::*;
        matches!((&self.status, new),
            (Pending, Running | Cancelled) |
            (Running, Paused | Completed | Failed(_) | Cancelled) |
            (Paused, Running | Cancelled)
        )
    }

    pub fn set_status(&mut self, status: JobStatus) -> Result<(), String> {
        if !self.can_transition(&status) {
            return Err(format!("cannot transition from {:?} to {:?}", self.status, status));
        }
        self.status = status;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn set_progress(&mut self, pct: f64) {
        self.progress = pct.clamp(0.0, 100.0);
        self.updated_at = Utc::now();
    }
}
