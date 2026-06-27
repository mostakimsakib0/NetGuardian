use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "netguardian")]
#[command(about = "Intelligent Network Resilience Middleware for Linux", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Show current network status
    Status,

    /// Continuously monitor network health
    Monitor {
        /// Interval in seconds between checks
        #[arg(short, long, default_value_t = 5)]
        interval: u64,
    },

    /// Show queued operations
    Queue,

    /// List loaded plugins
    Plugins,

    /// Manage community plugins
    Community {
        #[command(subcommand)]
        command: CommunityCommands,
    },

    /// Run diagnostics
    Doctor,

    /// Show network metrics (use --format prometheus for Prometheus output)
    Metrics {
        /// Output format: json or prometheus
        #[arg(short, long, default_value_t = String::from("json"))]
        format: String,
    },

    /// View logs
    Logs,

    /// Start the NetGuardian daemon
    Daemon,

    /// Manage running jobs
    Job {
        #[command(subcommand)]
        command: JobCommands,
    },

    /// Serve Prometheus metrics over HTTP
    MetricsServe {
        /// Listen address (e.g. 0.0.0.0:9090)
        #[arg(short, long, default_value_t = String::from("127.0.0.1:9090"))]
        listen: String,
    },
}

#[derive(Subcommand)]
pub enum JobCommands {
    /// List all jobs
    List,
    /// Pause a running job
    Pause { id: u64 },
    /// Resume a paused job
    Resume { id: u64 },
    /// Cancel a running or paused job
    Cancel { id: u64 },
    /// Show job details
    Info { id: u64 },
}

#[derive(Subcommand)]
pub enum CommunityCommands {
    /// Search the community plugin index
    Search {
        query: String,
    },
    /// Install a community plugin
    Install {
        name: String,
    },
    /// List installed community plugins
    List,
    /// Remove an installed community plugin
    Remove {
        name: String,
    },
    /// Show detailed info about a community plugin
    Info {
        name: String,
    },
    /// Refresh the community plugin index from remote
    Refresh,
}
