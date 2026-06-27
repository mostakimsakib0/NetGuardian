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

    /// Show network metrics
    Metrics,

    /// View logs
    Logs,

    /// Start the NetGuardian daemon
    Daemon,
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
