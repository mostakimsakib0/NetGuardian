# NetGuardian

**Status: Prototype / Work in Progress** — modules are maturing but the project is still in active architectural development.

Intelligent Network Resilience Middleware for Linux.

NetGuardian is a privacy-first, user-space daemon that monitors network health, adapts to changing conditions, and keeps your operations running through unreliable connections. Built in Rust.

## Features

### Network Monitoring
- **Connectivity checks** — ICMP ping with HTTP fallback (reqwest)
- **DNS health** — resolution speed & failure detection (hickory-resolver)
- **Gateway detection** — default route via `/proc/net/route`
- **Interface enumeration** — sysfs + `ip addr` for IP/MAC/state

### Retry Engine
- **5 policies**: Immediate, Fixed, ExponentialBackoff, Adaptive, Smart
- **Circuit breaker** — Closed → Open → HalfOpen with configurable thresholds
- **Failure classification** — Transient, Permanent, Timeout, DnsFailure, NetworkUnreachable, RateLimited
- **Jitter** — random delay variance to avoid thundering herds

### Adaptive Scheduling
- **Network quality tiers** — Good, Degraded, Bad based on latency & loss
- **Dynamic concurrency** — auto-adjusts based on quality + failure history + cooldown
- **Failure penalty weighting** — each failure reduces concurrency; success recovers it

### Plugin System
- **Plugin trait** — Initialize, Start, Pause, Resume, Retry, Cancel, HealthCheck, Shutdown lifecycle
- **CliPlugin trait** — binary-based plugins with default args and resume detection
- **Built-in plugins** — Git, Curl, Wget, Podman (container management)
- **Community plugins** — discover, install, search from an extensible index
- **8 community plugins** — rsync, docker, pip, npm, apt, ffmpeg, ssh, ansible

### Policy & Rules Engine
- **Conditions** — StatusIs, LatencyAbove, PacketLossAbove, DnsHealthy, And/Or/Not combinators
- **Actions** — PauseDownloads, ReduceConcurrency, Notify, RetryWithBackoff, SwitchDns, FlushCache
- **6 built-in rules** — cooldown-gated, loaded by default

### Process Management
- **Subprocess lifecycle** — each external command runs in its own process group (via `setpgid`)
- **Pause/Resume** — sends `SIGSTOP`/`SIGCONT` to the entire process group
- **Cancel** — sends `SIGTERM`, then `SIGKILL` after a grace period
- **`ProcessSupervisor`** — tracks all running processes per plugin, supports batch stop/cont/terminate

### Queue Management
- Priority-ordered operation queue with status lifecycle (Pending→Running→Paused→Completed→Failed)
- Retry failed operations, resume paused, auto-resume mode

### Download Manager
- Full lifecycle: Queued → Downloading → Paused → Completed → Failed
- Progress tracking (bytes downloaded / total), active limit control

### Cache Manager
- Typed caches: GitObjects, OciLayers, PipWheels, AptPackages, NpmPackages, Generic
- LRU eviction by access count & last access, size-based eviction

### Metrics Engine
- Uptime/downtime tracking, latency/loss samples, operation recording
- Retry/reconnect counters, bandwidth sampling

### Command-Line Interface
```
netguardian status            Show network status + metrics
netguardian monitor           Continuously watch network health
netguardian doctor            Run full diagnostics
netguardian metrics           Display session metrics (--format prometheus)
netguardian plugins           List loaded plugins + running processes
netguardian community         Manage community plugins
  search <query>              Search the plugin index
  install <name>              Install a community plugin
  list                        List installed community plugins
  remove <name>               Remove a community plugin
  info <name>                 Show plugin details
  refresh                     Refresh remote plugin index
netguardian queue             Show queued operations
netguardian logs              Show logs
netguardian daemon            Start the background daemon
netguardian job list          List all jobs
netguardian job info <id>     Show job details
netguardian job pause <id>    Pause a running job
netguardian job resume <id>   Resume a paused job
netguardian job cancel <id>   Cancel a running job
netguardian metrics-serve     Serve Prometheus metrics over HTTP
```

### Unix Socket IPC (daemon mode)
When running in daemon mode, NetGuardian listens on a Unix socket (`/var/run/netguardian.sock`)
for text-based commands:
```
echo "status" | nc -U /var/run/netguardian.sock
echo "metrics" | nc -U /var/run/netguardian.sock
echo "metrics/prometheus" | nc -U /var/run/netguardian.sock
echo "config" | nc -U /var/run/netguardian.sock
echo "health" | nc -U /var/run/netguardian.sock
echo "ping" | nc -U /var/run/netguardian.sock
echo "pause_job 1" | nc -U /var/run/netguardian.sock
echo "resume_job 1" | nc -U /var/run/netguardian.sock
echo "cancel_job 1" | nc -U /var/run/netguardian.sock
```

### Prometheus HTTP endpoint
```bash
# Start the Prometheus metrics HTTP server
netguardian metrics-serve --listen 0.0.0.0:9090

# Scrape from Prometheus:
#   - job_name: netguardian
#     static_configs:
#       - targets: ['localhost:9090']
```

## Installation

### Systemd (daemon mode)
```bash
sudo cp netguardian.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now netguardian
```

### Prerequisites

### Prerequisites
- Rust 1.75+ (edition 2021)
- Linux kernel (uses `/proc/net/route`, sysfs, `ip`)

### Build from source
```bash
git clone https://github.com/mostakimsakib0/NetGuardian.git
cd NetGuardian/netguardian
cargo build --release
sudo ./target/release/netguardian daemon
```

### Run tests
```bash
cargo test
```

### Prometheus metrics
```bash
netguardian metrics --format prometheus
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                         CLI                             │
│  status | monitor | doctor | metrics | daemon | queue   │
└──────┬────────────────────────────────────┬─────────────┘
       │                                    │
┌──────▼──────────┐              ┌──────────▼────────────┐
│   Event Bus     │◄────────────►│  Metrics Engine       │
│  pub/sub events │              │  uptime / latency     │
└──────┬──────────┘              │  Prometheus export    │
       │                        └────────────────────────┘
┌──────▼──────────────────────────────────────────────────┐
│                   Network Monitor                       │
│  connectivity │ DNS │ gateway │ interfaces              │
└──────┬──────────────────────────────────────────────────┘
       │
┌──────▼──────────┐  ┌──────────▼──────┐  ┌──────────▼───┐
│  Retry Engine   │  │     Rule        │  │  Adaptive    │
│  circuit + 5    │  │  Engine         │  │  Scheduler   │
│  policies       │  │  conditions →   │  │  concurrency │
│                 │  │  actions        │  │  + quality   │
└──────┬──────────┘  └─────────────────┘  └───────────────┘
       │
┌──────▼──────────────────────────────────────────────────┐
│                 Retry Orchestrator                       │
│      Wires RetryEngine ↔ Job ↔ EventBus                 │
└──────┬──────────────────────────────────────────────────┘
       │
┌──────▼──────────┐  ┌─────────────────┐  ┌───────────────┐
│   Job Model     │  │ Plugin Manager  │  │ Queue Manager │
│  central domain │  │ capabilities +  │  │ job-based     │
│  status machine │  │ built-in + com. │  │ priority      │
└──────┬──────────┘  └─────────────────┘  └───────────────┘
       │
┌──────▼─────────────────────────────────────────────────┐
│                    Job Executor                         │
│  Dequeues Job → finds Plugin → spawns Process           │
│  via Supervisor → tracks via Orchestrator              │
└──────┬─────────────────────────────────────────────────┘
       │
┌──────▼─────────────────────────────────────────────────┐
│                 Process Supervisor                     │
│  SIGSTOP / SIGCONT / SIGTERM / SIGKILL                 │
│  per-process-group subprocess management               │
└────────────────────────────────────────────────────────┘
┌─────────────────┐  ┌─────────────────┐  ┌───────────────┐
│ Download Mgr    │  │ Cache Manager   │  │     IPC       │
│ job-tracked     │  │ typed + LRU     │  │  Unix socket  │
│ progress        │  │                  │  │  commands     │
└─────────────────┘  └─────────────────┘  └───────────────┘
┌─────────────────┐  ┌─────────────────┐
│ Storage         │  │   AppConfig     │
│ JobStore (JSON) │  │  global config  │
│ EventLog        │  │   serialization │
└─────────────────┘  └─────────────────┘
```

## Configuration

Configuration is loaded from `/etc/netguardian/config.json` (auto-created with defaults on first run):

| Field | Description | Default |
|---|---|---|
| `daemon.pid_file` | PID file path | `/var/run/netguardian.pid` |
| `daemon.socket_path` | Unix socket path | `/var/run/netguardian.sock` |
| `daemon.monitor_interval_secs` | Monitor check interval | `5` |
| `network.ping_targets` | ICMP ping targets | `["1.1.1.1", "8.8.8.8"]` |
| `network.dns_servers` | DNS check servers | `["1.1.1.1", "8.8.8.8"]` |
| `network.latency_threshold_ms` | High latency threshold | `200.0` |
| `network.packet_loss_threshold_pct` | High loss threshold | `10.0` |
| `retry.policy` | Retry policy name | `"exponential"` |
| `retry.max_retries` | Max retry attempts | `5` |
| `retry.base_delay_secs` | Base delay between retries | `1` |
| `retry.multiplier` | Exponential backoff multiplier | `2.0` |
| `retry.max_delay_secs` | Maximum delay cap | `60` |
| `retry.circuit_breaker_threshold` | Failures before circuit opens | `5` |
| `retry.circuit_breaker_cooldown_secs` | Circuit breaker cooldown | `30` |
| `storage.jobs_path` | Job persistence path | `/var/lib/netguardian/jobs.json` |
| `storage.cache_dir` | Cache directory | `/var/cache/netguardian` |
| `storage.download_dir` | Downloads directory | `/tmp/netguardian-downloads` |
| `default_max_retries` | Default max retries per job | `3` |
| `active_download_limit` | Concurrent download limit | `3` |
| `log_level` | Log level | `"info"` |

Plugin index URL can be customized via `src/community_plugins.rs`:
```rust
const DEFAULT_INDEX_URL: &str = "https://raw.githubusercontent.com/netguardian/plugins/main/index.json";
```

## Modules

| Module | File | Description |
|---|---|---|
| CLI | `src/cli.rs` | Clap-based command parsing |
| Network Monitor | `src/monitor/` | Connectivity, DNS, gateway, interfaces |
| Event Bus | `src/event_bus/` | Async pub/sub for system events |
| Process Supervisor | `src/process_supervisor.rs` | Subprocess lifecycle: SIGSTOP/SIGCONT/SIGTERM with process groups |
| Job Executor | `src/executor.rs` | Connects Queue → Plugin → ProcessSupervisor → Orchestrator → EventBus |
| Job Model | `src/job.rs` | Central domain object with status machine |
| Retry Engine | `src/retry_engine.rs` | 5 retry policies + circuit breaker |
| Retry Orchestrator | `src/orchestrator.rs` | Wires RetryEngine ↔ Job ↔ EventBus |
| Adaptive Scheduler | `src/adaptive_scheduler.rs` | Dynamic concurrency + quality tiers |
| Plugin Manager | `src/plugin_manager.rs` | Plugin trait + capability system + built-in plugins |
| Community Plugins | `src/community_plugins.rs` | Plugin registry + manifest format + compatibility |
| Queue Manager | `src/queue_manager.rs` | Job-based priority queue (uses Job model) |
| Download Manager | `src/download_manager.rs` | Job-tracked download progress |
| Cache Manager | `src/cache_manager.rs` | Typed caches + LRU eviction |
| Metrics Engine | `src/metrics.rs` | Session metrics + Prometheus export |
| Rule Engine | `src/rule_engine.rs` | Condition/action rules engine |
| Storage | `src/storage.rs` | JobStore (persistent), AppConfig, EventLog |
| IPC | `src/ipc.rs` | Unix socket server for external tooling |
| Systemd Service | `netguardian.service` | Systemd unit file for daemon mode |

## Development

### Adding a community plugin
Add a `CommunityPluginManifest` to the `builtin_index()` function in `src/community_plugins.rs`:
```rust
CommunityPluginManifest {
    name: "my-tool".into(),
    version: "1.0.0".into(),
    description: "Description".into(),
    author: "You".into(),
    repository: Some("https://github.com/you/repo".into()),
    license: Some("MIT".into()),
    tags: vec!["tag1".into(), "tag2".into()],
    binary: "my-tool".into(),
    default_args: vec![],
    supports_resume: false,
    homepage: None,
    compatibility: Some(CompatibilityInfo {
        api_version: "0.1.0".into(),
        min_api_version: "0.1.0".into(),
    }),
    checksum: None,
    dependencies: vec![],
}
```

### Adding a built-in plugin
Implement the `Plugin` and `CliPlugin` traits in `src/plugin_manager.rs`, declare `capabilities()` (Resume, Cancel, Retry, Progress), then register via `discover_builtin()` or `pm.register()` in `main.rs`.

## License

MIT
