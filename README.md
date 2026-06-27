# NetGuardian

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
netguardian status       Show network status + metrics
netguardian monitor      Continuously watch network health
netguardian doctor       Run full diagnostics
netguardian metrics      Display session metrics
netguardian plugins      List loaded plugins
netguardian community    Manage community plugins
  search <query>         Search the plugin index
  install <name>         Install a community plugin
  list                   List installed community plugins
  remove <name>          Remove a community plugin
  info <name>            Show plugin details
  refresh                Refresh remote plugin index
netguardian queue        Show queued operations
netguardian logs         Show logs
netguardian daemon       Start the background daemon
```

## Installation

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

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                        CLI                          │
│  status | monitor | doctor | metrics | daemon       │
└──────┬──────────────────────────────────┬───────────┘
       │                                  │
┌──────▼──────────┐            ┌──────────▼──────────┐
│   Event Bus     │◄──────────►│  Metrics Engine     │
│  pub/sub events │            │  uptime / latency   │
└──────┬──────────┘            └─────────────────────┘
       │
┌──────▼──────────────────────────────────────────────┐
│                   Network Monitor                   │
│  connectivity │ DNS │ gateway │ interfaces          │
└──────┬──────────────────────────────────────────────┘
       │
┌──────▼──────────┐  ┌──────────▼──────┐  ┌──────────▼─┐
│  Retry Engine   │  │     Rule        │  │  Adaptive   │
│  circuit + 5    │  │  Engine         │  │  Scheduler  │
│  policies       │  │  conditions →   │  │  concurrency│
│                 │  │  actions        │  │  + quality  │
└─────────────────┘  └─────────────────┘  └─────────────┘
┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐
│ Plugin Manager  │  │ Queue Manager   │  │ Download    │
│ built-in +      │  │ priority queue  │  │ Manager     │
│ community       │  │ lifecycle       │  │ progress    │
└─────────────────┘  └─────────────────┘  └─────────────┘
┌─────────────────┐  ┌─────────────────┐
│ Cache Manager   │  │Community Plugin │
│ typed + LRU     │  │ Registry        │
└─────────────────┘  └─────────────────┘
```

## Configuration

Plugin index URL can be customized via `src/community_plugins.rs`:
```rust
const DEFAULT_INDEX_URL: &str = "https://raw.githubusercontent.com/netguardian/plugins/main/index.json";
```
Override at runtime by editing the constant and rebuilding.

## Modules

| Module | File | Description |
|---|---|---|
| CLI | `src/cli.rs` | Clap-based command parsing |
| Network Monitor | `src/monitor/` | Connectivity, DNS, gateway, interfaces |
| Event Bus | `src/event_bus/` | Async pub/sub for system events |
| Retry Engine | `src/retry_engine.rs` | 5 retry policies + circuit breaker |
| Adaptive Scheduler | `src/adaptive_scheduler.rs` | Dynamic concurrency + quality tiers |
| Plugin Manager | `src/plugin_manager.rs` | Plugin trait + built-in plugins |
| Community Plugins | `src/community_plugins.rs` | Plugin registry + manifest format |
| Queue Manager | `src/queue_manager.rs` | Priority operation queue |
| Download Manager | `src/download_manager.rs` | Download lifecycle + progress |
| Cache Manager | `src/cache_manager.rs` | Typed caches + LRU eviction |
| Metrics Engine | `src/metrics.rs` | Session metrics + bandwidth |
| Rule Engine | `src/rule_engine.rs` | Condition/action rules engine |

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
}
```

### Adding a built-in plugin
Implement the `Plugin` and `CliPlugin` traits in `src/plugin_manager.rs`, then register it in `main.rs`.

## License

MIT
