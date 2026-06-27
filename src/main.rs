mod adaptive_scheduler;
mod cache_manager;
mod cli;
mod community_plugins;
mod download_manager;
mod event_bus;
mod executor;
mod ipc;
mod job;
mod metrics;
mod monitor;
mod orchestrator;
mod plugin_manager;
mod process_supervisor;
mod queue_manager;
mod retry_engine;
mod rule_engine;
mod storage;

use clap::Parser;
use cli::{Cli, Commands, CommunityCommands};
use community_plugins::CommunityPluginRegistry;
use download_manager::DownloadManager;
use event_bus::bus::EventBus;
use event_bus::events::{NetGuardianEvent, NetworkMetrics};
use executor::JobExecutor;
use metrics::MetricsEngine;
use monitor::connectivity::check_connectivity;
use monitor::dns::check_dns_health;
use monitor::gateway::check_gateway;
use monitor::interface::list_interfaces;
use monitor::{ConnectionStatus, DnsStatus, GatewayStatus, NetworkStatus};
use orchestrator::RetryOrchestrator;
use plugin_manager::PluginManager;
use process_supervisor::ProcessSupervisor;
use queue_manager::QueueManager;
use rule_engine::RuleEngine;
use storage::{AppConfig, EventLog, JobStore};

use chrono::Utc;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = AppConfig::load(&PathBuf::from("/etc/netguardian/config.json"));
    let event_bus = EventBus::new();
    let metrics = Arc::new(MetricsEngine::new());

    match cli.command {
        Commands::Status => cmd_status(metrics).await?,
        Commands::Monitor { interval } => cmd_monitor(event_bus, metrics, interval).await?,
        Commands::Queue => cmd_queue().await?,
        Commands::Plugins => cmd_plugins().await?,
        Commands::Community { command } => cmd_community(command).await?,
        Commands::Doctor => cmd_doctor(metrics).await?,
        Commands::Metrics { format } => cmd_metrics(metrics, &format).await?,
        Commands::Logs => cmd_logs().await?,
        Commands::Daemon => cmd_daemon(event_bus, metrics, config).await?,
        Commands::Job { command } => cmd_job(command).await?,
        Commands::MetricsServe { listen } => cmd_metrics_serve(metrics, &listen).await?,
    }

    Ok(())
}

async fn collect_network_status() -> anyhow::Result<NetworkStatus> {
    let (latency_ms, packet_loss_pct) = check_connectivity().await?;
    let dns = check_dns_health().await?;
    let gateway = check_gateway().await?;
    let interfaces = list_interfaces().await?;

    let status = if packet_loss_pct >= 80.0 {
        ConnectionStatus::Offline
    } else if packet_loss_pct > 10.0 || latency_ms > 500.0 {
        ConnectionStatus::Degraded
    } else {
        ConnectionStatus::Online
    };

    Ok(NetworkStatus {
        status,
        latency_ms,
        packet_loss_pct,
        dns,
        gateway,
        interfaces,
        timestamp: Utc::now().to_rfc3339(),
    })
}

async fn cmd_status(metrics: Arc<MetricsEngine>) -> anyhow::Result<()> {
    let net = collect_network_status().await?;
    let snap = metrics.snapshot();
    let output = serde_json::json!({
        "network": net,
        "metrics": snap,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn cmd_monitor(
    event_bus: EventBus,
    metrics: Arc<MetricsEngine>,
    interval_secs: u64,
) -> anyhow::Result<()> {
    println!("Monitoring network every {} seconds...", interval_secs);

    let mut rule_engine = RuleEngine::new();
    rule_engine.load_defaults();
    let mut was_online = true;
    let mut log = EventLog::new(1000);

    loop {
        let status = collect_network_status().await?;
        let is_online = status.status == ConnectionStatus::Online;

        metrics.record_connectivity(status.latency_ms, status.packet_loss_pct);

        if is_online && !was_online {
            metrics.record_online();
            event_bus.publish(NetGuardianEvent::InternetRecovered(NetworkMetrics {
                latency_ms: status.latency_ms,
                packet_loss_pct: status.packet_loss_pct,
                dns_healthy: status.dns.healthy,
                jitter_ms: None,
                bandwidth_kbps: None,
            }));
            log.info("monitor", "Internet recovered");
            println!("[{}] Internet recovered", status.timestamp);
        } else if !is_online && was_online {
            metrics.record_offline();
            event_bus.publish(NetGuardianEvent::InternetLost);
            log.warn("monitor", "Internet lost");
            println!("[{}] Internet lost!", status.timestamp);
        }

        if status.packet_loss_pct > 10.0 {
            event_bus.publish(NetGuardianEvent::PacketLossHigh {
                packet_loss_pct: status.packet_loss_pct,
                latency_ms: status.latency_ms,
                interface: status.interfaces.first().map(|i| i.name.clone()),
            });
        }

        if status.latency_ms > 500.0 {
            event_bus.publish(NetGuardianEvent::LatencyHigh {
                latency_ms: status.latency_ms,
                packet_loss_pct: status.packet_loss_pct,
                interface: status.interfaces.first().map(|i| i.name.clone()),
            });
        }

        // Evaluate rules and execute actions
        let evaluations = rule_engine.evaluate(&status).await;
        for eval in &evaluations {
            log.info("rule", &format!("Rule '{}' triggered", eval.rule_name));
            println!("[rule] {} triggered: {:?}", eval.rule_name, eval.actions_taken);
        }

        let snap = metrics.snapshot();
        println!(
            "  Status: {:?} | Latency: {:.0}ms | Loss: {:.0}% | DNS: {} | Retries: {} | Reconnects: {}",
            status.status,
            status.latency_ms,
            status.packet_loss_pct,
            if status.dns.healthy { "OK" } else { "FAIL" },
            snap.total_retries,
            snap.reconnect_count,
        );

        was_online = is_online;
        tokio::time::sleep(tokio::time::Duration::from_secs(interval_secs)).await;
    }
}

async fn cmd_queue() -> anyhow::Result<()> {
    let mut store = JobStore::new(&PathBuf::from("/var/lib/netguardian/jobs.json"));
    let jobs = store.all();
    println!("{}", serde_json::to_string_pretty(&jobs)?);
    Ok(())
}

async fn cmd_plugins() -> anyhow::Result<()> {
    let mut pm = PluginManager::new();
    pm.discover_builtin();

    let plugins = pm.list().await;
    let mut output = serde_json::json!({ "plugins": plugins });

    // Show managed process info if supervisor has entries
    let supervisor = ProcessSupervisor::new();
    let running = supervisor.all_running().await;
    if !running.is_empty() {
        output["running_processes"] = serde_json::json!(running);
    }

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn cmd_community(command: CommunityCommands) -> anyhow::Result<()> {
    let registry = Arc::new(tokio::sync::Mutex::new(CommunityPluginRegistry::new()));

    match command {
        CommunityCommands::Search { query } => {
            let registry = registry.lock().await;
            let results = registry.search(&query);
            if results.is_empty() {
                println!("No community plugins found matching '{}'", query);
            } else {
                println!("Found {} plugin(s):", results.len());
                for m in results {
                    println!("  {} v{} - {} (tags: {})",
                        m.name, m.version, m.description, m.tags.join(", "));
                }
            }
        }
        CommunityCommands::Install { name } => {
            let mut registry = registry.lock().await;
            match registry.install(&name) {
                Ok(manifest) => {
                    println!("Installed community plugin '{}' v{}", manifest.name, manifest.version);
                    println!("  Description: {}", manifest.description);
                    println!("  Binary: {}", manifest.binary);
                    println!("  Support resume: {}", manifest.supports_resume);
                }
                Err(e) => println!("Install failed: {}", e),
            }
        }
        CommunityCommands::List => {
            let registry = registry.lock().await;
            let installed = registry.list_installed();
            if installed.is_empty() {
                println!("No community plugins installed");
            } else {
                println!("Installed community plugins:");
                for entry in &installed {
                    println!("  {} v{} (installed {})",
                        entry.manifest.name, entry.manifest.version, entry.installed_at);
                }
            }
        }
        CommunityCommands::Remove { name } => {
            let mut registry = registry.lock().await;
            if registry.remove(&name) {
                println!("Removed community plugin '{}'", name);
            } else {
                println!("Plugin '{}' is not installed", name);
            }
        }
        CommunityCommands::Info { name } => {
            let registry = registry.lock().await;
            match registry.info(&name) {
                Some(m) => {
                    println!("{} v{}", m.name, m.version);
                    println!("  Author: {}", m.author);
                    println!("  Description: {}", m.description);
                    println!("  Binary: {}", m.binary);
                    println!("  Default args: {}", m.default_args.join(" "));
                    println!("  Supports resume: {}", m.supports_resume);
                    println!("  Tags: {}", m.tags.join(", "));
                    if let Some(repo) = &m.repository {
                        println!("  Repository: {}", repo);
                    }
                    if let Some(license) = &m.license {
                        println!("  License: {}", license);
                    }
                    if let Some(homepage) = &m.homepage {
                        println!("  Homepage: {}", homepage);
                    }
                    println!("  Installed: {}", registry.is_installed(&name));
                }
                None => println!("Plugin '{}' not found in community index", name),
            }
        }
        CommunityCommands::Refresh => {
            let mut registry = registry.lock().await;
            match registry.refresh_index().await {
                Ok(count) => println!("Refreshed community index: {} plugins available", count),
                Err(e) => println!("Refresh failed: {}", e),
            }
        }
    }

    Ok(())
}

async fn cmd_doctor(metrics: Arc<MetricsEngine>) -> anyhow::Result<()> {
    println!("NetGuardian Diagnostics\n");

    let status = collect_network_status().await?;

    println!("[1/4] Gateway check:");
    println!(
        "  Gateway: {}",
        status.gateway.gateway_ip.unwrap_or_else(|| "N/A".into())
    );
    println!(
        "  Reachable: {}",
        if status.gateway.reachable { "YES" } else { "NO" }
    );

    println!("\n[2/4] DNS check:");
    println!(
        "  Status: {}",
        if status.dns.healthy { "OK" } else { "FAIL" }
    );
    println!("  Resolution: {:.0}ms", status.dns.resolution_ms);

    println!("\n[3/4] Connectivity check:");
    println!("  Status: {:?}", status.status);
    println!("  Latency: {:.0}ms", status.latency_ms);
    println!("  Packet loss: {:.0}%", status.packet_loss_pct);

    println!("\n[4/4] Network interfaces:");
    for iface in &status.interfaces {
        println!(
            "  {}: {} ({} up: {})",
            iface.name,
            iface.ip.as_deref().unwrap_or("N/A"),
            iface.mac.as_deref().unwrap_or("N/A"),
            iface.is_up,
        );
    }

    let snap = metrics.snapshot();
    println!("\n[5/5] Session metrics:");
    println!("  Uptime: {}s", snap.uptime_secs);
    println!("  Downtime: {}s", snap.downtime_secs);
    println!("  Retries: {}", snap.total_retries);
    println!("  Reconnects: {}", snap.reconnect_count);
    println!("  Successful ops: {}", snap.successful_operations);
    println!("  Failed ops: {}", snap.failed_operations);
    println!("  Avg latency: {:.1}ms", snap.avg_latency_ms);
    println!("  Avg loss: {:.1}%", snap.avg_packet_loss_pct);

    println!("\nDiagnostics complete.");
    Ok(())
}

async fn cmd_metrics(metrics: Arc<MetricsEngine>, format: &str) -> anyhow::Result<()> {
    if format == "prometheus" {
        print!("{}", metrics.format_prometheus());
    } else {
        let snap = metrics.snapshot();
        println!("{}", serde_json::to_string_pretty(&snap)?);
    }
    Ok(())
}

async fn cmd_logs() -> anyhow::Result<()> {
    let log = EventLog::new(1000);
    let entries = log.entries();
    println!("{}", serde_json::to_string_pretty(entries)?);
    Ok(())
}

async fn cmd_job(command: cli::JobCommands) -> anyhow::Result<()> {
    use cli::JobCommands;
    match command {
        JobCommands::List => {
            // Read jobs from persistent store
            let store = JobStore::new(&std::path::Path::new("/var/lib/netguardian/jobs.json"));
            let jobs = store.all();
            println!("{}", serde_json::to_string_pretty(&jobs)?);
        }
        JobCommands::Info { id } => {
            let store = JobStore::new(&std::path::Path::new("/var/lib/netguardian/jobs.json"));
            match store.get(id) {
                Some(job) => println!("{}", serde_json::to_string_pretty(job)?),
                None => println!("{{\"error\": \"job {} not found\"}}", id),
            }
        }
        JobCommands::Pause { id } => {
            // When running standalone, send command via IPC socket
            println!("{{\"info\": \"use 'echo pause_job {} | nc -U /var/run/netguardian.sock' to pause a running job\"}}", id);
        }
        JobCommands::Resume { id } => {
            println!("{{\"info\": \"use 'echo resume_job {} | nc -U /var/run/netguardian.sock' to resume\"}}", id);
        }
        JobCommands::Cancel { id } => {
            println!("{{\"info\": \"use 'echo cancel_job {} | nc -U /var/run/netguardian.sock' to cancel\"}}", id);
        }
    }
    Ok(())
}

async fn cmd_metrics_serve(metrics: Arc<MetricsEngine>, listen: &str) -> anyhow::Result<()> {
    use std::net::SocketAddr;
    let addr: SocketAddr = listen.parse().map_err(|e| anyhow::anyhow!("invalid address: {}", e))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Serving Prometheus metrics on http://{}/metrics", listen);

    loop {
        let (stream, peer) = listener.accept().await?;
        let metrics = metrics.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            let mut reader = BufReader::new(stream);
            let mut line = String::new();

            // Read HTTP request (just the first line for path matching)
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {
                    let path = line.split_whitespace().nth(1).unwrap_or("/");
                    let (status, content_type, body) = match path {
                        "/metrics" => ("200 OK", "text/plain; version=0.0.4", metrics.format_prometheus()),
                        "/health" => ("200 OK", "application/json", "{\"status\":\"ok\"}".into()),
                        "/" => ("200 OK", "text/plain", "NetGuardian Metrics\nGET /metrics\nGET /health\n".into()),
                        _ => ("404 Not Found", "text/plain", "Not found".into()),
                    };

                    let response = format!(
                        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status, content_type, body.len(), body
                    );

                    let _ = reader.get_mut().write_all(response.as_bytes()).await;
                    let _ = reader.get_mut().flush().await;
                }
            }
        });

        let _ = peer;
    }
}

async fn cmd_daemon(event_bus: EventBus, metrics: Arc<MetricsEngine>, config: AppConfig) -> anyhow::Result<()> {
    println!("NetGuardian daemon starting...");

    // ── PID file ──
    if let Some(parent) = std::path::Path::new(&config.daemon.pid_file).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&config.daemon.pid_file, format!("{}", std::process::id()));

    // ── Initialise components ──
    let pm = Arc::new(Mutex::new({
        let mut mgr = PluginManager::new();
        mgr.discover_builtin();
        mgr
    }));
    let supervisor = Arc::new(ProcessSupervisor::new());
    let qm = QueueManager::new();
    let dm = DownloadManager::new(&std::path::Path::new(&config.storage.download_dir));
    let mut store = JobStore::new(&std::path::Path::new(&config.storage.jobs_path));
    let mut log = EventLog::new(1000);

    let mut rule_engine = RuleEngine::new();
    rule_engine.load_defaults();

    // ── Job Executor ──
    let orch = Arc::new(Mutex::new(RetryOrchestrator::new(
        config.retry.max_retries,
        retry_engine::RetryPolicy::ExponentialBackoff {
            initial: std::time::Duration::from_secs(config.retry.base_delay_secs),
            multiplier: config.retry.multiplier,
            max_delay: std::time::Duration::from_secs(config.retry.max_delay_secs),
        },
    ).with_event_bus(event_bus.clone())));

    let executor = JobExecutor::new(
        pm.clone(),
        qm.clone(),
        dm.clone(),
        supervisor.clone(),
        orch.clone(),
        event_bus.clone(),
        metrics.clone(),
    );
    tokio::spawn(async move {
        executor.execute_loop().await;
    });

    // ── IPC server ──
    let ipc_metrics = metrics.clone();
    let ipc_config = config.clone();
    let ipc_bus = event_bus.clone();
    tokio::spawn(async move {
        let server = ipc::IpcServer::new(&ipc_config.daemon.socket_path);
        if let Err(e) = server.serve(&ipc_bus, &ipc_metrics, &ipc_config).await {
            eprintln!("IPC server error: {}", e);
        }
    });

    log.info("daemon", "Components initialized");

    // ── Signal handling ──
    let mut term_signal = Box::pin(tokio::signal::ctrl_c());
    let mut rx = event_bus.subscribe();

    // ── Monitor task ──
    let monitor_metrics = metrics.clone();
    let monitor_bus = event_bus.clone();
    let monitor_interval = config.daemon.monitor_interval_secs;
    tokio::spawn(async move {
        let mut rule_engine = RuleEngine::new();
        rule_engine.load_defaults();
        let mut was_online = true;

        loop {
            let status = collect_network_status().await.unwrap_or_else(|_| NetworkStatus {
                status: ConnectionStatus::Offline,
                latency_ms: 0.0,
                packet_loss_pct: 100.0,
                dns: DnsStatus {
                    healthy: false,
                    resolution_ms: 0.0,
                    nameservers: vec![],
                },
                gateway: GatewayStatus {
                    reachable: false,
                    gateway_ip: None,
                    interface: None,
                },
                interfaces: vec![],
                timestamp: Utc::now().to_rfc3339(),
            });
            let is_online = status.status == ConnectionStatus::Online;

            monitor_metrics.record_connectivity(status.latency_ms, status.packet_loss_pct);

            if is_online && !was_online {
                monitor_metrics.record_online();
                monitor_bus.publish(NetGuardianEvent::InternetRecovered(NetworkMetrics {
                    latency_ms: status.latency_ms,
                    packet_loss_pct: status.packet_loss_pct,
                    dns_healthy: status.dns.healthy,
                    jitter_ms: None,
                    bandwidth_kbps: None,
                }));
            } else if !is_online && was_online {
                monitor_metrics.record_offline();
                monitor_bus.publish(NetGuardianEvent::InternetLost);
            }

            // Rule evaluation
            let _ = rule_engine.evaluate(&status).await;

            was_online = is_online;
            tokio::time::sleep(tokio::time::Duration::from_secs(monitor_interval)).await;
        }
    });

    // ── Event processing loop ──
    loop {
        tokio::select! {
            _ = &mut term_signal => {
                log.info("daemon", "Shutdown signal received");
                println!("\nShutdown signal received. Cleaning up...");
                // Kill all managed processes before exit
                supervisor.terminate_all("", std::time::Duration::from_secs(3)).await;
                let _ = store.save();
                let _ = std::fs::remove_file(&config.daemon.pid_file);
                log.info("daemon", "Daemon shut down");
                break;
            }
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        log.info("daemon", &format!("Event: {:?}", event));
                        println!("Event: {:?}", event);
                    }
                    Err(_) => {
                        log.error("daemon", "Event bus error");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
