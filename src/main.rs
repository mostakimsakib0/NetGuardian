mod adaptive_scheduler;
mod cache_manager;
mod cli;
mod community_plugins;
mod download_manager;
mod event_bus;
mod metrics;
mod monitor;
mod plugin_manager;
mod queue_manager;
mod retry_engine;
mod rule_engine;

use clap::Parser;
use cli::{Cli, Commands, CommunityCommands};
use community_plugins::CommunityPluginRegistry;
use event_bus::bus::EventBus;
use event_bus::events::{NetGuardianEvent, NetworkMetrics};
use metrics::MetricsEngine;
use monitor::connectivity::check_connectivity;
use monitor::dns::check_dns_health;
use monitor::gateway::check_gateway;
use monitor::interface::list_interfaces;
use monitor::ConnectionStatus;
use monitor::NetworkStatus;

use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let event_bus = EventBus::new();
    let metrics = Arc::new(MetricsEngine::new());

    match cli.command {
        Commands::Status => cmd_status(metrics).await?,
        Commands::Monitor { interval } => cmd_monitor(event_bus, metrics, interval).await?,
        Commands::Queue => println!("{{ \"queue\": [] }}"),
        Commands::Plugins => cmd_plugins().await?,
        Commands::Community { command } => cmd_community(command).await?,
        Commands::Doctor => cmd_doctor(metrics).await?,
        Commands::Metrics => cmd_metrics(metrics).await?,
        Commands::Logs => println!("{{ \"logs\": [] }}"),
        Commands::Daemon => cmd_daemon(event_bus, metrics).await?,
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

    let mut was_online = true;

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
            }));
            println!("[{}] Internet recovered", status.timestamp);
        } else if !is_online && was_online {
            metrics.record_offline();
            event_bus.publish(NetGuardianEvent::InternetLost);
            println!("[{}] Internet lost!", status.timestamp);
        }

        if status.packet_loss_pct > 10.0 {
            event_bus.publish(NetGuardianEvent::PacketLossHigh {
                packet_loss_pct: status.packet_loss_pct,
            });
        }

        if status.latency_ms > 500.0 {
            event_bus.publish(NetGuardianEvent::LatencyHigh {
                latency_ms: status.latency_ms,
            });
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

async fn cmd_plugins() -> anyhow::Result<()> {
    let mut pm = plugin_manager::PluginManager::new();
    pm.register(Arc::new(Mutex::new(plugin_manager::GitPlugin::new())))
        .await;
    pm.register(Arc::new(Mutex::new(plugin_manager::CurlPlugin::new())))
        .await;
    pm.register(Arc::new(Mutex::new(plugin_manager::WgetPlugin::new())))
        .await;
    pm.register(Arc::new(Mutex::new(plugin_manager::PodmanPlugin::new())))
        .await;

    let plugins = pm.list().await;
    println!("{}", serde_json::to_string_pretty(&plugins)?);
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

async fn cmd_metrics(metrics: Arc<MetricsEngine>) -> anyhow::Result<()> {
    let snap = metrics.snapshot();
    println!("{}", serde_json::to_string_pretty(&snap)?);
    Ok(())
}

async fn cmd_daemon(event_bus: EventBus, metrics: Arc<MetricsEngine>) -> anyhow::Result<()> {
    println!("NetGuardian daemon starting...");

    let mut rx = event_bus.subscribe();

    tokio::spawn(async move {
        cmd_monitor(event_bus, metrics, 5).await.unwrap();
    });

    loop {
        match rx.recv().await {
            Ok(event) => {
                println!("Event: {:?}", event);
            }
            Err(_) => break,
        }
    }

    Ok(())
}
