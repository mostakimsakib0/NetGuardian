use anyhow::Result;
use std::time::Instant;

const PING_TARGETS: &[&str] = &["1.1.1.1", "8.8.8.8", "google.com"];
const PING_COUNT: usize = 4;

pub async fn check_connectivity() -> Result<(f64, f64)> {
    let mut total_latency = 0.0;
    let mut succeeded = 0;
    let mut total_probes = 0;

    for target in PING_TARGETS {
        let (probes, success, latencies) = ping_host(target).await?;
        total_probes += probes;
        succeeded += success;
        total_latency += latencies;
    }

    if succeeded == 0 {
        return http_fallback_check().await;
    }

    let packet_loss = if total_probes > 0 {
        ((total_probes - succeeded) as f64 / total_probes as f64) * 100.0
    } else {
        100.0
    };

    let avg_latency = total_latency / succeeded as f64;

    Ok((avg_latency, packet_loss))
}

async fn http_fallback_check() -> Result<(f64, f64)> {
    let start = Instant::now();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let urls = ["https://1.1.1.1", "https://cloudflare.com", "https://google.com"];
    let mut success = false;

    for url in &urls {
        if let Ok(resp) = client.get(*url).send().await {
            if resp.status().is_success() {
                success = true;
                break;
            }
        }
    }

    if success {
        let latency = start.elapsed().as_secs_f64() * 1000.0;
        Ok((latency, 0.0))
    } else {
        Ok((0.0, 100.0))
    }
}

async fn ping_host(host: &str) -> Result<(usize, usize, f64)> {
    let _start = Instant::now();

    let output = tokio::process::Command::new("ping")
        .arg("-c")
        .arg(PING_COUNT.to_string())
        .arg("-W")
        .arg("2")
        .arg(host)
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    let total_latency = parse_total_latency(&stdout);
    let success_count = parse_success_count(&stdout);

    if !output.status.success() && success_count == 0 {
        let no_route = stderr.contains("Network is unreachable")
            || stdout.contains("Network is unreachable")
            || stdout.contains("Destination Host Unreachable")
            || stdout.contains("100% packet loss");
        if no_route {
            return Ok((PING_COUNT, 0, 0.0));
        }
    }

    Ok((PING_COUNT, success_count, total_latency))
}

fn parse_total_latency(output: &str) -> f64 {
    output
        .lines()
        .filter_map(|line| {
            if line.contains("time=") {
                let part = line.split("time=").nth(1)?;
                let val = part.split_whitespace().next()?;
                val.trim_end_matches("ms").parse::<f64>().ok()
            } else {
                None
            }
        })
        .sum()
}

fn parse_success_count(output: &str) -> usize {
    output
        .lines()
        .filter(|line| line.contains("time=") || line.contains("bytes from"))
        .count()
}
