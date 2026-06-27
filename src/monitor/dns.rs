use crate::monitor::DnsStatus;

use anyhow::Result;
use std::time::Instant;
use hickory_resolver::TokioResolver;

const TEST_DOMAINS: &[&str] = &["google.com", "cloudflare.com"];

pub async fn check_dns_health() -> Result<DnsStatus> {
    let resolver = TokioResolver::builder_tokio()?.build()?;

    let nameservers = read_nameservers();

    let mut healthy = true;
    let mut total_time = 0.0;
    let mut resolved = 0;

    for domain in TEST_DOMAINS {
        let start = Instant::now();
        match resolver.lookup_ip(*domain).await {
            Ok(response) => {
                total_time += start.elapsed().as_secs_f64() * 1000.0;
                resolved += 1;
                let ips: Vec<_> = response.iter().collect();
                eprintln!("[debug] Resolved {} -> {:?}", domain, ips);
            }
            Err(e) => {
                eprintln!("[warn] DNS resolution failed for {}: {}", domain, e);
                healthy = false;
            }
        }
    }

    let resolution_ms = if resolved > 0 {
        total_time / resolved as f64
    } else {
        0.0
    };

    if resolved == 0 {
        healthy = false;
    }

    Ok(DnsStatus {
        healthy,
        resolution_ms,
        nameservers,
    })
}

fn read_nameservers() -> Vec<String> {
    let content = match std::fs::read_to_string("/etc/resolv.conf") {
        Ok(c) => c,
        Err(_) => return vec!["N/A".into()],
    };

    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with("nameserver") {
                line.split_whitespace().nth(1).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect()
}
