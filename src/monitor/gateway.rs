use crate::monitor::GatewayStatus;

use anyhow::Result;

pub async fn check_gateway() -> Result<GatewayStatus> {
    let (gateway_ip, interface) = find_default_gateway();

    let reachable = if let Some(ref ip) = gateway_ip {
        ping_gateway(ip).await?
    } else {
        false
    };

    Ok(GatewayStatus {
        reachable,
        gateway_ip,
        interface,
    })
}

fn find_default_gateway() -> (Option<String>, Option<String>) {
    let route = match std::fs::read_to_string("/proc/net/route").ok() {
        Some(r) => r,
        None => return (None, None),
    };

    for line in route.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 3 {
            continue;
        }

        let destination = fields[1];
        let gateway_hex = fields[2];

        if destination == "00000000" && gateway_hex != "00000000" {
            let ip = hex_to_ip(gateway_hex);
            let iface = fields[0].to_string();
            return (Some(ip), Some(iface));
        }
    }

    (None, None)
}

fn hex_to_ip(hex: &str) -> String {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .filter_map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16).ok()
        })
        .collect();

    if bytes.len() == 4 {
        format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3])
    } else {
        String::new()
    }
}

async fn ping_gateway(ip: &str) -> Result<bool> {
    let output = tokio::process::Command::new("ping")
        .arg("-c")
        .arg("1")
        .arg("-W")
        .arg("2")
        .arg(ip)
        .output()
        .await?;

    Ok(output.status.success())
}
