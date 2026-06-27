use crate::monitor::InterfaceInfo;

use anyhow::Result;

pub async fn list_interfaces() -> Result<Vec<InterfaceInfo>> {
    let mut interfaces = Vec::new();

    let dir = std::fs::read_dir("/sys/class/net")?;
    for entry in dir {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();

        let is_loopback = name == "lo";
        let is_up = is_interface_up(&name);

        let ip = get_interface_ip(&name).await;
        let mac = get_interface_mac(&name);
        let (rx_bytes, tx_bytes) = get_interface_stats(&name);

        interfaces.push(InterfaceInfo {
            name,
            ip,
            mac,
            is_up,
            is_loopback,
            rx_bytes,
            tx_bytes,
        });
    }

    Ok(interfaces)
}

fn is_interface_up(name: &str) -> bool {
    let operstate_path = format!("/sys/class/net/{}/operstate", name);
    std::fs::read_to_string(operstate_path)
        .ok()
        .map(|s| s.trim() == "up")
        .unwrap_or(false)
}

async fn get_interface_ip(name: &str) -> Option<String> {
    let output = tokio::process::Command::new("ip")
        .args(["-4", "-o", "addr", "show", name])
        .output()
        .await
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .split_whitespace()
        .find(|part| part.contains('/'))
        .map(|cidr| cidr.split('/').next().unwrap_or(cidr).to_string())
}

fn get_interface_mac(name: &str) -> Option<String> {
    let addr_path = format!("/sys/class/net/{}/address", name);
    std::fs::read_to_string(addr_path)
        .ok()
        .map(|s| s.trim().to_string())
}

fn get_interface_stats(name: &str) -> (u64, u64) {
    let rx_path = format!("/sys/class/net/{}/statistics/rx_bytes", name);
    let tx_path = format!("/sys/class/net/{}/statistics/tx_bytes", name);

    let rx = std::fs::read_to_string(rx_path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);

    let tx = std::fs::read_to_string(tx_path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);

    (rx, tx)
}
