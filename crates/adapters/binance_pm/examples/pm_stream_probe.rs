//! PM 用户流探针(生产,零下单):listenKey 全生命周期 + WS 连接 + 事件解码。
//!
//! 用法:
//! ```bash
//! sops exec-env secrets.trading.enc.env \
//!   'cargo run --example binance-pm-stream-probe --package nautilus-binance-pm -- 45'
//! ```
//! 验证:POST listenKey → WS 握手 → 服务端活动(ping 帧节奏 = 活性判据)→
//! 事件解码(有账户活动时)→ PUT 续期 → DELETE 关闭。

use std::time::Duration;

use futures_util::StreamExt;
use nautilus_binance_pm::config::{ENV_PM_API_KEY, ENV_PM_HMAC_SECRET};
use nautilus_binance_pm::http::client::BinancePmHttpClient;
use nautilus_binance_pm::websocket::messages::{PmUserStreamEvent, parse_user_stream_event};
use nautilus_binance_pm::websocket::runtime::connect_pm_stream;
use nautilus_network::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secs: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(45);
    let api_key = std::env::var(ENV_PM_API_KEY)?;
    let api_secret = std::env::var(ENV_PM_HMAC_SECRET)?;

    let client = BinancePmHttpClient::new(
        Some(api_key),
        Some(api_secret),
        None,
        Some(5000),
        Some(10),
        None,
    )?;

    let key = client.create_listen_key().await?;
    println!("[1] listenKey 创建:ok(长度 {})", key.listen_key.len());

    let (mut reader, ws) =
        connect_pm_stream("wss://fstream.binance.com/pm", &key.listen_key, None).await?;
    println!("[2] WS 握手:ok(注意:握手成功≠key 有效,活性看服务端 ping)");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let mut pings = 0u32;
    let mut events = 0u32;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, reader.next()).await {
            Ok(Some(Ok(Message::Ping(payload)))) => {
                pings += 1;
                let _ = ws.send_pong(payload.to_vec()).await;
                println!("[3] 服务端 ping #{pings}(已回 pong)——流活性确认");
            }
            Ok(Some(Ok(Message::Text(text)))) => {
                events += 1;
                match std::str::from_utf8(&text) {
                    Ok(t) => match parse_user_stream_event(t) {
                        PmUserStreamEvent::Unknown { event_type, .. } => {
                            println!("[3] 事件 #{events}: 未知类型 {event_type:?}(须核查)");
                        }
                        ev => println!("[3] 事件 #{events}: {}", event_name(&ev)),
                    },
                    Err(_) => println!("[3] 非 UTF-8 报文"),
                }
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => {
                println!("[!] 读错误: {e}");
                break;
            }
            Ok(None) => {
                println!("[!] 流关闭");
                break;
            }
            Err(_) => break, // 窗口耗尽
        }
    }
    println!("[4] 观察 {secs}s:服务端 ping×{pings},事件×{events}");

    client.keepalive_listen_key().await?;
    println!("[5] listenKey PUT 续期:ok(未见 -1125,key 有效性确认)");

    client.close_listen_key().await?;
    println!("[6] listenKey DELETE:ok");
    let _ = ws.send_close_message().await;
    println!("用户流通路验证完成。");
    Ok(())
}

fn event_name(ev: &PmUserStreamEvent) -> &'static str {
    match ev {
        PmUserStreamEvent::OrderTradeUpdate(_) => "ORDER_TRADE_UPDATE",
        PmUserStreamEvent::AccountUpdate(_) => "ACCOUNT_UPDATE",
        PmUserStreamEvent::AccountConfigUpdate(_) => "ACCOUNT_CONFIG_UPDATE",
        PmUserStreamEvent::ExecutionReport(_) => "executionReport",
        PmUserStreamEvent::OutboundAccountPosition(_) => "outboundAccountPosition",
        PmUserStreamEvent::BalanceUpdate(_) => "balanceUpdate",
        PmUserStreamEvent::LiabilityChange(_) => "liabilityChange(BROKEN 告警)",
        PmUserStreamEvent::OpenOrderLoss(_) => "openOrderLoss",
        PmUserStreamEvent::RiskLevelChange(_) => "RISK_LEVEL_CHANGE",
        PmUserStreamEvent::ListenKeyExpired(_) => "listenKeyExpired",
        PmUserStreamEvent::ConditionalOrderTradeUpdate(_) => "CONDITIONAL_ORDER_TRADE_UPDATE",
        PmUserStreamEvent::AlgoUpdate(_) => "ALGO_UPDATE",
        PmUserStreamEvent::Unknown { .. } => "unknown",
    }
}
