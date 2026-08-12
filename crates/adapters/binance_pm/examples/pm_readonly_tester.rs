//! L1 只读验证工具(生产账户,零下单)。
//!
//! 用法(密钥经 sops 注入,永不落盘):
//! ```bash
//! sops exec-env secrets.trading.enc.env \
//!   'cargo run --example binance-pm-readonly --package nautilus-binance-pm -- SOLUSDC'
//! ```
//! 覆盖 L1 清单:ping/time → balance/account(签名+字段解析)→ positionRisk →
//! 两腿 openOrders → positionSide/dual(one-way 校验)→ rateLimit/order。
//! 输出供与现有 papi 生产实现(oracle)逐字段 diff。

use nautilus_binance_pm::config::{ENV_PM_API_KEY, ENV_PM_HMAC_SECRET};
use nautilus_binance_pm::http::client::BinancePmHttpClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "SOLUSDC".to_string());
    let api_key = std::env::var(ENV_PM_API_KEY).ok();
    let api_secret = std::env::var(ENV_PM_HMAC_SECRET).ok();
    let signed = api_key.is_some();

    let client = BinancePmHttpClient::new(api_key, api_secret, None, Some(5000), Some(10), None)?;

    client.ping().await?;
    println!("[1] ping: ok");
    let t = client.server_time().await?;
    println!("[2] serverTime: {}", t.server_time);

    if !signed {
        println!("(未设置 {ENV_PM_API_KEY}/{ENV_PM_HMAC_SECRET},签名端点跳过)");
        return Ok(());
    }

    let balances = client.balances().await?;
    println!("[3] balance: {} 项资产", balances.len());
    for b in balances.iter().filter(|b| b.total_wallet_balance != "0") {
        println!(
            "    {} total={} umWallet={} crossBorrowed={} crossInterest={}",
            b.asset,
            b.total_wallet_balance,
            b.um_wallet_balance,
            b.cross_margin_borrowed,
            b.cross_margin_interest
        );
    }

    let account = client.account().await?;
    println!(
        "[4] account: uniMMR={} equity={} initMargin={} maintMargin={} status={}",
        account.uni_mmr,
        account.account_equity,
        account.account_initial_margin,
        account.account_maint_margin,
        account.account_status
    );

    let positions = client.um_position_risk(None).await?;
    println!("[5] um positionRisk: {} 项", positions.len());
    for p in &positions {
        println!(
            "    {} amt={} entry={} liq={} uPnL={}",
            p.symbol, p.position_amt, p.entry_price, p.liquidation_price, p.unrealized_profit
        );
    }

    let um_open = client.um_open_orders(&symbol).await?;
    let margin_open = client.margin_open_orders(&symbol).await?;
    println!(
        "[6] openOrders {symbol}: um={} margin={}",
        um_open.len(),
        margin_open.len()
    );

    let mode = client.um_position_mode().await?;
    println!(
        "[7] positionSide/dual: dual={}(必须 false=one-way,否则 reduceOnly 不成立)",
        mode.dual_side_position
    );
    if mode.dual_side_position {
        return Err("账户是 hedge 模式,执行客户端会拒绝启动".into());
    }

    let limits = client.order_rate_limit().await?;
    for l in &limits {
        println!(
            "[8] rateLimit/order: {}/{}x{} limit={} count={:?}",
            l.rate_limit_type, l.interval, l.interval_num, l.limit, l.count
        );
    }

    println!("L1 只读验证全部通过。下一步:与现有生产实现同时刻逐字段 diff。");
    Ok(())
}
