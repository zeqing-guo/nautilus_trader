//! papi 公开端点连通性探针(无需凭证)。
//!
//! 运行:`cargo run --example binance-pm-ping --package nautilus-binance-pm`

use nautilus_binance_pm::http::client::BinancePmHttpClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = BinancePmHttpClient::new(None, None, None, None, Some(10), None)?;

    client.ping().await?;
    println!("papi ping: ok");

    let time = client.server_time().await?;
    println!("papi serverTime: {}", time.server_time);

    Ok(())
}
