//! 官方 `binance-sdk`(MIT,binance/binance-connector-rust)集成层。
//!
//! # 使用边界(资金安全约束,勿越界)
//!
//! - **走 SDK**:全部只读端点(balance/account/positionRisk/allOrders/userTrades/
//!   myTrades/income/openOrders/positionSide/rateLimit…)、对账查询、listenKey
//!   生命周期(POST/PUT/DELETE)。官方每周跟进 Binance schema 变更,模型不必手写。
//! - **不走 SDK(用本 crate `http::client`)**:下单/撤单等**写端点**。原因
//!   (实测其源码 `common/utils.rs` http_request):传输层错误被塌缩为
//!   `ConnectorClientError { msg: format!("HTTP request failed: {e}") }`,丢失了
//!   reqwest 的 is_connect/is_timeout 区分——无法判别「连接失败=确定未达
//!   (Retriable)」与「发出后超时=可能已受理(Unknown)」,而这正是写路径
//!   三分类纪律的判据。HTTP 状态类错误(400/418/429/5xx)它结构化良好,
//!   读路径足够安全(GET 幂等,SDK 内部也只对 GET/DELETE 重试、POST 永不重试)。
//! - **限速**:SDK 自身无限速器。接入执行客户端时,每次 SDK 调用前必须先向本
//!   crate 的 GCRA 限速桶(`PM_GLOBAL_RATE_KEY`)取额度,保证与写路径共享
//!   papi 6000 权重/min 配额。
//!
//! # 版本纪律
//!
//! 依赖钉死 `=68.1.1`(2026-08-07)。官方 major 每周都在涨且含破坏性变更,升级
//! 必须显式 diff 其 CHANGELOG 的 "Derivatives Trading Portfolio Margin" 段落。
//! 注意:SDK 常量里的 PM testnet URL 是代码生成器产物,**实测路由不存在**
//! (2026-08-12:testnet host 的 /papi/* 一律 301 到营销页),恒用 production。

use anyhow::Context;
use binance_sdk::common::config::ConfigurationRestApi;
use binance_sdk::derivatives_trading_portfolio_margin::{
    DerivativesTradingPortfolioMarginRestApi, rest_api::RestApi,
};

/// 构造官方 SDK 的 PM REST client(生产环境,HMAC 凭证)。
///
/// `timeout_ms` 缺省 5000(SDK 默认 1000ms 偏激进,读路径放宽);SDK 内建重试
/// 仅作用于 GET/DELETE,保持默认即可。
///
/// # Errors
///
/// 配置构造失败时报错。
pub fn pm_rest_client(
    api_key: String,
    api_secret: String,
    timeout_ms: Option<u64>,
) -> anyhow::Result<RestApi> {
    let config = ConfigurationRestApi::builder()
        .api_key(api_key)
        .api_secret(api_secret)
        .timeout(timeout_ms.unwrap_or(5000))
        .build()
        .context("构造 binance-sdk ConfigurationRestApi 失败")?;
    Ok(DerivativesTradingPortfolioMarginRestApi::production(config))
}
