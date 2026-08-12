//! papi REST 响应 wire 模型(第一切片:只读端点)。
//!
//! 字段来源:官方 binance-sdk(MIT)`derivatives_trading_portfolio_margin` OpenAPI
//! 模型 + fuzzy-trading `binance_pm.rs` 生产实测。数值一律 String(Binance wire
//! 惯例),类型化转换放执行层。未知字段被 serde 忽略(不设 deny_unknown_fields,
//! papi 加字段不破解析)。

use serde::Deserialize;

/// Binance 标准错误响应 body。
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceErrorResponse {
    /// 错误码(负数)。
    pub code: i64,
    /// 错误消息。
    pub msg: String,
}

/// `GET /papi/v1/time` 响应。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmServerTime {
    /// 服务器毫秒时间戳。
    pub server_time: i64,
}

/// `GET /papi/v1/balance` 单资产条目(全量返回为数组)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmBalance {
    /// 资产。
    pub asset: String,
    /// 钱包总余额。
    pub total_wallet_balance: String,
    /// 全仓 margin 资产余额。
    pub cross_margin_asset: String,
    /// 全仓 margin 借入(负债本金)。
    pub cross_margin_borrowed: String,
    /// 全仓 margin 可用。
    pub cross_margin_free: String,
    /// 全仓 margin 利息(负债利息)。
    pub cross_margin_interest: String,
    /// 全仓 margin 冻结。
    pub cross_margin_locked: String,
    /// UM 钱包余额。
    pub um_wallet_balance: String,
    /// UM 未实现盈亏。
    #[serde(rename = "umUnrealizedPNL")]
    pub um_unrealized_pnl: String,
    /// CM 钱包余额。
    pub cm_wallet_balance: String,
    /// CM 未实现盈亏。
    #[serde(rename = "cmUnrealizedPNL")]
    pub cm_unrealized_pnl: String,
    /// 负余额(账户负债口径,#2631「负债=负余额」建模的直接来源)。
    #[serde(default)]
    pub negative_balance: Option<String>,
    /// 更新时间(毫秒)。
    pub update_time: i64,
}

/// `GET /papi/v1/account` 响应(账户级风险指标,含 uniMMR)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmAccount {
    /// 统一维持保证金率 uniMMR(风控核心指标)。
    #[serde(rename = "uniMMR")]
    pub uni_mmr: String,
    /// 账户权益(USD)。
    pub account_equity: String,
    /// 不含质押率折扣的实际权益(USD)。
    #[serde(default)]
    pub actual_equity: Option<String>,
    /// 账户初始保证金(USD)。
    pub account_initial_margin: String,
    /// 账户维持保证金(USD)。
    pub account_maint_margin: String,
    /// 账户状态(如 NORMAL / MARGIN_CALL / REDUCE_ONLY / FORCE_LIQUIDATION)。
    pub account_status: String,
    /// 更新时间(毫秒)。
    #[serde(default)]
    pub update_time: Option<i64>,
}

/// `GET /papi/v1/um/positionRisk` 单持仓条目(返回数组)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmUmPositionRisk {
    /// 交易对。
    pub symbol: String,
    /// 持仓量(带符号,空头为负)。
    pub position_amt: String,
    /// 开仓均价。
    pub entry_price: String,
    /// 标记价。
    pub mark_price: String,
    /// 未实现盈亏。
    #[serde(rename = "unRealizedProfit")]
    pub unrealized_profit: String,
    /// 强平价。
    pub liquidation_price: String,
    /// 杠杆。
    pub leverage: String,
    /// 持仓方向(BOTH/LONG/SHORT)。
    pub position_side: String,
    /// 名义价值。
    #[serde(default)]
    pub notional: Option<String>,
    /// 更新时间(毫秒)。
    pub update_time: i64,
}
