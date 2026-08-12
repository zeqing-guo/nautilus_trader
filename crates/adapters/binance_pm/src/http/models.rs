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

/// `POST /papi/v1/listenKey` 响应。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmListenKey {
    /// 用户流 listenKey(60 分钟有效)。
    pub listen_key: String,
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

/// UM 订单(下单/撤单/查单响应与 allOrders 条目同构)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmUmOrder {
    /// 交易对。
    pub symbol: String,
    /// 交易所订单 ID。
    pub order_id: i64,
    /// clientOrderId。
    pub client_order_id: String,
    /// 状态(NEW/PARTIALLY_FILLED/FILLED/CANCELED/EXPIRED/EXPIRED_IN_MATCH…;
    /// wire 层保持 String,执行层收窄且未知值归 Unknown+告警)。
    pub status: String,
    /// 委托价。
    pub price: String,
    /// 成交均价。
    #[serde(default)]
    pub avg_price: Option<String>,
    /// 原始数量。
    pub orig_qty: String,
    /// 已成交数量。
    pub executed_qty: String,
    /// 累计成交金额。
    #[serde(default)]
    pub cum_quote: Option<String>,
    /// GTC/IOC/FOK/GTX/GTD。
    pub time_in_force: String,
    /// LIMIT/MARKET。
    #[serde(rename = "type")]
    pub order_type: String,
    /// 是否 reduceOnly。
    #[serde(default)]
    pub reduce_only: bool,
    /// BUY/SELL。
    pub side: String,
    /// 持仓方向(one-way 恒 BOTH)。
    #[serde(default)]
    pub position_side: Option<String>,
    /// 更新时间(ms)。
    pub update_time: i64,
}

/// margin 订单响应的成交明细(仅 `newOrderRespType=FULL`)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmMarginFill {
    /// 成交价。
    pub price: String,
    /// 成交量。
    pub qty: String,
    /// 手续费。
    pub commission: String,
    /// 手续费资产。
    pub commission_asset: String,
    /// Trade ID(FillReport 幂等键)。
    #[serde(default)]
    pub trade_id: Option<i64>,
}

/// margin 订单(下单/撤单/查单响应同构;历史单 `cummulativeQuoteQty < 0`
/// 表示数据暂不可用)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmMarginOrder {
    /// 交易对。
    pub symbol: String,
    /// 交易所订单 ID。
    pub order_id: i64,
    /// clientOrderId(撤单响应中为撤单请求 id,原单在 `origClientOrderId`)。
    pub client_order_id: String,
    /// 原单 clientOrderId(撤单响应)。
    #[serde(default)]
    pub orig_client_order_id: Option<String>,
    /// 状态。
    pub status: String,
    /// 委托价。
    pub price: String,
    /// 原始数量。
    pub orig_qty: String,
    /// 已成交数量。
    pub executed_qty: String,
    /// 累计成交金额(< 0 = 数据暂不可用)。
    #[serde(default)]
    pub cummulative_quote_qty: Option<String>,
    /// GTC/IOC/FOK。
    #[serde(default)]
    pub time_in_force: Option<String>,
    /// LIMIT/MARKET/LIMIT_MAKER/…。
    #[serde(rename = "type")]
    pub order_type: String,
    /// BUY/SELL。
    pub side: String,
    /// 下单响应时间(ms;查单响应用 updateTime)。
    #[serde(default)]
    pub transact_time: Option<i64>,
    /// 更新时间(ms)。
    #[serde(default)]
    pub update_time: Option<i64>,
    /// 成交明细(仅 FULL;CCXT 认为 papi 不支持 FULL,待 L3 实测)。
    #[serde(default)]
    pub fills: Vec<PmMarginFill>,
}

/// UM 成交条目(`GET /papi/v1/um/userTrades`)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmUmTrade {
    /// 交易对。
    pub symbol: String,
    /// 成交 ID(幂等键)。
    pub id: i64,
    /// 订单 ID。
    pub order_id: i64,
    /// BUY/SELL。
    pub side: String,
    /// 成交价。
    pub price: String,
    /// 成交量。
    pub qty: String,
    /// 成交金额。
    #[serde(default)]
    pub quote_qty: Option<String>,
    /// 已实现盈亏。
    #[serde(default)]
    pub realized_pnl: Option<String>,
    /// 手续费。
    pub commission: String,
    /// 手续费资产。
    pub commission_asset: String,
    /// 成交时间(ms)。
    pub time: i64,
    /// 是否买方。
    pub buyer: bool,
    /// 是否 maker。
    pub maker: bool,
    /// 持仓方向。
    #[serde(default)]
    pub position_side: Option<String>,
}

/// margin 成交条目(`GET /papi/v1/margin/myTrades`)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmMarginTrade {
    /// 交易对。
    pub symbol: String,
    /// 成交 ID(幂等键)。
    pub id: i64,
    /// 订单 ID。
    pub order_id: i64,
    /// 成交价。
    pub price: String,
    /// 成交量。
    pub qty: String,
    /// 手续费。
    pub commission: String,
    /// 手续费资产。
    pub commission_asset: String,
    /// 成交时间(ms)。
    pub time: i64,
    /// 是否买方。
    pub is_buyer: bool,
    /// 是否 maker。
    pub is_maker: bool,
}

/// `GET /papi/v1/um/positionSide/dual` 响应(启动强制校验 one-way 的依据)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmPositionMode {
    /// true = hedge(双向),false = one-way(单向,我方要求)。
    pub dual_side_position: bool,
}

/// `GET /papi/v1/rateLimit/order` 条目(下单限速用量,接健康检查)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmRateLimitOrder {
    /// 限速类型(ORDERS)。
    pub rate_limit_type: String,
    /// 计数周期(MINUTE 等)。
    pub interval: String,
    /// 周期倍数。
    pub interval_num: u32,
    /// 上限。
    pub limit: u32,
    /// 当前用量(部分响应携带)。
    #[serde(default)]
    pub count: Option<u32>,
}

/// `GET /papi/v1/um/income` 条目(FUNDING_FEE 归档;仅保留 3 个月)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmIncome {
    /// 交易对(账户级收益为空)。
    #[serde(default)]
    pub symbol: Option<String>,
    /// 收益类型。
    pub income_type: String,
    /// 金额(带符号)。
    pub income: String,
    /// 资产。
    pub asset: String,
    /// 时间(ms)。
    pub time: i64,
    /// 流水 ID(幂等键)。
    pub tran_id: i64,
    /// 关联成交 ID。
    #[serde(default)]
    pub trade_id: Option<String>,
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
