//! PM 用户数据流事件 wire 模型与分发(12 种事件全集)。
//!
//! 事件目录与字段表来源:官方 user-data-streams.md + 官方 SDK OpenAPI 模型
//! (`binance-connector-python` PM websocket_streams models),推导与出处见
//! fuzzy-trading docs/research-report-mature-system.md §7.2。
//!
//! # 铁律(参考实现三家全部踩雷的地方)
//!
//! 1. **分腿判据是两层**:事件名 `e` + 业务单元 `fs`(仅 UM/CM 系事件有;
//!    margin 系事件结构上没有 `fs`,解析器不得因缺 `fs` 报错)。
//! 2. **未知事件名/缺 `e` 字段一律归 [`PmUserStreamEvent::Unknown`] 并由调用方
//!    告警,绝不静默丢弃**(vnpy 对未知订单类型静默 `return` 丢整条回报、
//!    NexusTrader 未知状态解码失败丢消息——都是丢腿事故的温床)。
//! 3. 状态/类型字段在 wire 层保持 String,不做枚举收窄——收窄(以及未知值
//!    归 Unknown+告警)是执行层的职责。

use serde::Deserialize;

/// UM/CM 业务单元标识(margin 系事件没有该字段)。
pub const FS_UM: &str = "UM";
/// COIN-M 业务单元(本项目不交易 CM,事件解析后由调用方过滤)。
pub const FS_CM: &str = "CM";

/// 清算单 clientOrderId 前缀(交易所系统单,不得当作我方订单)。
pub const AUTOCLOSE_PREFIX: &str = "autoclose-";
/// ADL 单 clientOrderId(交易所系统单)。
pub const ADL_CLIENT_ORDER_ID: &str = "adl_autoclose";

/// 分发后的 PM 用户流事件。
#[derive(Debug, Clone)]
pub enum PmUserStreamEvent {
    /// UM/CM 订单更新(空腿主事件)。
    OrderTradeUpdate(Box<PmOrderTradeUpdate>),
    /// UM/CM 余额与持仓更新(含 FUNDING_FEE 实时归因)。
    AccountUpdate(Box<PmAccountUpdate>),
    /// UM/CM 杠杆变更。
    AccountConfigUpdate(PmAccountConfigUpdate),
    /// margin 现货腿订单更新(多头腿主事件)。
    ExecutionReport(Box<PmExecutionReport>),
    /// margin 账户余额快照。
    OutboundAccountPosition(PmOutboundAccountPosition),
    /// margin 余额变动。
    BalanceUpdate(PmBalanceUpdate),
    /// PM 特有:负债变动。我方恒 NO_SIDE_EFFECT,正常运行下**应永不出现**——
    /// 收到即「绝不借贷」不变式被破坏,调用方必须接 Broken 级告警。
    LiabilityChange(PmLiabilityChange),
    /// PM 特有:全仓 margin 挂单占用保证金。
    OpenOrderLoss(PmOpenOrderLoss),
    /// PM 特有:风险等级/uniMMR 告警(REDUCE_ONLY/FORCE_LIQUIDATION 接 KILL)。
    RiskLevelChange(PmRiskLevelChange),
    /// listenKey 过期:与 WS 断线无关,收到后必须换新 key,否则不再有任何事件。
    ListenKeyExpired(PmListenKeyExpired),
    /// 条件单事件(2026-04-28 起废弃迁移到 algo,我方不用;安全忽略但不丢日志)。
    ConditionalOrderTradeUpdate(serde_json::Value),
    /// 算法单事件(替代 conditional,我方不用;安全忽略但不丢日志)。
    AlgoUpdate(serde_json::Value),
    /// 未知事件名或缺 `e` 字段:调用方必须告警 + 落原文,绝不静默丢弃。
    Unknown {
        /// 事件名(缺 `e` 时为 None)。
        event_type: Option<String>,
        /// 报文原文。
        raw: String,
    },
}

/// 只探测 `e` 与 `fs` 的信封。
#[derive(Debug, Deserialize)]
struct EventProbe {
    e: Option<String>,
    #[allow(dead_code)]
    fs: Option<String>,
}

/// 解析一条 PM 用户流报文并按事件名分发。
///
/// 解析失败的已知事件(字段结构对不上)同样归 [`PmUserStreamEvent::Unknown`]
/// ——报文格式漂移必须浮出水面,不能吞掉。
#[must_use]
pub fn parse_user_stream_event(raw: &str) -> PmUserStreamEvent {
    let probe: EventProbe = match serde_json::from_str(raw) {
        Ok(p) => p,
        Err(_) => {
            return PmUserStreamEvent::Unknown {
                event_type: None,
                raw: raw.to_string(),
            };
        }
    };

    let Some(event_type) = probe.e else {
        return PmUserStreamEvent::Unknown {
            event_type: None,
            raw: raw.to_string(),
        };
    };

    macro_rules! parse_or_unknown {
        ($ty:ty, $variant:expr) => {
            match serde_json::from_str::<$ty>(raw) {
                Ok(ev) => $variant(ev),
                Err(_) => PmUserStreamEvent::Unknown {
                    event_type: Some(event_type),
                    raw: raw.to_string(),
                },
            }
        };
    }

    match event_type.as_str() {
        "ORDER_TRADE_UPDATE" => {
            parse_or_unknown!(PmOrderTradeUpdate, |ev| {
                PmUserStreamEvent::OrderTradeUpdate(Box::new(ev))
            })
        }
        "ACCOUNT_UPDATE" => {
            parse_or_unknown!(PmAccountUpdate, |ev| PmUserStreamEvent::AccountUpdate(
                Box::new(ev)
            ))
        }
        "ACCOUNT_CONFIG_UPDATE" => {
            parse_or_unknown!(
                PmAccountConfigUpdate,
                PmUserStreamEvent::AccountConfigUpdate
            )
        }
        "executionReport" => {
            parse_or_unknown!(PmExecutionReport, |ev| {
                PmUserStreamEvent::ExecutionReport(Box::new(ev))
            })
        }
        "outboundAccountPosition" => {
            parse_or_unknown!(
                PmOutboundAccountPosition,
                PmUserStreamEvent::OutboundAccountPosition
            )
        }
        "balanceUpdate" => {
            parse_or_unknown!(PmBalanceUpdate, PmUserStreamEvent::BalanceUpdate)
        }
        "liabilityChange" => {
            parse_or_unknown!(PmLiabilityChange, PmUserStreamEvent::LiabilityChange)
        }
        "openOrderLoss" => {
            parse_or_unknown!(PmOpenOrderLoss, PmUserStreamEvent::OpenOrderLoss)
        }
        "RISK_LEVEL_CHANGE" => {
            parse_or_unknown!(PmRiskLevelChange, PmUserStreamEvent::RiskLevelChange)
        }
        "listenKeyExpired" => {
            parse_or_unknown!(PmListenKeyExpired, PmUserStreamEvent::ListenKeyExpired)
        }
        "CONDITIONAL_ORDER_TRADE_UPDATE" => {
            parse_or_unknown!(
                serde_json::Value,
                PmUserStreamEvent::ConditionalOrderTradeUpdate
            )
        }
        "ALGO_UPDATE" => {
            parse_or_unknown!(serde_json::Value, PmUserStreamEvent::AlgoUpdate)
        }
        _ => PmUserStreamEvent::Unknown {
            event_type: Some(event_type),
            raw: raw.to_string(),
        },
    }
}

// -------------------------------------------------------------------------------------------------
// UM/CM 系事件(带 `fs`)
// -------------------------------------------------------------------------------------------------

/// `ORDER_TRADE_UPDATE`(UM/CM 订单更新)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmOrderTradeUpdate {
    /// 业务单元:UM/CM。
    pub fs: String,
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 撮合时间(ms)。
    #[serde(rename = "T")]
    pub transaction_time: i64,
    /// 订单对象。
    pub o: PmOrderTradeUpdateOrder,
}

impl PmOrderTradeUpdate {
    /// 是否 UM 腿(本项目只交易 UM,CM 事件由调用方过滤并告警)。
    #[must_use]
    pub fn is_um(&self) -> bool {
        self.fs == FS_UM
    }
}

/// `ORDER_TRADE_UPDATE.o` 订单对象。
#[derive(Debug, Clone, Deserialize)]
pub struct PmOrderTradeUpdateOrder {
    /// 交易对。
    pub s: String,
    /// clientOrderId(对账锚点;`autoclose-*`/`adl_autoclose` 是系统单)。
    pub c: String,
    /// 方向 BUY/SELL。
    #[serde(rename = "S")]
    pub side: String,
    /// 订单类型(MARKET/LIMIT/LIQUIDATION)。
    #[serde(rename = "o")]
    pub order_type: String,
    /// Time in force(GTC/IOC/FOK/GTX)。
    #[serde(rename = "f")]
    pub time_in_force: String,
    /// 原始数量。
    pub q: String,
    /// 原始价格。
    pub p: String,
    /// 成交均价。
    pub ap: String,
    /// 执行类型(NEW/CANCELED/CALCULATED/EXPIRED/TRADE/AMENDMENT)。
    pub x: String,
    /// 订单状态(NEW/PARTIALLY_FILLED/FILLED/CANCELED/EXPIRED/EXPIRED_IN_MATCH;
    /// 注意 UM WS 流不会出现 REJECTED——拒单是 REST 同步返回)。
    #[serde(rename = "X")]
    pub order_status: String,
    /// 交易所订单 ID。
    pub i: i64,
    /// 本次成交量。
    pub l: String,
    /// 累计成交量。
    pub z: String,
    /// 本次成交价。
    #[serde(rename = "L")]
    pub last_price: String,
    /// 手续费资产(无手续费不推)。
    #[serde(rename = "N", default)]
    pub commission_asset: Option<String>,
    /// 手续费金额(无手续费不推)。
    #[serde(rename = "n", default)]
    pub commission: Option<String>,
    /// 成交时间(ms)。
    #[serde(rename = "T")]
    pub trade_time: i64,
    /// Trade ID(FillReport 幂等键)。
    pub t: i64,
    /// 是否 maker 方(判 post-only 是否真做了 maker)。
    pub m: bool,
    /// 是否 reduceOnly(平仓腿校验)。
    #[serde(rename = "R")]
    pub reduce_only: bool,
    /// 持仓方向。
    pub ps: String,
    /// 该笔已实现盈亏。
    #[serde(default)]
    pub rp: Option<String>,
    /// GTD 自动撤单时间(仅 GTD)。
    #[serde(default)]
    pub gtd: Option<i64>,
}

impl PmOrderTradeUpdateOrder {
    /// 是否交易所系统单(清算/ADL),不得当作我方 client_order_id 处理。
    #[must_use]
    pub fn is_system_order(&self) -> bool {
        self.c.starts_with(AUTOCLOSE_PREFIX) || self.c == ADL_CLIENT_ORDER_ID
    }
}

/// `ACCOUNT_UPDATE`(UM/CM 余额与持仓)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmAccountUpdate {
    /// 业务单元:UM/CM。
    pub fs: String,
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 撮合时间(ms)。
    #[serde(rename = "T")]
    pub transaction_time: i64,
    /// 更新体。
    pub a: PmAccountUpdateData,
}

/// `ACCOUNT_UPDATE.a`。
#[derive(Debug, Clone, Deserialize)]
pub struct PmAccountUpdateData {
    /// 事件原因(ORDER/FUNDING_FEE/DEPOSIT/…)。FUNDING_FEE 时可做资金费实时归因。
    pub m: String,
    /// 余额数组(FUNDING_FEE 全仓时只有 B 无 P)。
    #[serde(rename = "B", default)]
    pub balances: Vec<PmAccountUpdateBalance>,
    /// 持仓数组。
    #[serde(rename = "P", default)]
    pub positions: Vec<PmAccountUpdatePosition>,
    /// symbol,仅 m=FUNDING_FEE 时推送(2026-08-07 新增)。
    #[serde(rename = "S", default)]
    pub symbol: Option<String>,
}

/// `ACCOUNT_UPDATE.a.B[]`。
#[derive(Debug, Clone, Deserialize)]
pub struct PmAccountUpdateBalance {
    /// 资产。
    pub a: String,
    /// 钱包余额。
    pub wb: String,
    /// 全仓钱包余额。
    pub cw: String,
    /// 除 PnL 与手续费外的余额变动。
    #[serde(default)]
    pub bc: Option<String>,
}

/// `ACCOUNT_UPDATE.a.P[]`。
#[derive(Debug, Clone, Deserialize)]
pub struct PmAccountUpdatePosition {
    /// 交易对。
    pub s: String,
    /// 持仓量(带符号)。
    pub pa: String,
    /// 开仓均价。
    pub ep: String,
    /// 未实现盈亏。
    pub up: String,
    /// 持仓方向。
    pub ps: String,
    /// 盈亏平衡价。
    #[serde(default)]
    pub bep: Option<String>,
    /// 税前累计已实现。
    #[serde(default)]
    pub cr: Option<String>,
}

/// `ACCOUNT_CONFIG_UPDATE`(杠杆变更)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmAccountConfigUpdate {
    /// 业务单元:UM/CM。
    pub fs: String,
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 撮合时间(ms)。
    #[serde(rename = "T")]
    pub transaction_time: i64,
    /// 配置体。
    pub ac: PmAccountConfig,
}

/// `ACCOUNT_CONFIG_UPDATE.ac`。
#[derive(Debug, Clone, Deserialize)]
pub struct PmAccountConfig {
    /// 交易对。
    pub s: String,
    /// 杠杆。
    pub l: i64,
}

// -------------------------------------------------------------------------------------------------
// margin 系事件(无 `fs`)
// -------------------------------------------------------------------------------------------------

/// `executionReport`(margin 现货腿订单更新)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmExecutionReport {
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 交易对。
    pub s: String,
    /// clientOrderId。⚠️ 撤单事件里这是**撤单请求自己的 id**,原单 id 在 `C`
    /// ——用 [`Self::effective_client_order_id`] 取,勿直接读本字段。
    pub c: String,
    /// 方向。
    #[serde(rename = "S")]
    pub side: String,
    /// 订单类型(LIMIT/MARKET/LIMIT_MAKER/…)。
    #[serde(rename = "o")]
    pub order_type: String,
    /// Time in force(GTC/IOC/FOK;margin 无 GTX)。
    #[serde(rename = "f")]
    pub time_in_force: String,
    /// 原始数量。
    pub q: String,
    /// 原始价格。
    pub p: String,
    /// 原 clientOrderId(仅撤单事件可见,是「被撤那张单」的 ID)。
    #[serde(rename = "C", default)]
    pub orig_client_order_id: Option<String>,
    /// 执行类型(NEW/CANCELED/REJECTED/TRADE/EXPIRED/TRADE_PREVENTION;
    /// REJECTED 仅 cancelReplace 场景)。
    pub x: String,
    /// 订单状态。
    #[serde(rename = "X")]
    pub order_status: String,
    /// 拒单原因(仅被拒时可见,值为错误码)。
    #[serde(rename = "r", default)]
    pub reject_reason: Option<String>,
    /// 交易所订单 ID。
    pub i: i64,
    /// 本次成交量。
    pub l: String,
    /// 累计成交量。
    pub z: String,
    /// 本次成交价。
    #[serde(rename = "L")]
    pub last_price: String,
    /// 手续费金额。
    #[serde(rename = "n", default)]
    pub commission: Option<String>,
    /// 手续费资产。
    #[serde(rename = "N", default)]
    pub commission_asset: Option<String>,
    /// 成交时间(ms)。
    #[serde(rename = "T")]
    pub transaction_time: i64,
    /// Trade ID(FillReport 幂等键;非成交事件为 -1)。
    pub t: i64,
    /// 订单是否在簿上。
    pub w: bool,
    /// 是否 maker。
    pub m: bool,
    /// 订单创建时间(ms)。
    #[serde(rename = "O")]
    pub order_creation_time: i64,
    /// 累计成交金额。
    #[serde(rename = "Z")]
    pub cumulative_quote_qty: String,
    /// 本次成交金额。
    #[serde(rename = "Y")]
    pub last_quote_qty: String,
    /// 进簿时间(ms)。
    #[serde(rename = "W", default)]
    pub working_time: Option<i64>,
    /// 过期原因(仅过期可见:LIMIT_MAKER 穿价撤/STP/维护期撤等,原样落库)。
    #[serde(rename = "eR", default)]
    pub expiry_reason: Option<String>,
}

impl PmExecutionReport {
    /// 取该事件真正对应的我方 clientOrderId:撤单事件(`x == "CANCELED"`)里
    /// `c` 是撤单请求的 id,原单 id 在 `C`——两腿提取逻辑必须分开写的原因。
    #[must_use]
    pub fn effective_client_order_id(&self) -> &str {
        if self.x == "CANCELED" {
            self.orig_client_order_id.as_deref().unwrap_or(&self.c)
        } else {
            &self.c
        }
    }
}

/// `outboundAccountPosition`(margin 账户余额快照)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmOutboundAccountPosition {
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 最后账户更新时间(ms)。
    pub u: i64,
    /// updateId。
    #[serde(rename = "U", default)]
    pub update_id: Option<i64>,
    /// 余额数组。
    #[serde(rename = "B", default)]
    pub balances: Vec<PmSpotBalance>,
}

/// margin 余额条目。
#[derive(Debug, Clone, Deserialize)]
pub struct PmSpotBalance {
    /// 资产。
    pub a: String,
    /// 可用。
    pub f: String,
    /// 冻结。
    pub l: String,
}

/// `balanceUpdate`(margin 余额变动)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmBalanceUpdate {
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 资产。
    pub a: String,
    /// 变动额。
    pub d: String,
    /// Clear Time(ms)。
    #[serde(rename = "T")]
    pub clear_time: i64,
    /// updateId。
    #[serde(rename = "U", default)]
    pub update_id: Option<i64>,
}

/// `liabilityChange`(PM 特有:负债变动——我方纪律下应永不出现,收到即告警)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmLiabilityChange {
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 资产。
    pub a: String,
    /// 变动类型。
    pub t: String,
    /// Transaction ID。
    #[serde(rename = "T")]
    pub transaction_id: i64,
    /// 本金。
    #[serde(rename = "p")]
    pub principal: String,
    /// 利息。
    #[serde(rename = "i")]
    pub interest: String,
    /// 总负债。
    #[serde(rename = "l")]
    pub total_liability: String,
}

/// `openOrderLoss`(PM 特有:全仓 margin 挂单占用保证金)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmOpenOrderLoss {
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 占用数组。
    #[serde(rename = "O", default)]
    pub losses: Vec<PmOpenOrderLossEntry>,
}

/// `openOrderLoss.O[]`。
#[derive(Debug, Clone, Deserialize)]
pub struct PmOpenOrderLossEntry {
    /// 资产。
    pub a: String,
    /// 金额。
    pub o: String,
}

// -------------------------------------------------------------------------------------------------
// PM 账户级事件
// -------------------------------------------------------------------------------------------------

/// `RISK_LEVEL_CHANGE`(PM 特有:uniMMR 风险等级告警)。
///
/// `s` 取值 MARGIN_CALL / REDUCE_ONLY / FORCE_LIQUIDATION;后两者接 KILL。
/// 官方警告:剧烈行情下推送时仓位可能已被强平,只作风险指引。
#[derive(Debug, Clone, Deserialize)]
pub struct PmRiskLevelChange {
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
    /// uniMMR 水平。
    pub u: String,
    /// 风险等级。
    pub s: String,
    /// 账户权益(USD)。
    #[serde(default)]
    pub eq: Option<String>,
    /// 不含质押率的实际权益(USD)。
    #[serde(default)]
    pub ae: Option<String>,
    /// 总维持保证金(USD)。
    #[serde(default)]
    pub m: Option<String>,
}

/// `listenKeyExpired`(收到后必须换新 key,与 WS 断线无关)。
#[derive(Debug, Clone, Deserialize)]
pub struct PmListenKeyExpired {
    /// 事件时间(ms)。
    #[serde(rename = "E")]
    pub event_time: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // 黄金样本按官方字段表构造(docs/research-report-mature-system.md §7.2);
    // 真机抓取的 fixture 在 L2 影子运行阶段替换补充。

    #[test]
    fn order_trade_update_um_gtx_maker_fill() {
        let raw = r#"{"e":"ORDER_TRADE_UPDATE","fs":"UM","E":1786500000100,"T":1786500000099,
            "o":{"s":"SOLUSDC","c":"ft01J5KXAMPLE0000000000000AB","S":"SELL","o":"LIMIT","f":"GTX",
            "q":"1.34","p":"180.50","ap":"180.50","sp":"0","x":"TRADE","X":"PARTIALLY_FILLED",
            "i":9988776655,"l":"0.50","z":"0.50","L":"180.50","N":"USDC","n":"0.0000",
            "T":1786500000099,"t":123456789,"b":"0","a":"241.87","m":true,"R":false,
            "ps":"BOTH","rp":"0"}}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::OrderTradeUpdate(ev) => {
                assert!(ev.is_um());
                assert_eq!(ev.o.c, "ft01J5KXAMPLE0000000000000AB");
                assert_eq!(ev.o.time_in_force, "GTX");
                assert!(ev.o.m, "GTX 成交必须是 maker 方");
                assert_eq!(ev.o.t, 123456789);
                assert!(!ev.o.is_system_order());
            }
            other => panic!("expected OrderTradeUpdate, got {other:?}"),
        }
    }

    #[test]
    fn order_trade_update_cm_is_parsed_but_flagged_not_um() {
        let raw = r#"{"e":"ORDER_TRADE_UPDATE","fs":"CM","E":1,"T":1,
            "o":{"s":"BTCUSD_PERP","c":"x","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"1",
            "ap":"0","x":"NEW","X":"NEW","i":1,"l":"0","z":"0","L":"0","T":1,"t":-1,
            "m":false,"R":false,"ps":"BOTH"}}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::OrderTradeUpdate(ev) => assert!(!ev.is_um()),
            other => panic!("expected OrderTradeUpdate, got {other:?}"),
        }
    }

    #[test]
    fn order_trade_update_missing_fs_is_unknown_not_dropped() {
        // UM 系事件缺 fs = 报文格式漂移,必须浮出水面。
        let raw = r#"{"e":"ORDER_TRADE_UPDATE","E":1,"T":1,
            "o":{"s":"X","c":"y","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"1","ap":"0",
            "x":"NEW","X":"NEW","i":1,"l":"0","z":"0","L":"0","T":1,"t":-1,"m":false,
            "R":false,"ps":"BOTH"}}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::Unknown { event_type, .. } => {
                assert_eq!(event_type.as_deref(), Some("ORDER_TRADE_UPDATE"));
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn liquidation_order_is_flagged_as_system() {
        let raw = r#"{"e":"ORDER_TRADE_UPDATE","fs":"UM","E":1,"T":1,
            "o":{"s":"SOLUSDC","c":"autoclose-1786500000000","S":"BUY","o":"LIQUIDATION",
            "f":"IOC","q":"1","p":"0","ap":"0","x":"NEW","X":"NEW","i":1,"l":"0","z":"0",
            "L":"0","T":1,"t":-1,"m":false,"R":false,"ps":"BOTH"}}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::OrderTradeUpdate(ev) => assert!(ev.o.is_system_order()),
            other => panic!("expected OrderTradeUpdate, got {other:?}"),
        }
    }

    #[test]
    fn account_update_funding_fee_has_symbol_and_no_positions() {
        // 全仓 FUNDING_FEE:只有 B(单资产)无 P,带 a.S(2026-08-07 新增)。
        let raw = r#"{"e":"ACCOUNT_UPDATE","fs":"UM","E":1786500000000,"T":1786500000000,
            "a":{"m":"FUNDING_FEE","B":[{"a":"USDC","wb":"1234.5678","cw":"1234.5678"}],
            "S":"SOLUSDC"}}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::AccountUpdate(ev) => {
                assert_eq!(ev.a.m, "FUNDING_FEE");
                assert_eq!(ev.a.symbol.as_deref(), Some("SOLUSDC"));
                assert_eq!(ev.a.balances.len(), 1);
                assert!(ev.a.positions.is_empty());
            }
            other => panic!("expected AccountUpdate, got {other:?}"),
        }
    }

    #[test]
    fn execution_report_canceled_takes_orig_client_order_id() {
        // margin 撤单:c 是撤单请求 id,原单 id 在 C。
        let raw = r#"{"e":"executionReport","E":1786500000000,"s":"SOLUSDC",
            "c":"cancel_req_xyz","S":"BUY","o":"LIMIT_MAKER","f":"GTC","q":"1.34","p":"179.00",
            "C":"ft01J5ORIGINAL000000000000AB","x":"CANCELED","X":"CANCELED","i":112233,
            "l":"0","z":"0","L":"0","T":1786500000000,"t":-1,"w":false,"m":false,
            "O":1786499990000,"Z":"0","Y":"0"}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::ExecutionReport(ev) => {
                assert_eq!(
                    ev.effective_client_order_id(),
                    "ft01J5ORIGINAL000000000000AB"
                );
            }
            other => panic!("expected ExecutionReport, got {other:?}"),
        }
    }

    #[test]
    fn execution_report_expired_in_match_with_expiry_reason() {
        let raw = r#"{"e":"executionReport","E":1,"s":"SOLUSDC","c":"ft01J5X","S":"BUY",
            "o":"LIMIT_MAKER","f":"GTC","q":"1","p":"1","x":"EXPIRED","X":"EXPIRED_IN_MATCH",
            "i":1,"l":"0","z":"0","L":"0","T":1,"t":-1,"w":false,"m":false,"O":1,"Z":"0",
            "Y":"0","eR":"STP_EXPIRED"}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::ExecutionReport(ev) => {
                assert_eq!(ev.order_status, "EXPIRED_IN_MATCH");
                assert_eq!(ev.expiry_reason.as_deref(), Some("STP_EXPIRED"));
                // 非撤单事件从 c 取 id。
                assert_eq!(ev.effective_client_order_id(), "ft01J5X");
            }
            other => panic!("expected ExecutionReport, got {other:?}"),
        }
    }

    #[test]
    fn liability_change_parses_and_carries_liability_fields() {
        // 我方纪律下该事件应永不出现;解析必须成功以便触发 Broken 告警。
        let raw = r#"{"e":"liabilityChange","E":1786500000000,"a":"USDC","t":"BORROW",
            "T":998877,"p":"100.0","i":"0.01","l":"100.01"}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::LiabilityChange(ev) => {
                assert_eq!(ev.a, "USDC");
                assert_eq!(ev.total_liability, "100.01");
            }
            other => panic!("expected LiabilityChange, got {other:?}"),
        }
    }

    #[test]
    fn risk_level_change_reduce_only() {
        let raw = r#"{"e":"RISK_LEVEL_CHANGE","E":1786500000000,"u":"1.25","s":"REDUCE_ONLY",
            "eq":"5900.00","ae":"5800.00","m":"4720.00"}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::RiskLevelChange(ev) => {
                assert_eq!(ev.s, "REDUCE_ONLY");
                assert_eq!(ev.u, "1.25");
            }
            other => panic!("expected RiskLevelChange, got {other:?}"),
        }
    }

    #[test]
    fn open_order_loss_parses() {
        let raw = r#"{"e":"openOrderLoss","E":1,"O":[{"a":"USDC","o":"-12.34"}]}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::OpenOrderLoss(ev) => {
                assert_eq!(ev.losses.len(), 1);
                assert_eq!(ev.losses[0].o, "-12.34");
            }
            other => panic!("expected OpenOrderLoss, got {other:?}"),
        }
    }

    #[test]
    fn listen_key_expired_parses() {
        let raw = r#"{"e":"listenKeyExpired","E":1786500000000}"#;
        assert!(matches!(
            parse_user_stream_event(raw),
            PmUserStreamEvent::ListenKeyExpired(_)
        ));
    }

    #[test]
    fn outbound_account_position_and_balance_update_parse() {
        let raw = r#"{"e":"outboundAccountPosition","E":1,"u":1,"U":42,
            "B":[{"a":"SOL","f":"1.34","l":"0"}]}"#;
        assert!(matches!(
            parse_user_stream_event(raw),
            PmUserStreamEvent::OutboundAccountPosition(_)
        ));
        let raw = r#"{"e":"balanceUpdate","E":1,"a":"USDC","d":"-5.00","T":1}"#;
        assert!(matches!(
            parse_user_stream_event(raw),
            PmUserStreamEvent::BalanceUpdate(_)
        ));
    }

    #[test]
    fn deprecated_conditional_and_algo_are_tolerated() {
        let raw = r#"{"e":"CONDITIONAL_ORDER_TRADE_UPDATE","fs":"UM","E":1,"T":1,"so":{}}"#;
        assert!(matches!(
            parse_user_stream_event(raw),
            PmUserStreamEvent::ConditionalOrderTradeUpdate(_)
        ));
        let raw = r#"{"e":"ALGO_UPDATE","fs":"UM","E":1,"T":1,"ao":{"ia":false}}"#;
        assert!(matches!(
            parse_user_stream_event(raw),
            PmUserStreamEvent::AlgoUpdate(_)
        ));
    }

    #[test]
    fn unknown_event_name_is_surfaced_not_dropped() {
        let raw = r#"{"e":"SOME_FUTURE_EVENT","E":1,"x":{"whatever":true}}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::Unknown { event_type, raw } => {
                assert_eq!(event_type.as_deref(), Some("SOME_FUTURE_EVENT"));
                assert!(raw.contains("SOME_FUTURE_EVENT"));
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn missing_event_field_is_unknown() {
        let raw = r#"{"E":1,"data":"no e field"}"#;
        match parse_user_stream_event(raw) {
            PmUserStreamEvent::Unknown { event_type, .. } => assert!(event_type.is_none()),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn malformed_json_is_unknown() {
        match parse_user_stream_event("not json at all {") {
            PmUserStreamEvent::Unknown { event_type, .. } => assert!(event_type.is_none()),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }
}
