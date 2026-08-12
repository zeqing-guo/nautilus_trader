//! papi REST 查询/下单参数(serde_urlencoded 序列化,字段名 = 官方参数名)。
//!
//! 参数矩阵依据 fuzzy-trading docs/research-report-mature-system.md §7.3:
//! - UM post-only = `timeInForce=GTX`;margin post-only = `type=LIMIT_MAKER`
//!   (不接受 timeInForce)——两腿两套,勿混;
//! - one-way 模式:`reduceOnly` 可用、`positionSide` 省略;hedge 模式相反。
//!   我方只跑 one-way(启动强制校验),本模块不提供 positionSide;
//! - margin 腿 `sideEffectType` **恒显式 `NO_SIDE_EFFECT`**(绝不借贷铁律;
//!   新增的 AUTO_BORROW_REPAY 比 MARGIN_BUY 更危险,不能依赖服务端默认)。

use serde::Serialize;

/// clientOrderId 严格字符集(UM 正则 `^[.A-Z:/a-z0-9_-]{1,32}$`;margin 文档
/// 未给正则,两腿统一用严格集,现有 ft+ULID 28 字符兼容)。
///
/// # Errors
///
/// 为空、超 32 字符或含非法字符时报错(返回违规原因)。
pub fn validate_client_order_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > 32 {
        return Err(format!("clientOrderId 长度须 1-32,实际 {}", id.len()));
    }
    for c in id.chars() {
        let ok = c.is_ascii_alphanumeric() || matches!(c, '.' | ':' | '/' | '_' | '-');
        if !ok {
            return Err(format!("clientOrderId 含非法字符 {c:?}"));
        }
    }
    Ok(())
}

/// `GET /papi/v1/balance` 参数。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmBalanceParams {
    /// 指定资产(缺省返回全部)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,
}

/// `GET /papi/v1/um/positionRisk` 参数。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmPositionRiskParams {
    /// 指定交易对(缺省返回全部)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// `POST /papi/v1/um/order` 参数(one-way 模式;type 仅 LIMIT/MARKET)。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmUmNewOrderParams {
    /// 交易对。
    pub symbol: String,
    /// BUY/SELL。
    pub side: String,
    /// LIMIT/MARKET。
    #[serde(rename = "type")]
    pub order_type: String,
    /// GTC/IOC/FOK/GTX/GTD(GTX=post-only,穿价同步拒单 -5022 不留痕迹;
    /// GTD 须 goodTillDate ≥ now+600s)。MARKET 不传。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// 数量。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<String>,
    /// 价格(LIMIT 必传;与 priceMatch 互斥)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// 平仓腿恒 true(one-way 模式专用;hedge 模式禁传,我方只跑 one-way)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    /// 我方幂等根(ft+ULID),先过 [`validate_client_order_id`]。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_client_order_id: Option<String>,
    /// ACK/RESULT(UM 无 FULL)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_order_resp_type: Option<String>,
    /// 原生抢队首(QUEUE/OPPONENT 系;不能与 price 同传)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_match: Option<String>,
    /// STP 模式(仅 IOC/GTC/GTD 生效,GTX 不生效)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_trade_prevention_mode: Option<String>,
    /// GTD 到期时刻(ms,秒级精度,须 > now+600s)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub good_till_date: Option<i64>,
}

/// `POST /papi/v1/margin/order` 参数。
///
/// `sideEffectType` 不可配置——恒序列化为 `NO_SIDE_EFFECT`(绝不借贷铁律)。
/// 若未来确需借贷模式,必须新增显式构造器并先过风控评审。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmMarginNewOrderParams {
    /// 交易对。
    pub symbol: String,
    /// BUY/SELL。
    pub side: String,
    /// LIMIT/MARKET/LIMIT_MAKER(LIMIT_MAKER=post-only,不接受 timeInForce,
    /// 穿价拒单 -2010 + "Order would immediately match and take.")。
    #[serde(rename = "type")]
    pub order_type: String,
    /// GTC/IOC/FOK(margin 无 GTX/GTD;LIMIT_MAKER 不传)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// 数量(与 quoteOrderQty 二选一)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<String>,
    /// 报价币数量(仅 MARKET)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_order_qty: Option<String>,
    /// 价格。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// 幂等根,先过 [`validate_client_order_id`]。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_client_order_id: Option<String>,
    /// ACK/RESULT/FULL(FULL 直取 fills;CCXT 认为 papi 不支持 FULL,待 L3
    /// 实测,失败则回退 RESULT)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_order_resp_type: Option<String>,
    /// STP 模式。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_trade_prevention_mode: Option<String>,
    /// 恒 NO_SIDE_EFFECT,私有字段防旁路;经 `new_*` 构造器创建。
    side_effect_type: &'static str,
}

impl PmMarginNewOrderParams {
    /// 构造 margin 下单参数(sideEffectType 恒 NO_SIDE_EFFECT)。
    #[must_use]
    pub fn new(symbol: String, side: String, order_type: String) -> Self {
        Self {
            symbol,
            side,
            order_type,
            time_in_force: None,
            quantity: None,
            quote_order_qty: None,
            price: None,
            new_client_order_id: None,
            new_order_resp_type: None,
            self_trade_prevention_mode: None,
            side_effect_type: "NO_SIDE_EFFECT",
        }
    }
}

/// 撤单/查单通用参数(um 与 margin 同构;orderId 与 origClientOrderId 二选一)。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmOrderRefParams {
    /// 交易对(必传)。
    pub symbol: String,
    /// 交易所订单 ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<i64>,
    /// 我方 clientOrderId(查单裁决的锚点)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orig_client_order_id: Option<String>,
}

impl PmOrderRefParams {
    /// 按 clientOrderId 引用(Reconciler 裁决路径)。
    #[must_use]
    pub fn by_client_order_id(symbol: String, client_order_id: String) -> Self {
        Self {
            symbol,
            order_id: None,
            orig_client_order_id: Some(client_order_id),
        }
    }

    /// 按交易所订单 ID 引用。
    #[must_use]
    pub fn by_order_id(symbol: String, order_id: i64) -> Self {
        Self {
            symbol,
            order_id: Some(order_id),
            orig_client_order_id: None,
        }
    }
}

/// `GET /papi/v1/{um|margin}/allOrders` 参数(lookback 对账)。
///
/// ⚠️ 时间窗:UM 跨度 <7 天;margin 权重 **100**(每分钟最多 60 次,严禁按
/// symbol 循环,须时间窗合并 + 缓存)。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmAllOrdersParams {
    /// 交易对(必传)。
    pub symbol: String,
    /// 起始订单 ID(返回 ≥ 该 id)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<i64>,
    /// 起始时间(ms)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    /// 结束时间(ms)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    /// 条数上限。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// `GET /papi/v1/um/userTrades` / `margin/myTrades` 参数(成交流水对账)。
///
/// ⚠️ 时间窗不对称:UM ≤7 天,**margin <24 小时**(补历史须按天切片);
/// UM 侧 `fromId` 不能与时间窗同传。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmTradesParams {
    /// 交易对(必传)。
    pub symbol: String,
    /// 按订单过滤(仅 margin/myTrades 支持)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<i64>,
    /// 起始时间(ms)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    /// 结束时间(ms)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    /// 起始成交 ID(与时间窗互斥,UM)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_id: Option<i64>,
    /// 条数上限。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// `GET /papi/v1/um/income` 参数(资金费归档;仅保留 3 个月,分页 page+limit)。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmIncomeParams {
    /// 交易对(可选)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// 收益类型(FUNDING_FEE/REALIZED_PNL/COMMISSION/…)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub income_type: Option<String>,
    /// 起始时间(ms)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    /// 结束时间(ms)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    /// 页码(papi income 用 page 而非 fromId)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    /// 条数上限。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// `GET /papi/v1/{um|margin}/openOrders` 参数。
///
/// symbol 必传:margin 侧不带 symbol 按全市场 symbol 数计费(天价);UM 侧
/// 不带权重 40(带 = 1)。我方并发 symbol ≤3,逐 symbol 查更省。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmOpenOrdersParams {
    /// 交易对(强制)。
    pub symbol: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn um_gtx_open_short_serializes_full_matrix() {
        // 开仓 UM post-only 空单:GTX + 不带 positionSide(one-way)。
        let params = PmUmNewOrderParams {
            symbol: "SOLUSDC".to_string(),
            side: "SELL".to_string(),
            order_type: "LIMIT".to_string(),
            time_in_force: Some("GTX".to_string()),
            quantity: Some("1.34".to_string()),
            price: Some("180.50".to_string()),
            reduce_only: Some(false),
            new_client_order_id: Some("ft01J5KXAMPLE0000000000000AB".to_string()),
            new_order_resp_type: Some("RESULT".to_string()),
            price_match: None,
            self_trade_prevention_mode: None,
            good_till_date: None,
        };
        let qs = serde_urlencoded::to_string(&params).unwrap();
        assert_eq!(
            qs,
            "symbol=SOLUSDC&side=SELL&type=LIMIT&timeInForce=GTX&quantity=1.34\
             &price=180.50&reduceOnly=false&newClientOrderId=ft01J5KXAMPLE0000000000000AB\
             &newOrderRespType=RESULT"
        );
    }

    #[test]
    fn um_reduce_only_close_omits_position_side() {
        // 平仓腿:reduceOnly=true;one-way 模式绝不出现 positionSide 参数。
        let params = PmUmNewOrderParams {
            symbol: "SOLUSDC".to_string(),
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            time_in_force: Some("IOC".to_string()),
            quantity: Some("1.34".to_string()),
            price: Some("181.00".to_string()),
            reduce_only: Some(true),
            new_client_order_id: None,
            new_order_resp_type: None,
            price_match: None,
            self_trade_prevention_mode: None,
            good_till_date: None,
        };
        let qs = serde_urlencoded::to_string(&params).unwrap();
        assert!(qs.contains("reduceOnly=true"));
        assert!(!qs.contains("positionSide"));
    }

    #[test]
    fn margin_limit_maker_has_no_tif_and_always_no_side_effect() {
        // margin post-only:LIMIT_MAKER 无 timeInForce;sideEffectType 恒显式。
        let mut params = PmMarginNewOrderParams::new(
            "SOLUSDC".to_string(),
            "BUY".to_string(),
            "LIMIT_MAKER".to_string(),
        );
        params.quantity = Some("1.34".to_string());
        params.price = Some("179.80".to_string());
        params.new_order_resp_type = Some("FULL".to_string());
        let qs = serde_urlencoded::to_string(&params).unwrap();
        assert!(qs.contains("type=LIMIT_MAKER"));
        assert!(qs.contains("sideEffectType=NO_SIDE_EFFECT"));
        assert!(!qs.contains("timeInForce"));
    }

    #[test]
    fn margin_market_ioc_still_no_side_effect() {
        // 受控 IOC 补齐路径同样绝不借贷。
        let mut params = PmMarginNewOrderParams::new(
            "SOLUSDC".to_string(),
            "SELL".to_string(),
            "LIMIT".to_string(),
        );
        params.time_in_force = Some("IOC".to_string());
        params.quantity = Some("0.84".to_string());
        params.price = Some("179.00".to_string());
        let qs = serde_urlencoded::to_string(&params).unwrap();
        assert!(qs.contains("sideEffectType=NO_SIDE_EFFECT"));
        assert!(qs.contains("timeInForce=IOC"));
    }

    #[test]
    fn order_ref_by_client_order_id_for_reconciler() {
        let params = PmOrderRefParams::by_client_order_id(
            "SOLUSDC".to_string(),
            "ft01J5KXAMPLE0000000000000AB".to_string(),
        );
        let qs = serde_urlencoded::to_string(&params).unwrap();
        assert_eq!(
            qs,
            "symbol=SOLUSDC&origClientOrderId=ft01J5KXAMPLE0000000000000AB"
        );
    }

    #[test]
    fn client_order_id_validation_matches_strict_set() {
        // 现有 ft+ULID(28 字符)必须通过。
        assert!(validate_client_order_id("ft01J5KXAMPLE0000000000000AB").is_ok());
        assert!(validate_client_order_id("a.B:c/d_e-1").is_ok());
        // 超长(33)拒绝。
        assert!(validate_client_order_id(&"x".repeat(33)).is_err());
        assert!(validate_client_order_id("").is_err());
        // 非法字符拒绝。
        assert!(validate_client_order_id("bad id").is_err());
        assert!(validate_client_order_id("bad#id").is_err());
        // 系统保留前缀在事件侧识别,不在这里拦(交易所不会拒绝)。
    }
}
