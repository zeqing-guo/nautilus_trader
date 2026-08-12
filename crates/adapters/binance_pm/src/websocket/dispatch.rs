//! 用户流事件 → Nautilus 报告对象转换(实时通道;REST 侧在 `http::parse`)。
//!
//! 走报告路径(`send_order_status_report`/`send_fill_report`):引擎经对账管理
//! 器消化报告,天然幂等(`is_duplicate_fill` 按 trade id 去重),与启动/持续
//! 对账同一条代码路径——正确性优先于直发事件的低延迟(本策略非高频)。

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::enums::LiquiditySide;
use nautilus_model::identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId};
use nautilus_model::reports::{FillReport, OrderStatusReport};
use nautilus_model::types::{Currency, Money, Price, Quantity};
use rust_decimal::Decimal;
use std::str::FromStr;

use crate::http::parse::{map_order_side, map_order_status, map_order_type, map_tif};
use crate::websocket::messages::{PmExecutionReport, PmOrderTradeUpdate};

fn ms_to_nanos(ms: i64) -> UnixNanos {
    UnixNanos::from((ms.max(0) as u64) * 1_000_000)
}

fn qty(s: &str, precision: u8) -> anyhow::Result<Quantity> {
    let dec: Decimal = s.parse().map_err(|_| anyhow::anyhow!("非法数量 {s:?}"))?;
    Quantity::from_decimal_dp(dec, precision).map_err(|e| anyhow::anyhow!("数量精度: {e}"))
}

fn price(s: &str, precision: u8) -> anyhow::Result<Price> {
    let dec: Decimal = s.parse().map_err(|_| anyhow::anyhow!("非法价格 {s:?}"))?;
    Price::from_decimal_dp(dec, precision).map_err(|e| anyhow::anyhow!("价格精度: {e}"))
}

/// 手续费:事件未携带(如 maker 返佣 0)时记 0 USD 占位。
fn commission(amount: Option<&str>, asset: Option<&str>) -> Money {
    match (amount, asset) {
        (Some(n), Some(a)) => {
            let currency = Currency::try_from_str(a).unwrap_or(Currency::USD());
            Decimal::from_str(n)
                .ok()
                .and_then(|d| Money::from_decimal(d, currency).ok())
                .unwrap_or_else(|| Money::zero(currency))
        }
        _ => Money::zero(Currency::USD()),
    }
}

/// `ORDER_TRADE_UPDATE`(UM 腿)→ 状态报告 + 可选成交报告(`x == "TRADE"`)。
///
/// # Errors
///
/// 未知枚举/数值解析失败时报错(调用方告警,不静默丢弃)。
pub fn order_update_to_reports(
    ev: &PmOrderTradeUpdate,
    account_id: AccountId,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<(OrderStatusReport, Option<FillReport>)> {
    let o = &ev.o;
    let ts_event = ms_to_nanos(ev.event_time);
    let (order_type, type_post_only) = map_order_type(&o.order_type)?;
    let (tif, tif_post_only) = map_tif(&o.time_in_force)?;
    let side = map_order_side(&o.side)?;
    let status = map_order_status(&o.order_status)?;
    let venue_order_id = VenueOrderId::new(o.i.to_string());

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        Some(ClientOrderId::new(o.c.as_str())),
        venue_order_id,
        side,
        order_type,
        tif,
        status,
        qty(&o.q, size_precision)?,
        qty(&o.z, size_precision)?,
        ts_event,
        ts_event,
        ts_init,
        Some(UUID4::new()),
    )
    .with_post_only(type_post_only || tif_post_only)
    .with_reduce_only(o.reduce_only);

    if let Ok(p) = price(&o.p, price_precision)
        && p.as_decimal() > Decimal::ZERO
    {
        report = report.with_price(p);
    }

    let fill = if o.x == "TRADE" && o.t > 0 {
        Some(FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::new(o.i.to_string()),
            TradeId::new(o.t.to_string()),
            side,
            qty(&o.l, size_precision)?,
            price(&o.last_price, price_precision)?,
            commission(o.commission.as_deref(), o.commission_asset.as_deref()),
            if o.m {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            },
            Some(ClientOrderId::new(o.c.as_str())),
            None,
            ms_to_nanos(o.trade_time),
            ts_init,
            Some(UUID4::new()),
        ))
    } else {
        None
    };

    Ok((report, fill))
}

/// `executionReport`(margin 腿)→ 状态报告 + 可选成交报告。
///
/// clientOrderId 用 [`PmExecutionReport::effective_client_order_id`](撤单事件
/// 原单 id 在 `C`)。
///
/// # Errors
///
/// 未知枚举/数值解析失败时报错。
pub fn execution_report_to_reports(
    ev: &PmExecutionReport,
    account_id: AccountId,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<(OrderStatusReport, Option<FillReport>)> {
    let ts_event = ms_to_nanos(ev.event_time);
    let (order_type, type_post_only) = map_order_type(&ev.order_type)?;
    let (tif, tif_post_only) = map_tif(&ev.time_in_force)?;
    let side = map_order_side(&ev.side)?;
    let status = map_order_status(&ev.order_status)?;
    let client_order_id = ev.effective_client_order_id();

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        Some(ClientOrderId::new(client_order_id)),
        VenueOrderId::new(ev.i.to_string()),
        side,
        order_type,
        tif,
        status,
        qty(&ev.q, size_precision)?,
        qty(&ev.z, size_precision)?,
        ts_event,
        ts_event,
        ts_init,
        Some(UUID4::new()),
    )
    .with_post_only(type_post_only || tif_post_only);

    if let Ok(p) = price(&ev.p, price_precision)
        && p.as_decimal() > Decimal::ZERO
    {
        report = report.with_price(p);
    }

    let fill = if ev.x == "TRADE" && ev.t > 0 {
        Some(FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::new(ev.i.to_string()),
            TradeId::new(ev.t.to_string()),
            side,
            qty(&ev.l, size_precision)?,
            price(&ev.last_price, price_precision)?,
            commission(ev.commission.as_deref(), ev.commission_asset.as_deref()),
            if ev.m {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            },
            Some(ClientOrderId::new(client_order_id)),
            None,
            ms_to_nanos(ev.transaction_time),
            ts_init,
            Some(UUID4::new()),
        ))
    } else {
        None
    };

    Ok((report, fill))
}

#[cfg(test)]
mod tests {
    use nautilus_model::enums::OrderStatus;

    use super::*;
    use crate::websocket::messages::{PmUserStreamEvent, parse_user_stream_event};

    const UM_FILL: &str = r#"{"e":"ORDER_TRADE_UPDATE","fs":"UM","E":1786500000100,"T":1786500000099,
        "o":{"s":"SOLUSDC","c":"ft01J5KXAMPLE0000000000000AB","S":"SELL","o":"LIMIT","f":"GTX",
        "q":"1.34","p":"180.50","ap":"180.50","x":"TRADE","X":"PARTIALLY_FILLED",
        "i":9988776655,"l":"0.50","z":"0.50","L":"180.50","N":"USDC","n":"0.0001",
        "T":1786500000099,"t":123456789,"m":true,"R":false,"ps":"BOTH","rp":"0"}}"#;

    #[test]
    fn um_trade_event_yields_status_and_fill_reports() {
        let PmUserStreamEvent::OrderTradeUpdate(ev) = parse_user_stream_event(UM_FILL) else {
            panic!("fixture 解析失败");
        };
        let (report, fill) = order_update_to_reports(
            &ev,
            AccountId::from("BINANCE_PM-001"),
            InstrumentId::from("SOLUSDC-PERP.BINANCE"),
            2,
            2,
            UnixNanos::default(),
        )
        .unwrap();
        assert_eq!(report.order_status, OrderStatus::PartiallyFilled);
        assert!(report.post_only);
        let fill = fill.expect("TRADE 必须产出 FillReport");
        assert_eq!(fill.trade_id.to_string(), "123456789");
        assert_eq!(fill.liquidity_side, LiquiditySide::Maker);
        assert_eq!(fill.last_qty.to_string(), "0.50");
    }

    #[test]
    fn margin_cancel_event_uses_orig_id_and_no_fill() {
        let raw = r#"{"e":"executionReport","E":1786500000000,"s":"SOLUSDC",
            "c":"cancel_req_xyz","S":"BUY","o":"LIMIT_MAKER","f":"GTC","q":"1.34","p":"179.00",
            "C":"ft01J5ORIGINAL000000000000AB","x":"CANCELED","X":"CANCELED","i":112233,
            "l":"0","z":"0","L":"0","T":1786500000000,"t":-1,"w":false,"m":false,
            "O":1786499990000,"Z":"0","Y":"0"}"#;
        let PmUserStreamEvent::ExecutionReport(ev) = parse_user_stream_event(raw) else {
            panic!("fixture 解析失败");
        };
        let (report, fill) = execution_report_to_reports(
            &ev,
            AccountId::from("BINANCE_PM-001"),
            InstrumentId::from("SOLUSDC.BINANCE"),
            2,
            2,
            UnixNanos::default(),
        )
        .unwrap();
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(
            report.client_order_id.unwrap().to_string(),
            "ft01J5ORIGINAL000000000000AB"
        );
        assert!(fill.is_none());
    }

    #[test]
    fn unknown_status_in_event_errors_loudly() {
        let raw = UM_FILL.replace("PARTIALLY_FILLED", "SOME_FUTURE_STATE");
        let PmUserStreamEvent::OrderTradeUpdate(ev) = parse_user_stream_event(&raw) else {
            panic!("fixture 解析失败");
        };
        assert!(
            order_update_to_reports(
                &ev,
                AccountId::from("BINANCE_PM-001"),
                InstrumentId::from("SOLUSDC-PERP.BINANCE"),
                2,
                2,
                UnixNanos::default(),
            )
            .is_err()
        );
    }
}
