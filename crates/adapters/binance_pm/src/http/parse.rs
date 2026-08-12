//! papi wire 模型 → Nautilus 报告对象转换(对账的原料,必须做扎实)。
//!
//! 铁律:venue→内部枚举的映射**未命中一律返回 Err**,由调用方记错误并告警,
//! 绝不静默丢弃、绝不启发式兜底(vnpy 静默丢 GTX 回报、NexusTrader 解码失败
//! 丢整条消息——两类事故的对照点)。

use anyhow::Context;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::enums::{
    LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSideSpecified, TimeInForce,
};
use nautilus_model::identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId};
use nautilus_model::reports::{FillReport, OrderStatusReport, PositionStatusReport};
use nautilus_model::types::{Currency, Money, Price, Quantity};
use rust_decimal::Decimal;

use crate::http::models::{PmMarginOrder, PmMarginTrade, PmUmOrder, PmUmPositionRisk, PmUmTrade};

/// 订单状态映射(UM 与 margin 共用同一状态词表)。
///
/// `EXPIRED_IN_MATCH`(STP 触发过期)与 `EXPIRED` 同归 Expired——成因不同但
/// 对账语义相同(无挂单,已成交部分以 fills 为准)。
fn map_order_status(status: &str) -> anyhow::Result<OrderStatus> {
    match status {
        "NEW" => Ok(OrderStatus::Accepted),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" => Ok(OrderStatus::Canceled),
        "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(OrderStatus::Expired),
        "REJECTED" => Ok(OrderStatus::Rejected),
        other => anyhow::bail!("未知订单状态 {other:?}(papi 词表漂移,须告警并升级映射)"),
    }
}

fn map_order_side(side: &str) -> anyhow::Result<OrderSide> {
    match side {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        other => anyhow::bail!("未知订单方向 {other:?}"),
    }
}

/// 订单类型映射;返回 `(类型, post_only)`。
///
/// UM 的 post-only 藏在 TIF(GTX),margin 的藏在类型(LIMIT_MAKER);
/// `LIQUIDATION` 是交易所强平单(系统单),映射为 Market。
fn map_order_type(order_type: &str) -> anyhow::Result<(OrderType, bool)> {
    match order_type {
        "LIMIT" => Ok((OrderType::Limit, false)),
        "MARKET" => Ok((OrderType::Market, false)),
        "LIMIT_MAKER" => Ok((OrderType::Limit, true)),
        "LIQUIDATION" => Ok((OrderType::Market, false)),
        other => anyhow::bail!("未知订单类型 {other:?}"),
    }
}

/// TIF 映射;返回 `(TIF, post_only)`(GTX = GTC + post_only)。
fn map_tif(tif: &str) -> anyhow::Result<(TimeInForce, bool)> {
    match tif {
        "GTC" => Ok((TimeInForce::Gtc, false)),
        "IOC" => Ok((TimeInForce::Ioc, false)),
        "FOK" => Ok((TimeInForce::Fok, false)),
        "GTX" => Ok((TimeInForce::Gtc, true)),
        "GTD" => Ok((TimeInForce::Gtd, false)),
        other => anyhow::bail!("未知 TIF {other:?}"),
    }
}

fn ms_to_nanos(ms: i64) -> UnixNanos {
    UnixNanos::from((ms.max(0) as u64) * 1_000_000)
}

fn parse_qty(s: &str, precision: u8, what: &str) -> anyhow::Result<Quantity> {
    let dec: Decimal = s.parse().with_context(|| format!("非法 {what}: {s:?}"))?;
    Quantity::from_decimal_dp(dec, precision).with_context(|| format!("{what} 精度转换失败"))
}

fn parse_opt_price(s: &str, precision: u8) -> anyhow::Result<Option<Price>> {
    if s.is_empty() {
        return Ok(None);
    }
    let dec: Decimal = s.parse().with_context(|| format!("非法价格: {s:?}"))?;
    if dec == Decimal::ZERO {
        return Ok(None);
    }
    Ok(Some(
        Price::from_decimal_dp(dec, precision).context("价格精度转换失败")?,
    ))
}

impl PmUmOrder {
    /// 转 `OrderStatusReport`(UM 腿)。
    ///
    /// # Errors
    ///
    /// 未知枚举/数值解析失败时报错(调用方记错误并告警,不得静默丢弃)。
    pub fn to_order_status_report(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let ts_event = ms_to_nanos(self.update_time);
        let (order_type, type_post_only) = map_order_type(&self.order_type)?;
        let (tif, tif_post_only) = map_tif(&self.time_in_force)?;
        let status = map_order_status(&self.status)?;

        let mut report = OrderStatusReport::new(
            account_id,
            instrument_id,
            Some(ClientOrderId::new(self.client_order_id.as_str())),
            VenueOrderId::new(self.order_id.to_string()),
            map_order_side(&self.side)?,
            order_type,
            tif,
            status,
            parse_qty(&self.orig_qty, size_precision, "origQty")?,
            parse_qty(&self.executed_qty, size_precision, "executedQty")?,
            ts_event,
            ts_event,
            ts_init,
            Some(UUID4::new()),
        )
        .with_post_only(type_post_only || tif_post_only)
        .with_reduce_only(self.reduce_only);

        if let Some(price) = parse_opt_price(&self.price, price_precision)? {
            report = report.with_price(price);
        }
        if let Some(avg) = self.avg_price.as_deref() {
            if let Ok(dec) = avg.parse::<Decimal>()
                && dec > Decimal::ZERO
            {
                report = report.with_avg_px(dec);
            }
        }
        Ok(report)
    }
}

impl PmMarginOrder {
    /// 转 `OrderStatusReport`(margin 腿)。
    ///
    /// clientOrderId 取 `origClientOrderId`(撤单响应中 `clientOrderId` 是撤单
    /// 请求自己的 id)优先——与 WS `executionReport` 的 `C`/`c` 同一个坑。
    ///
    /// # Errors
    ///
    /// 未知枚举/数值解析失败时报错。
    pub fn to_order_status_report(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let ts_ms = self.update_time.or(self.transact_time).unwrap_or_default();
        let ts_event = ms_to_nanos(ts_ms);
        let (order_type, type_post_only) = map_order_type(&self.order_type)?;
        let (tif, tif_post_only) = match self.time_in_force.as_deref() {
            Some(t) => map_tif(t)?,
            // LIMIT_MAKER 响应可能不带 TIF(现货惯例)。
            None => (TimeInForce::Gtc, false),
        };
        let status = map_order_status(&self.status)?;
        let client_order_id = self
            .orig_client_order_id
            .as_deref()
            .unwrap_or(&self.client_order_id);

        let mut report = OrderStatusReport::new(
            account_id,
            instrument_id,
            Some(ClientOrderId::new(client_order_id)),
            VenueOrderId::new(self.order_id.to_string()),
            map_order_side(&self.side)?,
            order_type,
            tif,
            status,
            parse_qty(&self.orig_qty, size_precision, "origQty")?,
            parse_qty(&self.executed_qty, size_precision, "executedQty")?,
            ts_event,
            ts_event,
            ts_init,
            Some(UUID4::new()),
        )
        .with_post_only(type_post_only || tif_post_only);

        if let Some(price) = parse_opt_price(&self.price, price_precision)? {
            report = report.with_price(price);
        }
        Ok(report)
    }
}

impl PmUmTrade {
    /// 转 `FillReport`(UM 腿;trade id 为幂等键)。
    ///
    /// # Errors
    ///
    /// 未知枚举/数值解析失败时报错。
    pub fn to_fill_report(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> anyhow::Result<FillReport> {
        let commission_currency = Currency::try_from_str(&self.commission_asset)
            .with_context(|| format!("未知手续费资产 {}", self.commission_asset))?;
        let commission_dec: Decimal = self.commission.parse().context("非法手续费")?;
        let commission = Money::from_decimal(commission_dec, commission_currency)
            .context("手续费金额转换失败")?;

        Ok(FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::new(self.order_id.to_string()),
            TradeId::new(self.id.to_string()),
            map_order_side(&self.side)?,
            parse_qty(&self.qty, size_precision, "qty")?,
            parse_opt_price(&self.price, price_precision)?.context("成交价不能为空")?,
            commission,
            if self.maker {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            },
            None,
            None,
            ms_to_nanos(self.time),
            ts_init,
            Some(UUID4::new()),
        ))
    }
}

impl PmMarginTrade {
    /// 转 `FillReport`(margin 腿;side 由 `is_buyer` 推导)。
    ///
    /// # Errors
    ///
    /// 未知枚举/数值解析失败时报错。
    pub fn to_fill_report(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> anyhow::Result<FillReport> {
        let commission_currency = Currency::try_from_str(&self.commission_asset)
            .with_context(|| format!("未知手续费资产 {}", self.commission_asset))?;
        let commission_dec: Decimal = self.commission.parse().context("非法手续费")?;
        let commission = Money::from_decimal(commission_dec, commission_currency)
            .context("手续费金额转换失败")?;

        Ok(FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::new(self.order_id.to_string()),
            TradeId::new(self.id.to_string()),
            if self.is_buyer {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            parse_qty(&self.qty, size_precision, "qty")?,
            parse_opt_price(&self.price, price_precision)?.context("成交价不能为空")?,
            commission,
            if self.is_maker {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            },
            None,
            None,
            ms_to_nanos(self.time),
            ts_init,
            Some(UUID4::new()),
        ))
    }
}

impl PmUmPositionRisk {
    /// 转 `PositionStatusReport`(one-way 净持仓;`positionAmt` 符号定方向)。
    ///
    /// # Errors
    ///
    /// 数值解析失败时报错。
    pub fn to_position_status_report(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        size_precision: u8,
        ts_now: UnixNanos,
    ) -> anyhow::Result<PositionStatusReport> {
        let amt: Decimal = self.position_amt.parse().context("非法 positionAmt")?;
        let side = if amt > Decimal::ZERO {
            PositionSideSpecified::Long
        } else if amt < Decimal::ZERO {
            PositionSideSpecified::Short
        } else {
            PositionSideSpecified::Flat
        };
        let entry: Decimal = self.entry_price.parse().unwrap_or_default();
        let avg_px_open = if entry > Decimal::ZERO {
            Some(entry)
        } else {
            None
        };

        Ok(PositionStatusReport::new(
            account_id,
            instrument_id,
            side,
            Quantity::from_decimal_dp(amt.abs(), size_precision).context("positionAmt 精度")?,
            ts_now,
            ts_now,
            Some(UUID4::new()),
            None,
            avg_px_open,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_enums_error_never_silent() {
        assert!(map_order_status("SOME_NEW_STATUS").is_err());
        assert!(map_order_type("TRAILING_STOP_MARKET").is_err());
        assert!(map_tif("GTE_GTC").is_err());
        assert!(map_order_side("BOTH").is_err());
    }

    #[test]
    fn gtx_and_limit_maker_both_mean_post_only() {
        assert_eq!(map_tif("GTX").unwrap(), (TimeInForce::Gtc, true));
        assert_eq!(
            map_order_type("LIMIT_MAKER").unwrap(),
            (OrderType::Limit, true)
        );
    }

    #[test]
    fn expired_in_match_maps_to_expired() {
        assert_eq!(
            map_order_status("EXPIRED_IN_MATCH").unwrap(),
            OrderStatus::Expired
        );
    }

    #[test]
    fn um_order_report_roundtrip() {
        let order = PmUmOrder {
            symbol: "SOLUSDC".to_string(),
            order_id: 99,
            client_order_id: "ft01J5KXAMPLE0000000000000AB".to_string(),
            status: "PARTIALLY_FILLED".to_string(),
            price: "180.50".to_string(),
            avg_price: Some("180.50".to_string()),
            orig_qty: "1.34".to_string(),
            executed_qty: "0.50".to_string(),
            cum_quote: Some("90.25".to_string()),
            time_in_force: "GTX".to_string(),
            order_type: "LIMIT".to_string(),
            reduce_only: false,
            side: "SELL".to_string(),
            position_side: Some("BOTH".to_string()),
            update_time: 1_786_500_000_000,
        };
        let report = order
            .to_order_status_report(
                AccountId::from("BINANCE_PM-001"),
                InstrumentId::from("SOLUSDC-PERP.BINANCE"),
                2,
                2,
                UnixNanos::default(),
            )
            .unwrap();
        assert_eq!(report.order_status, OrderStatus::PartiallyFilled);
        assert!(report.post_only, "GTX 必须标记 post_only");
        assert_eq!(report.filled_qty.to_string(), "0.50");
    }

    #[test]
    fn margin_cancel_report_uses_orig_client_order_id() {
        let order = PmMarginOrder {
            symbol: "SOLUSDC".to_string(),
            order_id: 100,
            client_order_id: "cancel_req_x".to_string(),
            orig_client_order_id: Some("ft01J5ORIGINAL000000000000AB".to_string()),
            status: "CANCELED".to_string(),
            price: "179.80".to_string(),
            orig_qty: "1.34".to_string(),
            executed_qty: "0".to_string(),
            cummulative_quote_qty: Some("0".to_string()),
            time_in_force: None,
            order_type: "LIMIT_MAKER".to_string(),
            side: "BUY".to_string(),
            transact_time: Some(1),
            update_time: None,
            fills: Vec::new(),
        };
        let report = order
            .to_order_status_report(
                AccountId::from("BINANCE_PM-001"),
                InstrumentId::from("SOLUSDC.BINANCE"),
                2,
                2,
                UnixNanos::default(),
            )
            .unwrap();
        assert_eq!(
            report.client_order_id.unwrap().to_string(),
            "ft01J5ORIGINAL000000000000AB"
        );
        assert!(report.post_only, "LIMIT_MAKER 必须标记 post_only");
    }

    #[test]
    fn position_report_side_from_signed_amount() {
        let mut pos = PmUmPositionRisk {
            symbol: "SOLUSDC".to_string(),
            position_amt: "-1.34".to_string(),
            entry_price: "180.00".to_string(),
            mark_price: "179.00".to_string(),
            unrealized_profit: "1.34".to_string(),
            liquidation_price: "0".to_string(),
            leverage: "3".to_string(),
            position_side: "BOTH".to_string(),
            notional: None,
            update_time: 1,
        };
        let account = AccountId::from("BINANCE_PM-001");
        let iid = InstrumentId::from("SOLUSDC-PERP.BINANCE");
        let r = pos
            .to_position_status_report(account, iid, 2, UnixNanos::default())
            .unwrap();
        assert_eq!(r.position_side, PositionSideSpecified::Short);
        assert_eq!(r.quantity.to_string(), "1.34");

        pos.position_amt = "0".to_string();
        let r = pos
            .to_position_status_report(account, iid, 2, UnixNanos::default())
            .unwrap();
        assert_eq!(r.position_side, PositionSideSpecified::Flat);
    }
}
