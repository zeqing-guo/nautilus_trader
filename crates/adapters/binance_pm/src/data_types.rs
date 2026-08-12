//! PM 专属自定义数据类型(经 `CustomData` 走数据总线)。
//!
//! `AccountState` 没有 metadata 扩展位,uniMMR 等账户级风险指标走 CustomData
//! 旁路(策略/风控/监控经 `subscribe_data` 消费);账户级保证金另发
//! `MarginBalance(instrument_id=None)` 给 risk engine——双轨,见调研报告 §6.4。

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::data::{HasTsInit, custom::CustomDataTrait};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// PM 账户级风险指标快照(REST `/papi/v1/account` 或 WS `RISK_LEVEL_CHANGE`)。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinancePmAccountRisk {
    /// 统一维持保证金率 uniMMR(风控核心;strategy.toml 的 uni_mmr_warn/close
    /// 阈值作用于此)。
    pub uni_mmr: Decimal,
    /// 账户权益(USD)。
    pub account_equity: Decimal,
    /// 不含质押率折扣的实际权益(USD)。
    pub actual_equity: Decimal,
    /// 账户初始保证金(USD)。
    pub account_initial_margin: Decimal,
    /// 账户维持保证金(USD)。
    pub account_maint_margin: Decimal,
    /// 账户状态(NORMAL/MARGIN_CALL/REDUCE_ONLY/FORCE_LIQUIDATION;
    /// 后两者是 KILL 触发源)。
    pub account_status: String,
    /// 事件时间(ns)。
    pub ts_event: UnixNanos,
    /// 初始化时间(ns)。
    pub ts_init: UnixNanos,
}

impl BinancePmAccountRisk {
    /// 构造。
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        uni_mmr: Decimal,
        account_equity: Decimal,
        actual_equity: Decimal,
        account_initial_margin: Decimal,
        account_maint_margin: Decimal,
        account_status: String,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            uni_mmr,
            account_equity,
            actual_equity,
            account_initial_margin,
            account_maint_margin,
            account_status,
            ts_event,
            ts_init,
        }
    }

    /// 是否处于须立即停止开仓的风险等级(KILL 触发)。
    #[must_use]
    pub fn is_kill_level(&self) -> bool {
        self.account_status == "REDUCE_ONLY" || self.account_status == "FORCE_LIQUIDATION"
    }
}

impl HasTsInit for BinancePmAccountRisk {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for BinancePmAccountRisk {
    fn type_name(&self) -> &'static str {
        "BinancePmAccountRisk"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        if let Some(o) = other.as_any().downcast_ref::<Self>() {
            self == o
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::prelude::FromPrimitive;

    use super::*;

    fn risk(status: &str) -> BinancePmAccountRisk {
        BinancePmAccountRisk::new(
            Decimal::from_f64(1.25).unwrap(),
            Decimal::from(5900),
            Decimal::from(5800),
            Decimal::from(1000),
            Decimal::from(4720),
            status.to_string(),
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    #[test]
    fn kill_level_matches_reduce_only_and_liquidation() {
        assert!(!risk("NORMAL").is_kill_level());
        assert!(!risk("MARGIN_CALL").is_kill_level());
        assert!(risk("REDUCE_ONLY").is_kill_level());
        assert!(risk("FORCE_LIQUIDATION").is_kill_level());
    }

    #[test]
    fn custom_data_roundtrip() {
        let r = risk("NORMAL");
        let json = r.to_json().unwrap();
        let back: BinancePmAccountRisk = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
