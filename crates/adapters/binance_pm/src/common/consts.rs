//! Binance Portfolio Margin(papi)常量与限速配额。

use std::num::NonZeroU32;

use nautilus_network::ratelimiter::quota::Quota;

/// papi 生产 REST base(PM 无 testnet,实测 2026-08-12:testnet host 的 /papi/* 301 到营销页)。
pub const BINANCE_PM_HTTP_URL: &str = "https://papi.binance.com";

/// PM 用户数据流 WS base(`/ws/<listenKey>` 拼接)。
///
/// 注意 fstream 基址拆分迁移(legacy 2026-04-23 后下线,`/pm` 未被点名但需盯
/// changelog)——运行时应允许配置覆盖,不要在调用侧硬编码本常量。
pub const BINANCE_PM_WS_URL: &str = "wss://fstream.binance.com/pm";

/// 全局请求限速 key(papi 6000 权重/min 共享池)。
pub const PM_GLOBAL_RATE_KEY: &str = "binance_pm:global";

/// 下单限速 key(papi 1200 单/min 独立池)。
pub const PM_ORDER_RATE_KEY: &str = "binance_pm:orders";

/// 保守全局请求配额:8 req/s(papi 权重池 6000/min,多数只读端点权重 > 1,
/// 与 fuzzy-trading 生产实测配置同源)。
#[must_use]
pub fn pm_request_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(8).expect("non-zero constant")).expect("valid constant")
}

/// 保守下单配额:4 req/s(papi 下单池 1200/min = 20/s,取 1/5 余量,
/// 与 fuzzy-trading 生产 `PM_SIGNED_RATE_PER_SEC = 4.0` 对齐)。
#[must_use]
pub fn pm_order_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(4).expect("non-zero constant")).expect("valid constant")
}
