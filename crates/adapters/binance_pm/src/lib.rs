// -------------------------------------------------------------------------------------------------
//  fuzzy-trading fork 自研模块:Binance Portfolio Margin(papi)适配器。
//  独立于上游 crates/adapters/binance,上游文件零改动;复用其 SigningCredential 与公共解析。
//  设计依据:docs/research-report-mature-system.md(fuzzy-trading 仓库)§7-§8。
//  仅内部使用,不发布(publish = false)。
// -------------------------------------------------------------------------------------------------

//! Binance Portfolio Margin(统一账户,papi.binance.com)适配器。
//!
//! PM 账户下 UM 合约与 margin 现货共享保证金:一个账户、一个 venue、两类 instrument,
//! 恰好满足 Nautilus「一个 venue 一个 execution client」的路由约束——同所双腿
//! (margin 现货多头 × UM 永续空头)必须由本 crate 的单一 execution client 管理。
//!
//! 关键事实(实测,2026-08-12):
//! - papi 仅支持 HMAC/RSA 签名(不支持 Ed25519,与经典账户相反);
//! - papi 无公共行情端点(行情走既有 spot/fapi 公共流),但有公开 `/papi/v1/ping`
//!   与 `/papi/v1/time` 可作连通性探针;
//! - PM 无 testnet(testnet.binancefuture.com 的 /papi/* 301 到营销页)。

pub mod common;
pub mod http;
pub mod sdk;
