//! PM 执行客户端配置。
//!
//! 凭证缺省从环境变量解析(`BINANCE_PM_API_KEY` / `BINANCE_PM_HMAC_SECRET`,
//! 与 fuzzy-trading 生产 sops 密钥文件的变量名一致,兼容 `sops exec-env` 注入,
//! 永不落配置文件明文)。WS base 可配(fstream 基址拆分迁移风险,勿硬编码)。

use std::any::Any;

use nautilus_common::factories::ClientConfig;
use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId};

use crate::common::consts::BINANCE_PM_WS_URL;
use crate::websocket::listen_key::{
    DEFAULT_CREATE_RETRY_COOLDOWN_MS, DEFAULT_DEAD_STREAM_AFTER_MS, DEFAULT_KEEPALIVE_INTERVAL_MS,
    DEFAULT_ROTATE_AFTER_MS,
};

/// 凭证环境变量名(API key)。
pub const ENV_PM_API_KEY: &str = "BINANCE_PM_API_KEY";
/// 凭证环境变量名(HMAC secret;papi 不支持 Ed25519)。
pub const ENV_PM_HMAC_SECRET: &str = "BINANCE_PM_HMAC_SECRET";

/// PM 执行客户端配置。
#[derive(Debug, Clone)]
pub struct BinancePmExecClientConfig {
    /// Trader ID。
    pub trader_id: TraderId,
    /// 账户 ID(须与密钥文件 `FT_ACCOUNT` 对应的子账户一致,防串账)。
    pub account_id: AccountId,
    /// API key(None → 读 [`ENV_PM_API_KEY`])。
    pub api_key: Option<String>,
    /// HMAC secret(None → 读 [`ENV_PM_HMAC_SECRET`])。
    pub api_secret: Option<String>,
    /// REST base 覆盖(缺省 papi 生产;PM 无 testnet)。
    pub base_url_http: Option<String>,
    /// 用户流 WS base(缺省 [`BINANCE_PM_WS_URL`];fstream 拆分迁移时改这里)。
    pub base_url_ws: String,
    /// recvWindow(ms,≤60000)。
    pub recv_window_ms: u64,
    /// HTTP 超时(秒)。
    pub http_timeout_secs: u64,
    /// 代理。
    pub proxy_url: Option<String>,
    /// listenKey keepalive 周期(ms)。
    pub keepalive_interval_ms: i64,
    /// 连接主动轮换阈值(ms,须 < 24h)。
    pub rotate_after_ms: i64,
    /// 死流判据(ms,服务端 3min ping 节奏缺席阈值)。
    pub dead_stream_after_ms: i64,
    /// listenKey 重建冷却(ms)。
    pub create_retry_cooldown_ms: i64,
    /// 强制要求 one-way 持仓模式(true 时 hedge 账户拒绝启动)。
    /// 生产账户实测(2026-08-12)为 hedge 模式,适配器两种模式都支持
    /// (hedge:positionSide 必传、reduceOnly 禁传)→ 缺省 false。
    pub enforce_one_way_mode: bool,
    /// mass 对账(引擎不带 instrument 过滤时)覆盖的腿集合。
    ///
    /// **必须显式声明本部署实际交易的 instrument**:cache 里是行情客户端灌入
    /// 的币安全目录(数千 symbol),按 cache 扫描会对每个 symbol 打一次
    /// allOrders/myTrades(margin allOrders 权重 100)→ 秒爆 IP 权重上限
    /// (2026-08-13 生产实测 -1003,6000/min)。为空时 mass 对账直接报错
    /// (fail-closed),拒绝退化成全目录扫描。
    pub reconcile_instrument_ids: Vec<InstrumentId>,
}

impl Default for BinancePmExecClientConfig {
    fn default() -> Self {
        Self {
            trader_id: TraderId::from("TRADER-001"),
            account_id: AccountId::from("BINANCE_PM-001"),
            api_key: None,
            api_secret: None,
            base_url_http: None,
            base_url_ws: BINANCE_PM_WS_URL.to_string(),
            recv_window_ms: 5_000,
            http_timeout_secs: 60,
            proxy_url: None,
            keepalive_interval_ms: DEFAULT_KEEPALIVE_INTERVAL_MS,
            rotate_after_ms: DEFAULT_ROTATE_AFTER_MS,
            dead_stream_after_ms: DEFAULT_DEAD_STREAM_AFTER_MS,
            create_retry_cooldown_ms: DEFAULT_CREATE_RETRY_COOLDOWN_MS,
            enforce_one_way_mode: false,
            reconcile_instrument_ids: Vec::new(),
        }
    }
}

impl BinancePmExecClientConfig {
    /// 解析凭证:显式配置优先,否则读环境变量(sops exec-env 注入)。
    ///
    /// # Errors
    ///
    /// 两处都取不到时报错(信息不含任何密钥内容)。
    pub fn resolve_credentials(&self) -> anyhow::Result<(String, String)> {
        let key = match &self.api_key {
            Some(k) => k.clone(),
            None => std::env::var(ENV_PM_API_KEY).map_err(|_| {
                anyhow::anyhow!("缺少凭证:未配置 api_key 且 {ENV_PM_API_KEY} 未设置")
            })?,
        };
        let secret = match &self.api_secret {
            Some(s) => s.clone(),
            None => std::env::var(ENV_PM_HMAC_SECRET).map_err(|_| {
                anyhow::anyhow!("缺少凭证:未配置 api_secret 且 {ENV_PM_HMAC_SECRET} 未设置")
            })?,
        };
        Ok((key, secret))
    }
}

impl ClientConfig for BinancePmExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_safe() {
        let cfg = BinancePmExecClientConfig::default();
        // 安全缺省:生产 WS base、无明文凭证;持仓模式实测感知(不强制 one-way)。
        assert!(!cfg.enforce_one_way_mode);
        assert_eq!(cfg.base_url_ws, BINANCE_PM_WS_URL);
        assert!(cfg.api_key.is_none() && cfg.api_secret.is_none());
        assert!(cfg.rotate_after_ms < 24 * 60 * 60 * 1000);
    }

    #[test]
    fn explicit_credentials_take_precedence() {
        let cfg = BinancePmExecClientConfig {
            api_key: Some("k".to_string()),
            api_secret: Some("s".to_string()),
            ..Default::default()
        };
        let (k, s) = cfg.resolve_credentials().unwrap();
        assert_eq!((k.as_str(), s.as_str()), ("k", "s"));
    }
}
