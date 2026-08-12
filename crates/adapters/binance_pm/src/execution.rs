//! PM 执行客户端:一个 client 管两条腿(margin 现货 + UM 永续)。
//!
//! Nautilus 一个 venue 只允许一个 execution client(engine `register_client`
//! 对重复 venue bail),而 Binance spot/futures client 的 venue 都是 `BINANCE`
//! ——PM 单账户单 client 是同所双腿唯一自洽形态,本类型即该形态的落点:
//! `submit_order` 按 instrument symbol 是否 `-PERP` 后缀分流
//! `/papi/v1/um/order` 与 `/papi/v1/margin/order`。
//!
//! 结构照 `nautilus_binance::futures::execution::BinanceFuturesExecutionClient`
//! (本 fork 的头号参照物);切片推进:A 账户建模(本文件当前)→ B 下单/撤单
//! → C 三类报告生成器 → D 用户流编排(listenKey 状态机 + 事件分发)。

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nautilus_binance::common::consts::BINANCE_VENUE;
use nautilus_common::clients::ExecutionClient;
use nautilus_common::live::runner::get_exec_event_sender;
use nautilus_core::{UUID4, UnixNanos, time::AtomicTime, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, emitter::ExecutionEventEmitter};
use nautilus_model::accounts::AccountAny;
use nautilus_model::enums::{AccountType, OmsType};
use nautilus_model::events::AccountState;
use nautilus_model::identifiers::{AccountId, ClientId, InstrumentId, Venue};
use nautilus_model::types::{AccountBalance, Currency, MarginBalance, Money};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::common::error::BinancePmHttpResult;
use crate::common::tasks::TaskHandles;
use crate::config::BinancePmExecClientConfig;
use crate::data_types::BinancePmAccountRisk;
use crate::http::client::BinancePmHttpClient;
use crate::http::models::{PmAccount, PmBalance};

/// UM 永续 instrument 的 symbol 后缀(上游 `format_instrument_id` 惯例:
/// 现货 `BTCUSDT.BINANCE`,UM 永续 `BTCUSDT-PERP.BINANCE`)。
pub const UM_PERP_SUFFIX: &str = "-PERP";

/// 该 instrument 是否 UM 腿(否则按 margin 现货腿路由)。
///
/// 这是「一个 client 管两条腿」的唯一分流判据——绝不用启发式兜底
/// (vnpy 的坑:未匹配 symbol 静默当 UM 下单)。
#[must_use]
pub fn is_um_leg(instrument_id: &InstrumentId) -> bool {
    instrument_id.symbol.as_str().ends_with(UM_PERP_SUFFIX)
}

/// PM 执行客户端。
pub struct BinancePmExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    #[allow(dead_code)] // 切片 B/D(下单参数、WS 编排)使用。
    config: BinancePmExecClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: Arc<BinancePmHttpClient>,
    cancellation_token: CancellationToken,
    pending_tasks: TaskHandles,
    connected: Arc<AtomicBool>,
}

impl std::fmt::Debug for BinancePmExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BinancePmExecutionClient))
            .field("client_id", &self.core.client_id)
            .field("account_id", &self.core.account_id)
            .finish_non_exhaustive()
    }
}

impl BinancePmExecutionClient {
    /// 构造 PM 执行客户端。
    ///
    /// # Errors
    ///
    /// 凭证解析失败或 HTTP client 构建失败时报错。
    pub fn new(
        core: ExecutionClientCore,
        config: BinancePmExecClientConfig,
    ) -> anyhow::Result<Self> {
        let (api_key, api_secret) = config.resolve_credentials()?;
        let clock = get_atomic_clock_realtime();

        let http_client = BinancePmHttpClient::new(
            Some(api_key),
            Some(api_secret),
            config.base_url_http.clone(),
            Some(config.recv_window_ms),
            Some(config.http_timeout_secs),
            config.proxy_url.clone(),
        )?;

        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client: Arc::new(http_client),
            cancellation_token: CancellationToken::new(),
            pending_tasks: TaskHandles::default(),
            connected: Arc::new(AtomicBool::new(false)),
        })
    }

    #[allow(dead_code)] // 切片 B(submit/cancel)启用。
    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.pending_tasks.spawn(description, fut);
    }

    /// 拉取账户快照并发射 `AccountState`(启动/对账/`ACCOUNT_UPDATE` 全量重建
    /// 共用;`MarginAccount.apply()` 是替换语义,必须每次发全量)。
    ///
    /// # Errors
    ///
    /// REST 失败时报错。
    pub async fn refresh_account_state(&self) -> BinancePmHttpResult<()> {
        let balances = self.http_client.balances().await?;
        let account = self.http_client.account().await?;
        let ts_now = self.clock.get_time_ns();

        let state = build_account_state(
            &balances,
            &account,
            self.core.account_id,
            self.core.account_type,
            ts_now,
        );
        self.emitter.emit_account_state(
            state.balances.clone(),
            state.margins.clone(),
            true,
            ts_now,
        );

        let risk = build_account_risk(&account, ts_now);
        if let Some(risk) = risk {
            if risk.is_kill_level() {
                // KILL 触发源:账户已进 REDUCE_ONLY/FORCE_LIQUIDATION。
                log::error!(
                    "PM 账户风险等级 {}(uniMMR={}),KILL 级告警",
                    risk.account_status,
                    risk.uni_mmr
                );
            }
            // 后续切片 D:经 data bus 发布 CustomData 给策略/风控/监控。
        }
        Ok(())
    }
}

/// 把 papi 余额/账户快照映射为 `AccountState`。
///
/// 负债建模(#2631 方向,model 层已支持负 total):每资产
/// `total = totalWalletBalance - crossMarginBorrowed - crossMarginInterest`,
/// 借入使 total 为负、`free` 承载缺口;账户级保证金以 USD 两条
/// `MarginBalance(instrument_id=None)` 上报给 risk engine。
///
/// ⚠️ L1 校准项:free 的口径(crossMarginFree + umWalletBalance)按官方字段
/// 语义推导,须在 L1 用现有生产实现做 oracle 逐字段 diff 后定稿。
#[must_use]
pub fn build_account_state(
    balances: &[PmBalance],
    account: &PmAccount,
    account_id: AccountId,
    account_type: AccountType,
    ts_now: UnixNanos,
) -> AccountState {
    let mut out_balances: Vec<AccountBalance> = Vec::new();

    for b in balances {
        let Some(currency) = Currency::try_from_str(&b.asset) else {
            log::warn!("未知资产 {},跳过(须登记 Currency)", b.asset);
            continue;
        };

        let total_wallet = parse_dec(&b.total_wallet_balance);
        let borrowed = parse_dec(&b.cross_margin_borrowed) + parse_dec(&b.cross_margin_interest);
        let total = total_wallet - borrowed;
        let free = parse_dec(&b.cross_margin_free) + parse_dec(&b.um_wallet_balance);

        if total.is_zero() && free.is_zero() {
            continue;
        }

        let free_capped = free.min(total.max(Decimal::ZERO));
        match AccountBalance::from_total_and_free(total, free_capped, currency) {
            Ok(bal) => out_balances.push(bal),
            Err(e) => log::warn!("资产 {} AccountBalance 构建失败:{e}", b.asset),
        }
    }

    // 账户级(cross)保证金:PM 以 USD 计价,uniMMR 的分子分母。
    let usd = Currency::USD();
    let initial = Money::from_decimal(parse_dec(&account.account_initial_margin), usd)
        .unwrap_or_else(|_| Money::zero(usd));
    let maintenance = Money::from_decimal(parse_dec(&account.account_maint_margin), usd)
        .unwrap_or_else(|_| Money::zero(usd));
    let margins = if initial.is_zero() && maintenance.is_zero() {
        Vec::new()
    } else {
        vec![MarginBalance::new(initial, maintenance, None)]
    };

    AccountState::new(
        account_id,
        account_type,
        out_balances,
        margins,
        true, // venue 上报,引擎不再本地推算
        UUID4::new(),
        ts_now,
        ts_now,
        None,
    )
}

/// 把 `/papi/v1/account` 映射为 uniMMR 风险快照(CustomData 旁路载荷)。
#[must_use]
pub fn build_account_risk(account: &PmAccount, ts_now: UnixNanos) -> Option<BinancePmAccountRisk> {
    let uni_mmr = Decimal::from_str(&account.uni_mmr).ok()?;
    Some(BinancePmAccountRisk::new(
        uni_mmr,
        parse_dec(&account.account_equity),
        account
            .actual_equity
            .as_deref()
            .map(parse_dec_str)
            .unwrap_or_default(),
        parse_dec(&account.account_initial_margin),
        parse_dec(&account.account_maint_margin),
        account.account_status.clone(),
        ts_now,
        ts_now,
    ))
}

fn parse_dec(s: &str) -> Decimal {
    parse_dec_str(s)
}

fn parse_dec_str(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap_or_default()
}

impl ExecutionClient for BinancePmExecutionClient {
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *BINANCE_VENUE
    }

    fn oms_type(&self) -> OmsType {
        // one-way 单向净持仓(启动强制校验 positionSide/dual)。
        OmsType::Netting
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        log::info!(
            "Started: client_id={}, account_id={}(PM 单 client 双腿)",
            self.core.client_id,
            self.core.account_id,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        self.cancellation_token.cancel();
        self.pending_tasks.abort_all();
        self.core.set_stopped();
        self.core.set_disconnected();
        self.connected.store(false, Ordering::Relaxed);
        log::info!("Stopped: client_id={}", self.core.client_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::Symbol;

    use super::*;

    fn iid(symbol: &str) -> InstrumentId {
        InstrumentId::new(Symbol::from(symbol), *BINANCE_VENUE)
    }

    #[test]
    fn leg_routing_is_suffix_exact_no_heuristics() {
        assert!(is_um_leg(&iid("SOLUSDC-PERP")));
        assert!(is_um_leg(&iid("BTCUSDT-PERP")));
        // 现货腿(margin)。
        assert!(!is_um_leg(&iid("SOLUSDC")));
        assert!(!is_um_leg(&iid("BTCUSDT")));
        // 形近但非后缀的不许误判。
        assert!(!is_um_leg(&iid("PERPUSDT")));
    }

    fn balance_fixture() -> PmBalance {
        PmBalance {
            asset: "USDC".to_string(),
            total_wallet_balance: "1000.5".to_string(),
            cross_margin_asset: "400.5".to_string(),
            cross_margin_borrowed: "0".to_string(),
            cross_margin_free: "400.5".to_string(),
            cross_margin_interest: "0".to_string(),
            cross_margin_locked: "0".to_string(),
            um_wallet_balance: "600.0".to_string(),
            um_unrealized_pnl: "12.3".to_string(),
            cm_wallet_balance: "0".to_string(),
            cm_unrealized_pnl: "0".to_string(),
            negative_balance: Some("0".to_string()),
            update_time: 1,
        }
    }

    fn account_fixture() -> PmAccount {
        PmAccount {
            uni_mmr: "5.4321".to_string(),
            account_equity: "6000".to_string(),
            actual_equity: Some("5900".to_string()),
            account_initial_margin: "1000".to_string(),
            account_maint_margin: "472".to_string(),
            account_status: "NORMAL".to_string(),
            update_time: Some(1),
        }
    }

    #[test]
    fn account_state_reports_account_level_margin_in_usd() {
        let state = build_account_state(
            &[balance_fixture()],
            &account_fixture(),
            AccountId::from("BINANCE_PM-001"),
            AccountType::Margin,
            UnixNanos::default(),
        );
        assert_eq!(state.balances.len(), 1);
        assert_eq!(state.margins.len(), 1);
        // 账户级:instrument_id 必须为 None(cross margin 语义)。
        assert!(state.margins[0].instrument_id.is_none());
        assert!(state.is_reported);
    }

    #[test]
    fn borrowed_assets_produce_negative_total() {
        // 负债 = 负余额(#2631):borrowed+interest > wallet 时 total 为负。
        let mut b = balance_fixture();
        b.total_wallet_balance = "10".to_string();
        b.cross_margin_borrowed = "100".to_string();
        b.cross_margin_interest = "0.5".to_string();
        b.cross_margin_free = "0".to_string();
        b.um_wallet_balance = "0".to_string();
        let state = build_account_state(
            &[b],
            &account_fixture(),
            AccountId::from("BINANCE_PM-001"),
            AccountType::Margin,
            UnixNanos::default(),
        );
        assert_eq!(state.balances.len(), 1);
        assert!(state.balances[0].total.as_decimal() < Decimal::ZERO);
    }

    #[test]
    fn account_risk_parses_uni_mmr_and_kill_level() {
        let risk = build_account_risk(&account_fixture(), UnixNanos::default()).unwrap();
        assert_eq!(risk.uni_mmr.to_string(), "5.4321");
        assert!(!risk.is_kill_level());

        let mut acc = account_fixture();
        acc.account_status = "REDUCE_ONLY".to_string();
        assert!(
            build_account_risk(&acc, UnixNanos::default())
                .unwrap()
                .is_kill_level()
        );
    }
}
