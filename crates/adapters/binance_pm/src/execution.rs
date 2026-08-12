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
use nautilus_binance::common::symbol::format_binance_symbol;
use nautilus_common::clients::ExecutionClient;
use nautilus_common::live::runner::get_exec_event_sender;
use nautilus_common::messages::execution::{CancelAllOrders, CancelOrder, SubmitOrder};
use nautilus_core::{UUID4, UnixNanos, time::AtomicTime, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, emitter::ExecutionEventEmitter};
use nautilus_model::accounts::AccountAny;
use nautilus_model::enums::{AccountType, OmsType, OrderSide, OrderType, TimeInForce};
use nautilus_model::events::{AccountState, OrderEventAny, OrderRejected};
use nautilus_model::identifiers::{AccountId, ClientId, InstrumentId, Venue};
use nautilus_model::orders::Order;
use nautilus_model::types::{AccountBalance, Currency, MarginBalance, Money};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::common::error::{
    BinancePmHttpError, BinancePmHttpResult, CODE_GTX_REJECT, CODE_NEW_ORDER_REJECTED,
    MSG_WOULD_IMMEDIATELY_MATCH, PmOutcome,
};
use crate::common::tasks::TaskHandles;
use crate::config::BinancePmExecClientConfig;
use crate::data_types::BinancePmAccountRisk;
use crate::http::client::BinancePmHttpClient;
use crate::http::models::{PmAccount, PmBalance};
use crate::http::query::{
    PmMarginNewOrderParams, PmOrderRefParams, PmUmNewOrderParams, validate_client_order_id,
};

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
        self.emitter
            .emit_account_state(state.balances, state.margins, true, ts_now);

        // KILL 触发源:账户已进 REDUCE_ONLY/FORCE_LIQUIDATION。
        // 后续切片 D:经 data bus 发布 CustomData 给策略/风控/监控。
        if let Some(risk) = build_account_risk(&account, ts_now)
            && risk.is_kill_level()
        {
            log::error!(
                "PM 账户风险等级 {}(uniMMR={}),KILL 级告警",
                risk.account_status,
                risk.uni_mmr
            );
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

// -------------------------------------------------------------------------------------------------
// 下单参数映射(纯函数;未知枚举一律 bail,绝不静默兜底)
// -------------------------------------------------------------------------------------------------

fn order_side_str(side: OrderSide) -> anyhow::Result<&'static str> {
    match side {
        OrderSide::Buy => Ok("BUY"),
        OrderSide::Sell => Ok("SELL"),
        OrderSide::NoOrderSide => anyhow::bail!("非法订单方向 NoOrderSide"),
    }
}

fn tif_str(tif: TimeInForce) -> anyhow::Result<&'static str> {
    match tif {
        TimeInForce::Gtc => Ok("GTC"),
        TimeInForce::Ioc => Ok("IOC"),
        TimeInForce::Fok => Ok("FOK"),
        other => {
            anyhow::bail!("PM 适配器不支持 TimeInForce::{other:?}(GTD 用 UM goodTillDate,未接)")
        }
    }
}

/// 腿下单规格(从 `OrderAny` 提取的原语,参数映射层的输入)。
struct LegOrderSpec {
    symbol: String,
    side: OrderSide,
    order_type: OrderType,
    tif: TimeInForce,
    post_only: bool,
    reduce_only: bool,
    quantity: String,
    price: Option<String>,
    client_order_id: String,
}

/// 分流后的腿参数。
enum LegParams {
    Um(PmUmNewOrderParams),
    Margin(PmMarginNewOrderParams),
}

/// 构建 UM 腿下单参数(post-only = GTX;one-way 语义,无 positionSide)。
fn build_um_params(spec: LegOrderSpec) -> anyhow::Result<PmUmNewOrderParams> {
    validate_client_order_id(&spec.client_order_id).map_err(|e| anyhow::anyhow!(e))?;
    let side = order_side_str(spec.side)?;

    let (type_str, tif_opt, price_opt) = match spec.order_type {
        OrderType::Limit => {
            let tif_val = if spec.post_only {
                "GTX"
            } else {
                tif_str(spec.tif)?
            };
            anyhow::ensure!(spec.price.is_some(), "LIMIT 单必须携带价格");
            ("LIMIT".to_string(), Some(tif_val.to_string()), spec.price)
        }
        OrderType::Market => {
            anyhow::ensure!(!spec.post_only, "MARKET 单不能 post-only");
            ("MARKET".to_string(), None, None)
        }
        other => anyhow::bail!("UM 腿不支持订单类型 {other:?}(papi um/order 仅 LIMIT/MARKET)"),
    };

    Ok(PmUmNewOrderParams {
        symbol: spec.symbol,
        side: side.to_string(),
        order_type: type_str,
        time_in_force: tif_opt,
        quantity: Some(spec.quantity),
        price: price_opt,
        reduce_only: Some(spec.reduce_only),
        new_client_order_id: Some(spec.client_order_id),
        new_order_resp_type: Some("RESULT".to_string()),
        price_match: None,
        self_trade_prevention_mode: None,
        good_till_date: None,
    })
}

/// 构建 margin 腿下单参数(post-only = LIMIT_MAKER 无 TIF;绝不借贷由参数
/// 类型保证;margin 无 reduceOnly 概念,携带即 bail)。
fn build_margin_params(spec: LegOrderSpec) -> anyhow::Result<PmMarginNewOrderParams> {
    validate_client_order_id(&spec.client_order_id).map_err(|e| anyhow::anyhow!(e))?;
    anyhow::ensure!(
        !spec.reduce_only,
        "margin 腿无 reduceOnly 概念(平仓即卖出库存),策略层不应携带该标志"
    );
    let side = order_side_str(spec.side)?;

    let mut params = PmMarginNewOrderParams::new(
        spec.symbol,
        side.to_string(),
        String::new(), // 下方按类型填充
    );

    match spec.order_type {
        OrderType::Limit if spec.post_only => {
            // 现货惯例:LIMIT_MAKER 不接受 timeInForce。
            params.order_type = "LIMIT_MAKER".to_string();
            anyhow::ensure!(spec.price.is_some(), "LIMIT_MAKER 单必须携带价格");
            params.price = spec.price;
            params.quantity = Some(spec.quantity);
        }
        OrderType::Limit => {
            params.order_type = "LIMIT".to_string();
            params.time_in_force = Some(tif_str(spec.tif)?.to_string());
            anyhow::ensure!(spec.price.is_some(), "LIMIT 单必须携带价格");
            params.price = spec.price;
            params.quantity = Some(spec.quantity);
        }
        OrderType::Market => {
            anyhow::ensure!(!spec.post_only, "MARKET 单不能 post-only");
            params.order_type = "MARKET".to_string();
            params.quantity = Some(spec.quantity);
        }
        other => anyhow::bail!("margin 腿不支持订单类型 {other:?}"),
    }

    params.new_client_order_id = Some(spec.client_order_id);
    // FULL 是否被 papi 接受存在 SDK/CCXT 矛盾(待 L3 实测),先用 RESULT 保守。
    params.new_order_resp_type = Some("RESULT".to_string());
    Ok(params)
}

/// 该错误是否为 post-only 穿价拒单(UM `-5022` / margin `-2010`+特定 msg)。
fn is_post_only_rejection(err: &BinancePmHttpError) -> bool {
    match err {
        BinancePmHttpError::BinanceError { code, msg, .. } => {
            *code == CODE_GTX_REJECT
                || (*code == CODE_NEW_ORDER_REJECTED && msg == MSG_WOULD_IMMEDIATELY_MATCH)
        }
        _ => false,
    }
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

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("拒绝提交已终态订单 {}", order.client_order_id());
            return Ok(());
        }

        let instrument_id = order.instrument_id();
        let symbol = format_binance_symbol(&instrument_id);
        let side = order.order_side();
        let order_type = order.order_type();
        let tif = order.time_in_force();
        let post_only = order.is_post_only();
        let reduce_only = order.is_reduce_only();
        let quantity = order.quantity().to_string();
        let price = order.price().map(|p| p.to_string());
        let client_order_id = order.client_order_id();
        let strategy_id = order.strategy_id();
        let um_leg = is_um_leg(&instrument_id);

        // 参数构建失败 = 未发出,直接报错(引擎转 Denied),不发 Submitted。
        let spec = LegOrderSpec {
            symbol,
            side,
            order_type,
            tif,
            post_only,
            reduce_only,
            quantity,
            price,
            client_order_id: client_order_id.to_string(),
        };
        let params = if um_leg {
            LegParams::Um(build_um_params(spec)?)
        } else {
            LegParams::Margin(build_margin_params(spec)?)
        };

        self.emitter.emit_order_submitted(&order);

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let clock = self.clock;

        self.spawn_task("pm_submit_order", async move {
            let result = match &params {
                LegParams::Um(p) => http_client.submit_um_order(p).await.map(|_| ()),
                LegParams::Margin(p) => http_client.submit_margin_order(p).await.map(|_| ()),
            };

            if let Err(e) = result {
                match e.outcome_for_write() {
                    // 可能已受理:严禁盲重发、严禁造假终态——保持 in-flight,
                    // 引擎 in-flight 检查按 client_order_id 查单裁决。
                    PmOutcome::Unknown => {
                        log::warn!(
                            "下单结果未知,等待对账裁决 client_order_id={client_order_id}: {e}"
                        );
                    }
                    // 确定未达/明确拒绝:订单确定不存在,发 Rejected(策略可
                    // 改价重发;穿价拒单带 due_post_only 供 maker 退让逻辑)。
                    PmOutcome::Retriable | PmOutcome::Rejected => {
                        let ts_now = clock.get_time_ns();
                        let rejected = OrderRejected::new(
                            trader_id,
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            account_id,
                            format!("submit-order-error: {e}").into(),
                            UUID4::new(),
                            ts_now,
                            ts_now,
                            false,
                            is_post_only_rejection(&e),
                        );
                        emitter.send_order_event(OrderEventAny::Rejected(rejected));
                    }
                }
                anyhow::bail!("submit failed: {e}");
            }
            // 成交/受理事件由用户流(切片 D)与对账报告(切片 C)权威给出。
            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let symbol = format_binance_symbol(&instrument_id);
        let client_order_id = cmd.client_order_id;
        let um_leg = is_um_leg(&instrument_id);
        let http_client = self.http_client.clone();

        let params = PmOrderRefParams::by_client_order_id(symbol, client_order_id.to_string());

        self.spawn_task("pm_cancel_order", async move {
            let result = if um_leg {
                http_client.cancel_um_order(&params).await.map(|_| ())
            } else {
                http_client.cancel_margin_order(&params).await.map(|_| ())
            };

            if let Err(e) = result {
                // -2011/-2013(订单未知/不存在)是对账语义:该单已终态或从未
                // 存在——引擎 in-flight 检查会收敛 PENDING_CANCEL,此处只记录。
                log::warn!("撤单未确认 client_order_id={client_order_id},转对账: {e}");
                anyhow::bail!("cancel failed: {e}");
            }
            // Canceled 事件由用户流/对账权威给出。
            Ok(())
        });

        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let symbol = format_binance_symbol(&instrument_id);
        let um_leg = is_um_leg(&instrument_id);
        let http_client = self.http_client.clone();

        self.spawn_task("pm_cancel_all_orders", async move {
            let result = if um_leg {
                http_client.cancel_all_um_orders(&symbol).await
            } else {
                http_client.cancel_all_margin_orders(&symbol).await
            };
            if let Err(e) = result {
                log::warn!("全撤未确认 symbol={symbol},转对账: {e}");
                anyhow::bail!("cancel-all failed: {e}");
            }
            Ok(())
        });

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

    fn spec(
        side: OrderSide,
        order_type: OrderType,
        tif: TimeInForce,
        post_only: bool,
        reduce_only: bool,
        price: Option<&str>,
    ) -> LegOrderSpec {
        LegOrderSpec {
            symbol: "SOLUSDC".to_string(),
            side,
            order_type,
            tif,
            post_only,
            reduce_only,
            quantity: "1.34".to_string(),
            price: price.map(ToString::to_string),
            client_order_id: "ft01J5KXAMPLE0000000000000AB".to_string(),
        }
    }

    #[test]
    fn um_params_post_only_maps_to_gtx() {
        let p = build_um_params(spec(
            OrderSide::Sell,
            OrderType::Limit,
            TimeInForce::Gtc,
            true,
            false,
            Some("180.50"),
        ))
        .unwrap();
        assert_eq!(p.time_in_force.as_deref(), Some("GTX"));
        assert_eq!(p.order_type, "LIMIT");
        assert_eq!(p.reduce_only, Some(false));
    }

    #[test]
    fn um_params_market_rejects_post_only_and_price() {
        assert!(
            build_um_params(spec(
                OrderSide::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                true, // post_only 与 MARKET 冲突
                true,
                None,
            ))
            .is_err()
        );
        let p = build_um_params(spec(
            OrderSide::Buy,
            OrderType::Market,
            TimeInForce::Ioc,
            false,
            true, // reduce_only 平仓
            None,
        ))
        .unwrap();
        assert_eq!(p.order_type, "MARKET");
        assert!(p.time_in_force.is_none());
        assert!(p.price.is_none());
        assert_eq!(p.reduce_only, Some(true));
    }

    #[test]
    fn um_params_reject_unsupported_types() {
        // 未知类型 bail,绝不静默兜底(vnpy 的坑)。
        assert!(
            build_um_params(spec(
                OrderSide::Buy,
                OrderType::StopMarket,
                TimeInForce::Gtc,
                false,
                false,
                None,
            ))
            .is_err()
        );
    }

    #[test]
    fn margin_params_post_only_maps_to_limit_maker_without_tif() {
        let p = build_margin_params(spec(
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            true,
            false,
            Some("179.80"),
        ))
        .unwrap();
        assert_eq!(p.order_type, "LIMIT_MAKER");
        assert!(p.time_in_force.is_none());
        let qs = serde_urlencoded::to_string(&p).unwrap();
        assert!(qs.contains("sideEffectType=NO_SIDE_EFFECT"));
    }

    #[test]
    fn margin_params_reject_reduce_only() {
        // margin 腿无 reduceOnly 概念,策略层携带即错误。
        assert!(
            build_margin_params(spec(
                OrderSide::Sell,
                OrderType::Limit,
                TimeInForce::Ioc,
                false,
                true, // reduce_only
                Some("1"),
            ))
            .is_err()
        );
    }

    #[test]
    fn post_only_rejection_detection_covers_both_legs() {
        let um = BinancePmHttpError::BinanceError {
            code: CODE_GTX_REJECT,
            msg: "x".to_string(),
            status: 400,
        };
        assert!(is_post_only_rejection(&um));
        let margin = BinancePmHttpError::BinanceError {
            code: CODE_NEW_ORDER_REJECTED,
            msg: MSG_WOULD_IMMEDIATELY_MATCH.to_string(),
            status: 400,
        };
        assert!(is_post_only_rejection(&margin));
        let other = BinancePmHttpError::BinanceError {
            code: CODE_NEW_ORDER_REJECTED,
            msg: "Account has insufficient balance for requested action.".to_string(),
            status: 400,
        };
        assert!(!is_post_only_rejection(&other));
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
