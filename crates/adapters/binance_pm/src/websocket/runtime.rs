//! PM 用户流运行时:单连接 stream 模式 + [`ListenKeyLifecycle`] 状态机驱动。
//!
//! 结构:一个长驻 tokio 任务持有 `(MessageReader, WebSocketClient)`,
//! `tokio::select!` 在「消息到达 / 秒级 tick / 取消」间轮转;所有生命周期
//! 决策(keepalive/重建/轮换/死流)出自纯状态机,本文件只执行副作用。
//!
//! 运维铁律落点(调研 §7.7/§7.8):
//! - stream 模式无自动重连——重连显式由状态机 `Reconnect` 动作驱动,且
//!   **必带全量 resync**(断线窗口状态不可假设);
//! - `/pm/ws/<任意串>` 都 101:连接成功不等于流健康,活性靠服务端 ping 喂狗;
//! - 服务端 ping 必须回 pong(stream 模式不自动回);
//! - 未知事件/转换失败一律 error 日志浮出,绝不静默丢弃;
//! - `liabilityChange` = 「绝不借贷」不变式被破坏 → Broken 级告警;
//!   `RISK_LEVEL_CHANGE` REDUCE_ONLY/FORCE_LIQUIDATION → KILL 级告警。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures_util::StreamExt;

use nautilus_live::emitter::ExecutionEventEmitter;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use nautilus_network::Message;
use nautilus_network::ratelimiter::quota::Quota;
use nautilus_network::websocket::{MessageReader, WebSocketClient, WebSocketConfig};
use tokio_util::sync::CancellationToken;

use crate::common::error::CODE_LISTEN_KEY_NOT_EXIST;
use crate::common::error::{BinancePmHttpError, PmOutcome};
use crate::config::BinancePmExecClientConfig;
use crate::http::client::BinancePmHttpClient;
use crate::websocket::dispatch::{execution_report_to_reports, order_update_to_reports};
use crate::websocket::listen_key::{ListenKeyLifecycle, LkAction};
use crate::websocket::messages::{PmUserStreamEvent, parse_user_stream_event};

/// instrument 精度快照:`(symbol, is_um)` → `(InstrumentId, price_p, size_p)`。
///
/// 流任务运行在多线程 runtime,不能触碰 `?Send` 的引擎 cache——由执行客户端
/// 在 connect/resync 时从 cache 重建本快照(watchlist 规模,极小)。
pub type InstrumentSnapshot = Arc<RwLock<HashMap<(String, bool), (InstrumentId, u8, u8)>>>;

/// 用户流任务上下文(全部 Send)。
pub struct StreamCtx {
    /// REST client(listenKey 生命周期 + resync 快照)。
    pub http: Arc<BinancePmHttpClient>,
    /// 事件发射器(报告路径)。
    pub emitter: ExecutionEventEmitter,
    /// 账户 ID。
    pub account_id: AccountId,
    /// instrument 精度快照。
    pub instruments: InstrumentSnapshot,
    /// 客户端配置(WS base/生命周期参数)。
    pub config: BinancePmExecClientConfig,
    /// 取消令牌(disconnect/stop 触发)。
    pub cancel: CancellationToken,
    /// 时钟(ns)。
    pub clock: &'static nautilus_core::time::AtomicTime,
}

/// 连接 PM 用户流(stream 模式,单连接)。
///
/// # Errors
///
/// 握手失败时报错。注意:握手成功**不代表** listenKey 有效(fstream 对任意
/// 路径都 101),健康与否由服务端 ping 节奏与 keepalive 结果判定。
pub async fn connect_pm_stream(
    ws_base: &str,
    listen_key: &str,
    proxy_url: Option<String>,
) -> anyhow::Result<(MessageReader, WebSocketClient)> {
    let url = format!("{}/ws/{}", ws_base.trim_end_matches('/'), listen_key);
    let config = WebSocketConfig {
        url,
        headers: Vec::new(),
        heartbeat: None,
        heartbeat_msg: None,
        // stream 模式忽略 reconnect_* 字段(文档明示),重连由状态机驱动。
        reconnect_timeout_ms: None,
        reconnect_delay_initial_ms: None,
        reconnect_delay_max_ms: None,
        reconnect_backoff_factor: None,
        reconnect_jitter_ms: None,
        reconnect_max_attempts: None,
        idle_timeout_ms: None,
        backend: Default::default(),
        proxy_url,
    };
    let (reader, client) = WebSocketClient::connect_stream(
        config,
        Vec::new(),
        Some(Quota::per_second(std::num::NonZeroU32::new(2).expect("non-zero")).expect("quota")),
    )
    .await
    .map_err(|e| anyhow::anyhow!("PM 用户流握手失败: {e}"))?;
    Ok((reader, client))
}

/// keepalive 失败是否为「key 不存在」(-1125 或等价明确拒绝)。
fn keepalive_key_gone(err: &BinancePmHttpError) -> bool {
    match err {
        BinancePmHttpError::BinanceError { code, .. } => *code == CODE_LISTEN_KEY_NOT_EXIST,
        e => e.outcome_for_write() == PmOutcome::Rejected,
    }
}

struct Connection {
    reader: MessageReader,
    client: WebSocketClient,
}

/// 用户流主循环(长驻任务;由执行客户端 `connect()` spawn)。
pub async fn run_user_stream(ctx: StreamCtx) {
    let mut lifecycle = ListenKeyLifecycle::with_params(
        ctx.config.keepalive_interval_ms,
        ctx.config.rotate_after_ms,
        ctx.config.dead_stream_after_ms,
        ctx.config.create_retry_cooldown_ms,
    );
    let mut conn: Option<Connection> = None;
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // 账户快照刷新去抖(ACCOUNT_UPDATE 节流 50ms,REST 全量刷新最多 1 次/2s)。
    let mut account_refresh_due = false;
    let mut last_account_refresh_ms: i64 = 0;

    loop {
        let now_ms = (ctx.clock.get_time_ns().as_u64() / 1_000_000) as i64;

        // 有连接时同时等消息;无连接时只等 tick(状态机会发起重建)。
        let actions: Vec<LkAction> = if let Some(c) = conn.as_mut() {
            tokio::select! {
                () = ctx.cancel.cancelled() => break,
                _ = tick.tick() => lifecycle.poll(now_ms),
                msg = c.reader.next() => match msg {
                    Some(Ok(m)) => {
                        lifecycle.on_server_activity(now_ms);
                        handle_message(&ctx, m, &mut lifecycle, now_ms, &mut account_refresh_due, conn.as_ref())
                            .await
                    }
                    Some(Err(e)) => {
                        log::warn!("PM 用户流读错误,按断线处理: {e}");
                        conn = None;
                        lifecycle.on_disconnected()
                    }
                    None => {
                        log::warn!("PM 用户流关闭(24h 强断或服务端断开)");
                        conn = None;
                        lifecycle.on_disconnected()
                    }
                },
            }
        } else {
            tokio::select! {
                () = ctx.cancel.cancelled() => break,
                _ = tick.tick() => {
                    let mut a = lifecycle.poll(now_ms);
                    // 连接缺失但 key 在(重连失败后的兜底重试)。
                    if a.is_empty() && lifecycle.key().is_some() {
                        a.push(LkAction::Reconnect { full_resync: true });
                    }
                    a
                }
            }
        };

        // 执行状态机动作(队列式:动作可能派生后续动作)。
        let mut queue = actions;
        while let Some(action) = queue.pop() {
            match action {
                LkAction::CreateKey => match ctx.http.create_listen_key().await {
                    Ok(k) => {
                        log::info!("listenKey 已创建/续期");
                        queue.extend(lifecycle.on_key_created(k.listen_key, now_ms));
                    }
                    Err(e) => {
                        log::warn!("listenKey 创建失败,冷却后重试: {e}");
                        lifecycle.on_key_create_failed(now_ms);
                    }
                },
                LkAction::Keepalive => match ctx.http.keepalive_listen_key().await {
                    Ok(()) => lifecycle.on_keepalive_ok(now_ms),
                    Err(e) => {
                        let key_gone = keepalive_key_gone(&e);
                        log::warn!("listenKey 续期失败(key_gone={key_gone}),作废重建: {e}");
                        queue.extend(lifecycle.on_keepalive_failed(key_gone, now_ms));
                    }
                },
                LkAction::Reconnect { full_resync } => {
                    let Some(key) = lifecycle.key().map(ToString::to_string) else {
                        continue; // key 缺失:CreateKey 路径会再触发重连
                    };
                    if let Some(old) = conn.take() {
                        let _ = old.client.send_close_message().await;
                    }
                    match connect_pm_stream(
                        &ctx.config.base_url_ws,
                        &key,
                        ctx.config.proxy_url.clone(),
                    )
                    .await
                    {
                        Ok((reader, client)) => {
                            conn = Some(Connection { reader, client });
                            lifecycle.on_connected(now_ms);
                            log::info!("PM 用户流已连接(full_resync={full_resync})");
                            if full_resync {
                                account_refresh_due = true;
                                // 订单/持仓层面的收敛由引擎持续对账(open/inflight/
                                // position 检查)驱动,此处刷新账户快照即可。
                            }
                        }
                        Err(e) => {
                            log::warn!("PM 用户流重连失败,tick 兜底重试: {e}");
                        }
                    }
                }
                LkAction::AlertDeadStream => {
                    log::error!(
                        "PM 用户流疑似死流(服务端 ping 缺席 {}ms),重建连接",
                        ctx.config.dead_stream_after_ms
                    );
                }
            }
        }

        // 账户快照刷新(全量重建再发,apply 是替换语义)。
        if account_refresh_due && now_ms - last_account_refresh_ms >= 2_000 {
            account_refresh_due = false;
            last_account_refresh_ms = now_ms;
            if let Err(e) = refresh_account(&ctx).await {
                log::warn!("账户快照刷新失败(下轮重试): {e}");
                account_refresh_due = true;
            }
        }
    }

    // 退出:优雅关闭连接与 listenKey(best-effort)。
    if let Some(c) = conn.take() {
        let _ = c.client.send_close_message().await;
    }
    if let Err(e) = ctx.http.close_listen_key().await {
        log::debug!("close_listen_key 失败(忽略): {e}");
    }
    log::info!("PM 用户流任务退出");
}

async fn refresh_account(ctx: &StreamCtx) -> anyhow::Result<()> {
    let balances = ctx.http.balances().await?;
    let account = ctx.http.account().await?;
    let ts_now = ctx.clock.get_time_ns();
    let state = crate::execution::build_account_state(
        &balances,
        &account,
        ctx.account_id,
        nautilus_model::enums::AccountType::Margin,
        ts_now,
    );
    ctx.emitter
        .emit_account_state(state.balances, state.margins, true, ts_now);
    if let Some(risk) = crate::execution::build_account_risk(&account, ts_now)
        && risk.is_kill_level()
    {
        log::error!(
            "KILL:PM 账户风险等级 {}(uniMMR={})",
            risk.account_status,
            risk.uni_mmr
        );
    }
    Ok(())
}

fn lookup(
    instruments: &InstrumentSnapshot,
    symbol: &str,
    um: bool,
) -> Option<(InstrumentId, u8, u8)> {
    instruments
        .read()
        .ok()?
        .get(&(symbol.to_string(), um))
        .copied()
}

/// 处理一条 WS 消息;可能产生生命周期动作(listenKeyExpired)。
async fn handle_message(
    ctx: &StreamCtx,
    msg: Message,
    lifecycle: &mut ListenKeyLifecycle,
    now_ms: i64,
    account_refresh_due: &mut bool,
    conn: Option<&Connection>,
) -> Vec<LkAction> {
    match msg {
        Message::Ping(payload) => {
            // stream 模式不自动回 pong,必须手动——否则 10 分钟被服务端断。
            if let Some(c) = conn
                && let Err(e) = c.client.send_pong(payload.to_vec()).await
            {
                log::warn!("pong 发送失败: {e}");
            }
            Vec::new()
        }
        Message::Text(text) => match std::str::from_utf8(&text) {
            Ok(text) => handle_event(ctx, text, lifecycle, now_ms, account_refresh_due),
            Err(e) => {
                log::error!("用户流报文非 UTF-8(须人工核查): {e}");
                Vec::new()
            }
        },
        Message::Close(_) => {
            log::warn!("收到服务端 Close 帧");
            Vec::new() // reader 随后返回 None,统一走断线路径
        }
        _ => Vec::new(),
    }
}

fn handle_event(
    ctx: &StreamCtx,
    text: &str,
    lifecycle: &mut ListenKeyLifecycle,
    now_ms: i64,
    account_refresh_due: &mut bool,
) -> Vec<LkAction> {
    let ts_init = ctx.clock.get_time_ns();
    match parse_user_stream_event(text) {
        PmUserStreamEvent::OrderTradeUpdate(ev) => {
            if !ev.is_um() {
                log::error!(
                    "收到非 UM 业务单元订单事件(fs={}),本策略不交易 CM,须人工核查",
                    ev.fs
                );
                return Vec::new();
            }
            if ev.o.is_system_order() {
                log::error!(
                    "KILL:收到交易所系统单(清算/ADL)事件 symbol={} clientOrderId={}",
                    ev.o.s,
                    ev.o.c
                );
            }
            let Some((instrument_id, pp, sp)) = lookup(&ctx.instruments, &ev.o.s, true) else {
                log::error!("UM 事件 symbol={} 无已加载 instrument,丢弃并告警", ev.o.s);
                return Vec::new();
            };
            match order_update_to_reports(&ev, ctx.account_id, instrument_id, pp, sp, ts_init) {
                Ok((report, fill)) => {
                    ctx.emitter.send_order_status_report(report);
                    if let Some(fill) = fill {
                        ctx.emitter.send_fill_report(fill);
                    }
                }
                Err(e) => log::error!("UM 订单事件转换失败(格式漂移,须人工核查): {e}"),
            }
            Vec::new()
        }
        PmUserStreamEvent::ExecutionReport(ev) => {
            let Some((instrument_id, pp, sp)) = lookup(&ctx.instruments, &ev.s, false) else {
                log::error!("margin 事件 symbol={} 无已加载 instrument,丢弃并告警", ev.s);
                return Vec::new();
            };
            match execution_report_to_reports(&ev, ctx.account_id, instrument_id, pp, sp, ts_init) {
                Ok((report, fill)) => {
                    ctx.emitter.send_order_status_report(report);
                    if let Some(fill) = fill {
                        ctx.emitter.send_fill_report(fill);
                    }
                }
                Err(e) => log::error!("margin 订单事件转换失败(格式漂移,须人工核查): {e}"),
            }
            Vec::new()
        }
        PmUserStreamEvent::AccountUpdate(ev) => {
            if ev.a.m == "FUNDING_FEE" {
                log::info!(
                    "资金费到账 symbol={:?} balances={}",
                    ev.a.symbol,
                    ev.a.balances.len()
                );
            }
            *account_refresh_due = true;
            Vec::new()
        }
        PmUserStreamEvent::OutboundAccountPosition(_) | PmUserStreamEvent::BalanceUpdate(_) => {
            *account_refresh_due = true;
            Vec::new()
        }
        PmUserStreamEvent::LiabilityChange(ev) => {
            // 我方恒 NO_SIDE_EFFECT:此事件出现 = 「绝不借贷」不变式被破坏。
            log::error!(
                "BROKEN:收到 liabilityChange(asset={} 本金={} 利息={} 总负债={})——绝不借贷不变式被破坏,立即人工核查",
                ev.a,
                ev.principal,
                ev.interest,
                ev.total_liability
            );
            *account_refresh_due = true;
            Vec::new()
        }
        PmUserStreamEvent::RiskLevelChange(ev) => {
            if ev.s == "REDUCE_ONLY" || ev.s == "FORCE_LIQUIDATION" {
                log::error!("KILL:RISK_LEVEL_CHANGE 等级={} uniMMR={}", ev.s, ev.u);
            } else {
                log::warn!("RISK_LEVEL_CHANGE 等级={} uniMMR={}", ev.s, ev.u);
            }
            *account_refresh_due = true;
            Vec::new()
        }
        PmUserStreamEvent::OpenOrderLoss(ev) => {
            log::info!("openOrderLoss(全仓挂单占用):{} 项", ev.losses.len());
            *account_refresh_due = true;
            Vec::new()
        }
        PmUserStreamEvent::AccountConfigUpdate(ev) => {
            log::info!("杠杆变更 symbol={} leverage={}", ev.ac.s, ev.ac.l);
            Vec::new()
        }
        PmUserStreamEvent::ListenKeyExpired(_) => {
            log::warn!("listenKeyExpired:作废并重建(与断线无关)");
            lifecycle.on_listen_key_expired(now_ms)
        }
        PmUserStreamEvent::ConditionalOrderTradeUpdate(_) | PmUserStreamEvent::AlgoUpdate(_) => {
            log::debug!("忽略条件单/算法单事件(本策略不使用)");
            Vec::new()
        }
        PmUserStreamEvent::Unknown { event_type, raw } => {
            log::error!(
                "未知用户流事件(须人工核查,词表可能漂移)type={event_type:?} raw={}",
                &raw[..raw.len().min(512)]
            );
            Vec::new()
        }
    }
}
