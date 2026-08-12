//! listenKey 生命周期纯状态机(决策与副作用分离,离线全覆盖测试)。
//!
//! 副作用(REST 调用/重连)由运行时层执行;本模块只做决策。设计对照三家参考
//! 实现的事故点(调研报告 §7.7):
//! - **重建后 keepalive 计时必须重置**(NexusTrader:重建后新 key 永不续期,
//!   每 60 分钟死流循环);
//! - **PUT 失败(尤其 -1125)必须立刻作废本地 key 并重建**,不能只记日志
//!   (vnpy:续期超时被吞,key 陈旧);
//! - **`/pm/ws/<任意串>` 都 101 握手成功**——「已连接」不是「流健康」,活性
//!   判据独立:服务端 3 分钟 ping 节奏缺席即疑死流;
//! - **24h 强制断开**:~23h 主动优雅轮换,不等被踢;
//! - `listenKeyExpired` 事件与断线无关,收到后必须重建 key + 重连 + 全量 resync。

/// keepalive 周期缺省 20 分钟(key 有效 60 分钟,与 CCXT/NexusTrader 一致)。
pub const DEFAULT_KEEPALIVE_INTERVAL_MS: i64 = 20 * 60 * 1000;

/// 主动轮换连接缺省 23 小时(服务端 24h 强制断开)。
pub const DEFAULT_ROTATE_AFTER_MS: i64 = 23 * 60 * 60 * 1000;

/// 死流判据缺省 4 分钟:服务端每 3 分钟发 ping,一个周期 + 1 分钟余量内
/// 无任何服务端活动(ping/数据帧)即疑死流。
pub const DEFAULT_DEAD_STREAM_AFTER_MS: i64 = 4 * 60 * 1000;

/// 重建 key 失败后的冷却,防止对 REST 端点打转。
pub const DEFAULT_CREATE_RETRY_COOLDOWN_MS: i64 = 5 * 1000;

/// 状态机产出的动作(由运行时执行副作用)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LkAction {
    /// 调 `POST /papi/v1/listenKey` 创建/取回 key。
    CreateKey,
    /// 调 `PUT /papi/v1/listenKey` 续期。
    Keepalive,
    /// 用当前 key 重连用户流并重新订阅;`full_resync` 为真时运行时必须随后
    /// 触发全量对账(两腿 openOrders + positionRisk + balance + 时间窗补成交)。
    Reconnect {
        /// 是否需要全量 resync(断线窗口可能丢事件时为真)。
        full_resync: bool,
    },
    /// 疑似死流告警(运行时应记录并随 Reconnect 处理)。
    AlertDeadStream,
}

/// listenKey 生命周期状态机。
///
/// 时间一律由调用方注入(毫秒时间戳),不读系统时钟——可注入测试。
#[derive(Debug)]
pub struct ListenKeyLifecycle {
    key: Option<String>,
    /// key 创建/最近一次成功续期的时刻(keepalive 计时基准)。
    keepalive_basis_ms: i64,
    /// 当前连接建立时刻(None = 未连接)。
    connected_at_ms: Option<i64>,
    /// 最近一次服务端活动(ping/任何帧)。
    last_server_activity_ms: i64,
    /// 最近一次 CreateKey 请求发出时刻(冷却用)。
    last_create_request_ms: Option<i64>,
    /// 已发出 Keepalive 且未收到结果(防重复发)。
    keepalive_inflight: bool,
    keepalive_interval_ms: i64,
    rotate_after_ms: i64,
    dead_stream_after_ms: i64,
    create_retry_cooldown_ms: i64,
}

impl ListenKeyLifecycle {
    /// 以缺省参数构造。
    #[must_use]
    pub fn new() -> Self {
        Self::with_params(
            DEFAULT_KEEPALIVE_INTERVAL_MS,
            DEFAULT_ROTATE_AFTER_MS,
            DEFAULT_DEAD_STREAM_AFTER_MS,
            DEFAULT_CREATE_RETRY_COOLDOWN_MS,
        )
    }

    /// 自定义参数构造(测试/TOML 配置)。
    #[must_use]
    pub fn with_params(
        keepalive_interval_ms: i64,
        rotate_after_ms: i64,
        dead_stream_after_ms: i64,
        create_retry_cooldown_ms: i64,
    ) -> Self {
        Self {
            key: None,
            keepalive_basis_ms: 0,
            connected_at_ms: None,
            last_server_activity_ms: 0,
            last_create_request_ms: None,
            keepalive_inflight: false,
            keepalive_interval_ms,
            rotate_after_ms,
            dead_stream_after_ms,
            create_retry_cooldown_ms,
        }
    }

    /// 当前 key(运行时拼 WS URL 用)。
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// 周期驱动:返回当前应执行的动作(幂等,运行时按序执行)。
    #[must_use]
    pub fn poll(&mut self, now_ms: i64) -> Vec<LkAction> {
        let mut actions = Vec::new();

        // 无 key:冷却期外发起创建。
        if self.key.is_none() {
            let cooled = self
                .last_create_request_ms
                .is_none_or(|t| now_ms - t >= self.create_retry_cooldown_ms);
            if cooled {
                self.last_create_request_ms = Some(now_ms);
                actions.push(LkAction::CreateKey);
            }
            return actions;
        }

        // keepalive 到期(计时基准 = 创建或最近一次成功续期)。
        if !self.keepalive_inflight
            && now_ms - self.keepalive_basis_ms >= self.keepalive_interval_ms
        {
            self.keepalive_inflight = true;
            actions.push(LkAction::Keepalive);
        }

        if let Some(connected_at) = self.connected_at_ms {
            // 24h 强制断开前主动轮换(优雅重连,无事件丢失窗口,不必 resync;
            // 保守起见仍要求 resync——轮换瞬间的推送可能落在旧连接关闭之后)。
            if now_ms - connected_at >= self.rotate_after_ms {
                self.connected_at_ms = None;
                actions.push(LkAction::Reconnect { full_resync: true });
            } else if now_ms - self.last_server_activity_ms >= self.dead_stream_after_ms {
                // 死流:服务端 ping 节奏缺席。断线窗口未知,必须全量 resync。
                self.connected_at_ms = None;
                actions.push(LkAction::AlertDeadStream);
                actions.push(LkAction::Reconnect { full_resync: true });
            }
        }

        actions
    }

    /// CreateKey 成功回报:**keepalive 计时基准重置到现在**(NexusTrader bug
    /// 的对照点),并要求运行时用新 key 重连。
    #[must_use]
    pub fn on_key_created(&mut self, key: String, now_ms: i64) -> Vec<LkAction> {
        self.key = Some(key);
        self.keepalive_basis_ms = now_ms;
        self.keepalive_inflight = false;
        self.last_create_request_ms = None;
        // 旧连接(若有)绑定的是旧 key 的流,必须重连 + resync。
        self.connected_at_ms = None;
        vec![LkAction::Reconnect { full_resync: true }]
    }

    /// CreateKey 失败回报:进入冷却,poll 到期后重试。
    pub fn on_key_create_failed(&mut self, now_ms: i64) {
        self.last_create_request_ms = Some(now_ms);
    }

    /// Keepalive 成功回报:重置计时基准。
    pub fn on_keepalive_ok(&mut self, now_ms: i64) {
        self.keepalive_inflight = false;
        self.keepalive_basis_ms = now_ms;
    }

    /// Keepalive 失败回报。
    ///
    /// `key_gone` = 错误被判定为「key 不存在」(-1125 或等价 Rejected):
    /// **立刻作废本地 key 并重建**,绝不重试 PUT(vnpy 的坑:吞掉失败导致
    /// 陈旧 key 静默死流)。传输类失败(Unknown)则保守重建——key 状态不明时
    /// 宁可重建也不能带着可能已失效的 key 继续跑。
    #[must_use]
    pub fn on_keepalive_failed(&mut self, key_gone: bool, now_ms: i64) -> Vec<LkAction> {
        self.keepalive_inflight = false;
        let _ = key_gone; // 目前两类失败同策略:作废重建;保留参数以便将来分流。
        self.key = None;
        self.last_create_request_ms = Some(now_ms);
        vec![LkAction::CreateKey]
    }

    /// 收到 `listenKeyExpired` 事件:与断线无关,key 立即作废,重建 + 重连 +
    /// 全量 resync(重连动作由 `on_key_created` 发出)。
    #[must_use]
    pub fn on_listen_key_expired(&mut self, now_ms: i64) -> Vec<LkAction> {
        self.key = None;
        self.connected_at_ms = None;
        self.last_create_request_ms = Some(now_ms);
        vec![LkAction::CreateKey]
    }

    /// WS 连接建立回报(运行时在 Reconnect 动作完成后调用)。
    pub fn on_connected(&mut self, now_ms: i64) {
        self.connected_at_ms = Some(now_ms);
        self.last_server_activity_ms = now_ms;
    }

    /// 任何服务端活动(ping 帧/数据帧)——死流判据的喂狗。
    pub fn on_server_activity(&mut self, now_ms: i64) {
        self.last_server_activity_ms = now_ms;
    }

    /// WS 意外断开回报:断线窗口可能丢事件,重连必须全量 resync
    /// (vnpy 的坑:重连什么都不查,断线期状态永久丢失)。
    #[must_use]
    pub fn on_disconnected(&mut self) -> Vec<LkAction> {
        self.connected_at_ms = None;
        vec![LkAction::Reconnect { full_resync: true }]
    }
}

impl Default for ListenKeyLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KA: i64 = 1_200_000; // 20min
    const ROT: i64 = 82_800_000; // 23h
    const DEAD: i64 = 240_000; // 4min
    const COOL: i64 = 5_000;

    fn lk() -> ListenKeyLifecycle {
        ListenKeyLifecycle::with_params(KA, ROT, DEAD, COOL)
    }

    /// 标准启动序列:创建 → 重连(带 resync)→ 已连接。
    fn boot(lk: &mut ListenKeyLifecycle, now: i64) {
        assert_eq!(lk.poll(now), vec![LkAction::CreateKey]);
        let actions = lk.on_key_created("k1".to_string(), now);
        assert_eq!(actions, vec![LkAction::Reconnect { full_resync: true }]);
        lk.on_connected(now);
    }

    #[test]
    fn keepalive_fires_on_interval_and_resets_on_ok() {
        let mut lk = lk();
        boot(&mut lk, 0);
        // 未到期不发。
        lk.on_server_activity(KA - 1);
        assert!(lk.poll(KA - 1).is_empty());
        // 到期发一次,inflight 期间不重复发。
        lk.on_server_activity(KA);
        assert_eq!(lk.poll(KA), vec![LkAction::Keepalive]);
        lk.on_server_activity(KA + 1);
        assert!(lk.poll(KA + 1).is_empty());
        // 成功后计时基准重置。
        lk.on_keepalive_ok(KA);
        lk.on_server_activity(2 * KA - 1);
        assert!(lk.poll(2 * KA - 1).is_empty());
        lk.on_server_activity(2 * KA);
        assert_eq!(lk.poll(2 * KA), vec![LkAction::Keepalive]);
    }

    #[test]
    fn keepalive_timer_restarts_after_rebuild() {
        // NexusTrader bug 对照:重建后新 key 的 keepalive 必须以重建时刻起算。
        let mut lk = lk();
        boot(&mut lk, 0);
        lk.on_server_activity(KA);
        assert_eq!(lk.poll(KA), vec![LkAction::Keepalive]);
        // -1125:作废 + 重建。
        assert_eq!(lk.on_keepalive_failed(true, KA), vec![LkAction::CreateKey]);
        assert!(lk.key().is_none());
        let t_rebuild = KA + 1_000;
        let _ = lk.on_key_created("k2".to_string(), t_rebuild);
        lk.on_connected(t_rebuild);
        // 新基准 = 重建时刻:旧基准算出来的"到期"不得触发。
        lk.on_server_activity(t_rebuild + KA - 1);
        assert!(lk.poll(t_rebuild + KA - 1).is_empty());
        lk.on_server_activity(t_rebuild + KA);
        assert_eq!(lk.poll(t_rebuild + KA), vec![LkAction::Keepalive]);
    }

    #[test]
    fn listen_key_expired_forces_rebuild_and_resync() {
        let mut lk = lk();
        boot(&mut lk, 0);
        assert_eq!(lk.on_listen_key_expired(10), vec![LkAction::CreateKey]);
        assert!(lk.key().is_none());
        // 重建完成 → 重连必须带 full_resync。
        assert_eq!(
            lk.on_key_created("k2".to_string(), 20),
            vec![LkAction::Reconnect { full_resync: true }]
        );
    }

    #[test]
    fn rotates_connection_before_24h_kick() {
        let mut lk = lk();
        boot(&mut lk, 0);
        lk.on_keepalive_ok(ROT - 1); // 排除 keepalive 干扰
        lk.on_server_activity(ROT);
        let actions = lk.poll(ROT);
        assert!(actions.contains(&LkAction::Reconnect { full_resync: true }));
    }

    #[test]
    fn dead_stream_detected_when_server_ping_absent() {
        // `/pm/ws/<任意串>` 都 101:连接成功不等于健康,靠服务端活动节奏判死流。
        let mut lk = lk();
        boot(&mut lk, 0);
        lk.on_keepalive_ok(DEAD - 1); // 排除 keepalive 干扰
        // 无任何服务端活动越过阈值 → 告警 + 重连(带 resync)。
        let actions = lk.poll(DEAD);
        assert_eq!(
            actions,
            vec![
                LkAction::AlertDeadStream,
                LkAction::Reconnect { full_resync: true }
            ]
        );
        // 有活动喂狗则不触发。
        let mut lk2 = lk_with_boot();
        lk2.on_keepalive_ok(DEAD - 1);
        lk2.on_server_activity(DEAD - 1);
        assert!(lk2.poll(DEAD).is_empty());
    }

    fn lk_with_boot() -> ListenKeyLifecycle {
        let mut l = lk();
        boot(&mut l, 0);
        l
    }

    #[test]
    fn unexpected_disconnect_requires_full_resync() {
        let mut lk = lk_with_boot();
        assert_eq!(
            lk.on_disconnected(),
            vec![LkAction::Reconnect { full_resync: true }]
        );
    }

    #[test]
    fn create_failure_respects_cooldown() {
        let mut lk = lk();
        assert_eq!(lk.poll(0), vec![LkAction::CreateKey]);
        lk.on_key_create_failed(0);
        // 冷却期内不重试。
        assert!(lk.poll(COOL - 1).is_empty());
        // 冷却期满重试。
        assert_eq!(lk.poll(COOL), vec![LkAction::CreateKey]);
    }

    #[test]
    fn transport_failure_on_keepalive_also_rebuilds() {
        // key 状态不明时宁可重建,不带着可能失效的 key 继续跑。
        let mut lk = lk_with_boot();
        lk.on_server_activity(KA);
        assert_eq!(lk.poll(KA), vec![LkAction::Keepalive]);
        assert_eq!(lk.on_keepalive_failed(false, KA), vec![LkAction::CreateKey]);
        assert!(lk.key().is_none());
    }
}
