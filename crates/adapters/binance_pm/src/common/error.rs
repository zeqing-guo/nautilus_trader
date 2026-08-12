//! PM 适配器错误类型与「结果三分类」。
//!
//! 三分类语义与 fuzzy-trading `ft_exec::ExecError` 同源,也与 Nautilus 执行文档的
//! command outcome policy 同构:
//! - `PmOutcome::Retriable`:**确定未达/未受理**,同参重试安全;
//! - `PmOutcome::Unknown`:**可能已受理**(超时/5xx/传输后解析失败)——严禁盲重发,
//!   必须按 client_order_id 查单裁决;
//! - `PmOutcome::Rejected`:交易所**明确拒绝**,订单确定不存在,按拒因分流处理。

use nautilus_network::http::HttpClientError;

/// 写操作(下单/撤单/改单)结果三分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PmOutcome {
    /// 确定未达交易所(签名前校验失败、时间戳被拒等),同参重试安全。
    Retriable,
    /// 结果未知(超时/5xx/断连/解析失败),可能已受理——必须查单裁决,严禁盲重发。
    Unknown,
    /// 交易所明确拒绝,订单确定不存在。
    Rejected,
}

/// PM HTTP 层错误。
#[derive(Debug, thiserror::Error)]
pub enum BinancePmHttpError {
    /// 调用签名端点但未配置凭证。
    #[error("missing credentials for signed papi endpoint")]
    MissingCredentials,
    /// 请求构造/参数序列化失败(未发出)。
    #[error("validation error: {0}")]
    ValidationError(String),
    /// 响应 JSON 解析失败(请求已发出且拿到 2xx——结果未知)。
    #[error("json error: {0}")]
    JsonError(String),
    /// 传输层错误(超时/断连等,来自 nautilus-network)。
    #[error(transparent)]
    HttpClientError(#[from] HttpClientError),
    /// Binance 业务错误码(HTTP 非 2xx 且 body 携带 code/msg)。
    #[error("binance error {code}: {msg}")]
    BinanceError {
        /// Binance 错误码(如 -5022、-2010、-1021)。
        code: i64,
        /// 错误消息原文。
        msg: String,
        /// HTTP 状态码。
        status: u16,
    },
    /// 非 2xx 且 body 无法解析为标准错误结构。
    #[error("unexpected status {status}: {body}")]
    UnexpectedStatus {
        /// HTTP 状态码。
        status: u16,
        /// 响应 body 原文(截断)。
        body: String,
    },
}

/// UM post-only(GTX)穿价拒单专用码。
///
/// 关键语义(官方公告 2023-03-21 生效):被拒的单**不留任何痕迹**——无历史、无 WS
/// 事件、查单必返 -2013。因此 **-5022 这个同步错误码是唯一判据**,绝不能把 GTX 单
/// 降级成 Unknown 走查单裁决(「查不到」与「超时未达」表现完全相同,会误判)。
pub const CODE_GTX_REJECT: i64 = -5022;

/// margin 腿 LIMIT_MAKER 穿价拒单码(需配合 msg 判定,现货侧无专用码)。
pub const CODE_NEW_ORDER_REJECTED: i64 = -2010;

/// margin LIMIT_MAKER 穿价拒单的 msg 原文。
pub const MSG_WOULD_IMMEDIATELY_MATCH: &str = "Order would immediately match and take.";

/// 撤单时订单未知(-2011,对账语义同 -2013 查无此单)。
pub const CODE_CANCEL_UNKNOWN_ORDER: i64 = -2011;

/// 查无此单。
pub const CODE_ORDER_DOES_NOT_EXIST: i64 = -2013;

/// 时间戳超出 recvWindow(确定被拒于处理之前)。
pub const CODE_TIMESTAMP_OUT_OF_WINDOW: i64 = -1021;

/// clientOrderId 重复(幂等冲突——说明此前的 Unknown 其实已受理)。
pub const CODE_DUPLICATE_CLIENT_ORDER_ID: i64 = -4116;

/// listenKey 不存在(PUT 续期失败,须重新 POST 重建——不是重试)。
pub const CODE_LISTEN_KEY_NOT_EXIST: i64 = -1125;

/// FOK 未全成拒单(与 -5022 同族:不留痕迹、无 WS 事件)。
pub const CODE_FOK_REJECT: i64 = -5021;

/// 官方 503 "执行状态未知" 文案(general-info §HTTP 503:必须查单裁决,严禁当失败)。
pub const MSG_503_UNKNOWN: &str = "Unknown error, please check your request or try again later.";

/// margin 腿 -2010 重复 clientOrderId 文案(=「原单存在」强信号,转查单收敛)。
pub const MSG_DUPLICATE_ORDER: &str = "Duplicate order sent.";

/// margin 腿 -2011 撤单找不到文案(转查单裁决)。
pub const MSG_UNKNOWN_ORDER: &str = "Unknown order sent.";

impl BinancePmHttpError {
    /// 对**写操作**(下单/撤单/改单)分类结果。
    ///
    /// 映射依据:PM error-code.md + general-info.md §HTTP 503(官方逐条写明执行
    /// 状态)+ spot errors.md 的 -2010/-2011 msg 细分表。完整推导与出处见
    /// fuzzy-trading docs/research-report-mature-system.md §7。
    /// 原则:**拿不准一律 Unknown**——Unknown 走查单裁决最多慢,盲重发会翻倍裸腿。
    #[must_use]
    pub fn outcome_for_write(&self) -> PmOutcome {
        match self {
            // 未发出的请求:同参重试安全。
            Self::MissingCredentials | Self::ValidationError(_) => PmOutcome::Retriable,
            // 2xx 但解析失败:交易所大概率已受理。
            Self::JsonError(_) => PmOutcome::Unknown,
            // 传输层(超时/断连/TLS 中断):请求已发出但无确定应答。
            Self::HttpClientError(_) => PmOutcome::Unknown,
            Self::BinanceError { code, msg, status } => {
                Self::classify_binance_code(*code, msg, *status)
            }
            Self::UnexpectedStatus { status, .. } => {
                if *status >= 500 {
                    PmOutcome::Unknown
                } else {
                    PmOutcome::Rejected
                }
            }
        }
    }

    fn classify_binance_code(code: i64, msg: &str, status: u16) -> PmOutcome {
        match code {
            // ---- Unknown:官方明示「执行状态未知」,必须按 client_order_id 查单裁决 ----
            // -1006 UNEXPECTED_RESP / -1007 TIMEOUT:官方原文 "execution status unknown"。
            -1006 | -1007 => PmOutcome::Unknown,
            // -1000 / -51999:未说明执行状态,保守归 Unknown。
            -1000 | -51999 => PmOutcome::Unknown,
            // 幂等冲突:此前的 Unknown 其实已受理——按「原单存在」处理,走查单收敛。
            CODE_DUPLICATE_CLIENT_ORDER_ID => PmOutcome::Unknown,

            // ---- Retriable:官方明示「100% failure」或确定被拒于撮合之前 ----
            // -1008 系统过载(reduce-only/平仓单豁免该限制);-1015/-5041 队列限速。
            -1008 | -1015 | -5041 => PmOutcome::Retriable,
            // -1001 DISCONNECTED = 官方 503 "Internal error…please try again" 文案。
            -1001 => PmOutcome::Retriable,
            // -1003 限速(429/418 须退避,418 是 IP 封禁,重试前必须等待)。
            -1003 => PmOutcome::Retriable,
            // 校时类:重新校时后重发安全(不校时就重发会死循环)。
            CODE_TIMESTAMP_OUT_OF_WINDOW | -5028 => PmOutcome::Retriable,
            // margin 侧系统忙。
            -51007 | -51998 => PmOutcome::Retriable,

            // ---- Rejected:明确拒绝,订单确定不存在 ----
            // post-only/FOK 穿价拒单:不留痕迹、无 WS 事件,-5022/-5021 是唯一判据。
            CODE_GTX_REJECT | CODE_FOK_REJECT => PmOutcome::Rejected,
            // margin 腿 -2010:含 LIMIT_MAKER 穿价与 Duplicate(后者提示转查单,
            // 但分类上同属「本次请求被拒」);-2011/-2013 撤单/查单找不到。
            CODE_NEW_ORDER_REJECTED | CODE_CANCEL_UNKNOWN_ORDER | CODE_ORDER_DOES_NOT_EXIST => {
                PmOutcome::Rejected
            }
            // listenKey 不存在:重建而非重试(归 Rejected 防止盲目 PUT 循环)。
            CODE_LISTEN_KEY_NOT_EXIST => PmOutcome::Rejected,

            // ---- 兜底 ----
            // 503 携带官方「执行状态未知」文案 → Unknown;其余 5xx 保守 Unknown。
            _ if status >= 500 && msg == MSG_503_UNKNOWN => PmOutcome::Unknown,
            _ if status >= 500 => PmOutcome::Unknown,
            // 其余 4xx 业务码(参数/资金/持仓模式/过滤器/权限)一律明确拒绝。
            _ => PmOutcome::Rejected,
        }
    }

    /// 该错误是否为「原单可能已存在」的强信号,调用方须立即转 Reconciler
    /// 按 client_order_id 查单收敛(UM 的 -4116、margin 的 -2010+Duplicate)。
    #[must_use]
    pub fn implies_order_may_exist(&self) -> bool {
        match self {
            Self::BinanceError { code, msg, .. } => {
                *code == CODE_DUPLICATE_CLIENT_ORDER_ID
                    || (*code == CODE_NEW_ORDER_REJECTED && msg == MSG_DUPLICATE_ORDER)
            }
            _ => false,
        }
    }
}

/// PM HTTP 结果别名。
pub type BinancePmHttpResult<T> = Result<T, BinancePmHttpError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn binance_err(code: i64, status: u16) -> BinancePmHttpError {
        BinancePmHttpError::BinanceError {
            code,
            msg: "?".to_string(),
            status,
        }
    }

    #[test]
    fn gtx_reject_is_rejected_not_unknown() {
        // -5022 必须是明确拒绝:被拒的 GTX 单不留任何痕迹,走查单裁决会误判。
        assert_eq!(
            binance_err(CODE_GTX_REJECT, 400).outcome_for_write(),
            PmOutcome::Rejected
        );
    }

    #[test]
    fn timestamp_reject_is_retriable() {
        assert_eq!(
            binance_err(CODE_TIMESTAMP_OUT_OF_WINDOW, 400).outcome_for_write(),
            PmOutcome::Retriable
        );
    }

    #[test]
    fn duplicate_client_order_id_is_unknown() {
        // -4116 说明此前的 Unknown 已受理,须查单收敛,不能当拒绝丢弃。
        assert_eq!(
            binance_err(CODE_DUPLICATE_CLIENT_ORDER_ID, 400).outcome_for_write(),
            PmOutcome::Unknown
        );
    }

    #[test]
    fn server_error_is_unknown() {
        assert_eq!(
            binance_err(-1000, 502).outcome_for_write(),
            PmOutcome::Unknown
        );
        let err = BinancePmHttpError::UnexpectedStatus {
            status: 504,
            body: "gateway timeout".to_string(),
        };
        assert_eq!(err.outcome_for_write(), PmOutcome::Unknown);
    }

    #[test]
    fn official_unknown_codes_require_reconciliation() {
        // -1006/-1007:官方原文 "execution status unknown"。
        assert_eq!(
            binance_err(-1006, 400).outcome_for_write(),
            PmOutcome::Unknown
        );
        assert_eq!(
            binance_err(-1007, 408).outcome_for_write(),
            PmOutcome::Unknown
        );
    }

    #[test]
    fn overload_and_throttle_are_retriable() {
        // -1008 系统过载官方明示 "100% failure";-1003 限速退避后重发。
        assert_eq!(
            binance_err(-1008, 503).outcome_for_write(),
            PmOutcome::Retriable
        );
        assert_eq!(
            binance_err(-1003, 429).outcome_for_write(),
            PmOutcome::Retriable
        );
    }

    #[test]
    fn fok_reject_and_listen_key_are_rejected() {
        assert_eq!(
            binance_err(CODE_FOK_REJECT, 400).outcome_for_write(),
            PmOutcome::Rejected
        );
        // -1125 是「重建 listenKey」信号,绝不能进重试循环。
        assert_eq!(
            binance_err(CODE_LISTEN_KEY_NOT_EXIST, 400).outcome_for_write(),
            PmOutcome::Rejected
        );
    }

    #[test]
    fn duplicate_signals_imply_order_may_exist() {
        assert!(binance_err(CODE_DUPLICATE_CLIENT_ORDER_ID, 400).implies_order_may_exist());
        let margin_dup = BinancePmHttpError::BinanceError {
            code: CODE_NEW_ORDER_REJECTED,
            msg: MSG_DUPLICATE_ORDER.to_string(),
            status: 400,
        };
        assert!(margin_dup.implies_order_may_exist());
        // 普通 -2010(如穿价)不是「原单存在」信号。
        let gtx_like = BinancePmHttpError::BinanceError {
            code: CODE_NEW_ORDER_REJECTED,
            msg: MSG_WOULD_IMMEDIATELY_MATCH.to_string(),
            status: 400,
        };
        assert!(!gtx_like.implies_order_may_exist());
    }
}
