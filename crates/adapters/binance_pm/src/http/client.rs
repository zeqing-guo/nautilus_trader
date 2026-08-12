//! papi HTTP client(第一切片:公开探针 + 签名只读端点)。
//!
//! 复用上游 `nautilus_binance::common::credential::SigningCredential`(HMAC/Ed25519
//! 自动识别;papi 只支持 HMAC/RSA,故此处凭证实际恒为 HMAC 分支)与
//! `nautilus_network::http::HttpClient`(GCRA 限速)。签名材料拼装方式与上游
//! futures client 完全一致:`query + timestamp[&recvWindow]` → sign → 追加
//! `&signature=`(percent-encode 对 HMAC hex 是 no-op)。

use std::collections::HashMap;

use jiff::Timestamp;
use nautilus_binance::common::consts::BINANCE_API_KEY_HEADER;
use nautilus_binance::common::credential::SigningCredential;
use nautilus_network::http::{HttpClient, HttpResponse, Method};
use serde::{Serialize, de::DeserializeOwned};

use crate::common::consts::{
    BINANCE_PM_HTTP_URL, PM_GLOBAL_RATE_KEY, PM_ORDER_RATE_KEY, pm_order_quota, pm_request_quota,
};
use crate::common::error::{BinancePmHttpError, BinancePmHttpResult};
use crate::http::models::{
    BinanceErrorResponse, PmAccount, PmBalance, PmServerTime, PmUmPositionRisk,
};
use crate::http::query::{PmBalanceParams, PmPositionRiskParams};

/// papi REST client。
#[derive(Debug)]
pub struct BinancePmHttpClient {
    client: HttpClient,
    base_url: String,
    credential: Option<SigningCredential>,
    recv_window: Option<u64>,
}

impl BinancePmHttpClient {
    /// 构造 client。
    ///
    /// `api_key`/`api_secret` 必须成对提供(或都不提供,仅可调公开端点);
    /// `base_url_override` 用于代理/未来基址迁移,缺省 [`BINANCE_PM_HTTP_URL`]。
    ///
    /// # Errors
    ///
    /// 凭证不成对,或底层 `HttpClient` 构建失败时报错。
    pub fn new(
        api_key: Option<String>,
        api_secret: Option<String>,
        base_url_override: Option<String>,
        recv_window: Option<u64>,
        timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> BinancePmHttpResult<Self> {
        let credential = match (api_key, api_secret) {
            (Some(key), Some(secret)) => Some(SigningCredential::new(key, secret)),
            (None, None) => None,
            _ => return Err(BinancePmHttpError::MissingCredentials),
        };

        let keyed_quotas = vec![
            (PM_GLOBAL_RATE_KEY.to_string(), pm_request_quota()),
            (PM_ORDER_RATE_KEY.to_string(), pm_order_quota()),
        ];

        let client = HttpClient::new(
            HashMap::new(),
            vec![BINANCE_API_KEY_HEADER.to_string()],
            keyed_quotas,
            None,
            timeout_secs,
            proxy_url,
        )?;

        let base_url = base_url_override
            .unwrap_or_else(|| BINANCE_PM_HTTP_URL.to_string())
            .trim_end_matches('/')
            .to_string();

        Ok(Self {
            client,
            base_url,
            credential,
            recv_window,
        })
    }

    /// `GET /papi/v1/ping`(公开,连通性探针)。
    ///
    /// # Errors
    ///
    /// 传输失败或非 2xx 时报错。
    pub async fn ping(&self) -> BinancePmHttpResult<()> {
        let _: serde_json::Value = self
            .request::<(), _>(Method::GET, "/papi/v1/ping", None, false, false)
            .await?;
        Ok(())
    }

    /// `GET /papi/v1/time`(公开)。
    ///
    /// # Errors
    ///
    /// 传输失败或非 2xx 时报错。
    pub async fn server_time(&self) -> BinancePmHttpResult<PmServerTime> {
        self.request::<(), _>(Method::GET, "/papi/v1/time", None, false, false)
            .await
    }

    /// `GET /papi/v1/balance`(签名)——全资产余额。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn balances(&self) -> BinancePmHttpResult<Vec<PmBalance>> {
        self.request::<(), _>(Method::GET, "/papi/v1/balance", None, true, false)
            .await
    }

    /// `GET /papi/v1/balance?asset=`(签名)——单资产余额(papi 语义:带 asset
    /// 返回单对象而非数组)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn balance(&self, asset: &str) -> BinancePmHttpResult<PmBalance> {
        let params = PmBalanceParams {
            asset: Some(asset.to_string()),
        };
        self.request(Method::GET, "/papi/v1/balance", Some(&params), true, false)
            .await
    }

    /// `GET /papi/v1/account`(签名)——账户级风险指标(uniMMR/权益/维持保证金)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn account(&self) -> BinancePmHttpResult<PmAccount> {
        self.request::<(), _>(Method::GET, "/papi/v1/account", None, true, false)
            .await
    }

    /// `GET /papi/v1/um/positionRisk`(签名)——UM 持仓。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn um_position_risk(
        &self,
        symbol: Option<&str>,
    ) -> BinancePmHttpResult<Vec<PmUmPositionRisk>> {
        let params = PmPositionRiskParams {
            symbol: symbol.map(ToString::to_string),
        };
        self.request(
            Method::GET,
            "/papi/v1/um/positionRisk",
            Some(&params),
            true,
            false,
        )
        .await
    }

    /// 通用请求:可选签名,`use_order_quota` 决定是否额外占用下单配额桶。
    async fn request<P, T>(
        &self,
        method: Method,
        path: &str,
        params: Option<&P>,
        signed: bool,
        use_order_quota: bool,
    ) -> BinancePmHttpResult<T>
    where
        P: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let mut query = params
            .map(serde_urlencoded::to_string)
            .transpose()
            .map_err(|e| BinancePmHttpError::ValidationError(e.to_string()))?
            .unwrap_or_default();

        let mut headers = HashMap::new();

        if signed {
            let cred = self
                .credential
                .as_ref()
                .ok_or(BinancePmHttpError::MissingCredentials)?;

            if !query.is_empty() {
                query.push('&');
            }

            let timestamp = Timestamp::now().as_millisecond();
            query.push_str(&format!("timestamp={timestamp}"));

            if let Some(recv_window) = self.recv_window {
                query.push_str(&format!("&recvWindow={recv_window}"));
            }

            let signature = percent_encode(&cred.sign(&query));
            query.push_str(&format!("&signature={signature}"));
            headers.insert(
                BINANCE_API_KEY_HEADER.to_string(),
                cred.api_key().to_string(),
            );
        }

        let url = build_url(&self.base_url, path, &query);
        let keys = rate_limit_keys(use_order_quota);

        let response = self
            .client
            .request(
                method,
                url,
                None::<&HashMap<String, Vec<String>>>,
                Some(headers),
                None,
                None,
                Some(keys),
            )
            .await?;

        if !response.status.is_success() {
            return Err(parse_error_response(&response));
        }

        serde_json::from_slice::<T>(&response.body)
            .map_err(|e| BinancePmHttpError::JsonError(e.to_string()))
    }
}

/// 拼 URL(papi 路径一律全路径传入,不做 api_path 前缀推断)。
fn build_url(base_url: &str, path: &str, query: &str) -> String {
    let mut url = format!("{base_url}{path}");
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }
    url
}

fn rate_limit_keys(use_orders: bool) -> Vec<String> {
    if use_orders {
        vec![
            PM_GLOBAL_RATE_KEY.to_string(),
            PM_ORDER_RATE_KEY.to_string(),
        ]
    } else {
        vec![PM_GLOBAL_RATE_KEY.to_string()]
    }
}

/// 与上游 futures client 相同的 percent-encode(HMAC hex 为 no-op,
/// Ed25519 base64 的 `+/=` 会被转义——papi 恒 HMAC,保留以防呆)。
fn percent_encode(input: &str) -> String {
    let mut result = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push('%');
                result.push_str(&format!("{byte:02X}"));
            }
        }
    }
    result
}

fn parse_error_response(response: &HttpResponse) -> BinancePmHttpError {
    let status = response.status.as_u16();
    let body = String::from_utf8_lossy(&response.body).to_string();

    if let Ok(err) = serde_json::from_str::<BinanceErrorResponse>(&body) {
        return BinancePmHttpError::BinanceError {
            code: err.code,
            msg: err.msg,
            status,
        };
    }

    BinancePmHttpError::UnexpectedStatus { status, body }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Binance 官方 SIGNED endpoint 示例向量(文档给出 secret/query/签名),
    /// 平移自 fuzzy-trading `binance_common.rs`,锁死 papi 的 HMAC 签名材料拼装方式。
    const OFFICIAL_SECRET: &str =
        "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
    const OFFICIAL_QUERY: &str = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
    const OFFICIAL_SIGNATURE: &str =
        "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71";

    #[test]
    fn signing_credential_falls_back_to_hmac_and_matches_official_vector() {
        let cred = SigningCredential::new("key".to_string(), OFFICIAL_SECRET.to_string());
        // papi 只支持 HMAC——该 secret 必须落 HMAC 分支而非被误判为 Ed25519。
        assert!(!cred.is_ed25519());
        assert_eq!(cred.sign(OFFICIAL_QUERY), OFFICIAL_SIGNATURE);
        // HMAC hex 经 percent-encode 应为 no-op。
        assert_eq!(percent_encode(OFFICIAL_SIGNATURE), OFFICIAL_SIGNATURE);
    }

    #[test]
    fn build_url_shapes() {
        assert_eq!(
            build_url("https://papi.binance.com", "/papi/v1/ping", ""),
            "https://papi.binance.com/papi/v1/ping"
        );
        assert_eq!(
            build_url("https://papi.binance.com", "/papi/v1/balance", "asset=USDT"),
            "https://papi.binance.com/papi/v1/balance?asset=USDT"
        );
    }

    #[test]
    fn rate_limit_keys_include_order_bucket_only_for_orders() {
        assert_eq!(rate_limit_keys(false), vec![PM_GLOBAL_RATE_KEY.to_string()]);
        assert_eq!(
            rate_limit_keys(true),
            vec![
                PM_GLOBAL_RATE_KEY.to_string(),
                PM_ORDER_RATE_KEY.to_string()
            ]
        );
    }
}
