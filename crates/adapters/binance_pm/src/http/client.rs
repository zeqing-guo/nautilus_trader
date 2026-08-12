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
    BinanceErrorResponse, PmAccount, PmBalance, PmIncome, PmListenKey, PmMarginOrder,
    PmMarginTrade, PmPositionMode, PmRateLimitOrder, PmServerTime, PmUmOrder, PmUmPositionRisk,
    PmUmTrade,
};
use crate::http::query::{
    PmAllOrdersParams, PmBalanceParams, PmIncomeParams, PmMarginNewOrderParams, PmOpenOrdersParams,
    PmOrderRefParams, PmPositionRiskParams, PmTradesParams, PmUmNewOrderParams,
};

/// 端点安全模式(官方 Security Type 三态)。
///
/// 注意:listenKey(USER_STREAM)是 **仅 API-Key、不签名**——官方 SDK 实现如此
/// (general-info 的 Security Type 表述有误,以 SDK/实测为准);PUT 不带任何参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Security {
    /// 无鉴权(papi 仅 ping/time)。
    Public,
    /// 仅 `X-MBX-APIKEY` 头,不签名(listenKey 生命周期)。
    ApiKeyOnly,
    /// API-Key 头 + HMAC 签名(其余全部端点)。
    Signed,
}

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
            .request::<(), _>(Method::GET, "/papi/v1/ping", None, Security::Public, false)
            .await?;
        Ok(())
    }

    /// `GET /papi/v1/time`(公开)。
    ///
    /// # Errors
    ///
    /// 传输失败或非 2xx 时报错。
    pub async fn server_time(&self) -> BinancePmHttpResult<PmServerTime> {
        self.request::<(), _>(Method::GET, "/papi/v1/time", None, Security::Public, false)
            .await
    }

    /// `GET /papi/v1/balance`(签名)——全资产余额。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn balances(&self) -> BinancePmHttpResult<Vec<PmBalance>> {
        self.request::<(), _>(
            Method::GET,
            "/papi/v1/balance",
            None,
            Security::Signed,
            false,
        )
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
        self.request(
            Method::GET,
            "/papi/v1/balance",
            Some(&params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/account`(签名)——账户级风险指标(uniMMR/权益/维持保证金)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn account(&self) -> BinancePmHttpResult<PmAccount> {
        self.request::<(), _>(
            Method::GET,
            "/papi/v1/account",
            None,
            Security::Signed,
            false,
        )
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
            Security::Signed,
            false,
        )
        .await
    }

    /// `POST /papi/v1/um/order`(签名,占下单配额)——UM 腿下单。
    ///
    /// 写路径纪律:调用方须先 `validate_client_order_id`;结果错误经
    /// `outcome_for_write` 三分类——GTX 穿价 `-5022` 归 Rejected(确定不存在,
    /// 可改价重发,**绝不当 Unknown 走查单**);超时/5xx 归 Unknown(禁盲重发,
    /// 按 client_order_id 查单裁决)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn submit_um_order(
        &self,
        params: &PmUmNewOrderParams,
    ) -> BinancePmHttpResult<PmUmOrder> {
        self.request(
            Method::POST,
            "/papi/v1/um/order",
            Some(params),
            Security::Signed,
            true,
        )
        .await
    }

    /// `POST /papi/v1/margin/order`(签名,占下单配额)——margin 腿下单
    /// (sideEffectType 恒 NO_SIDE_EFFECT,由参数类型保证)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错(LIMIT_MAKER 穿价 = `-2010` +
    /// `"Order would immediately match and take."`)。
    pub async fn submit_margin_order(
        &self,
        params: &PmMarginNewOrderParams,
    ) -> BinancePmHttpResult<PmMarginOrder> {
        self.request(
            Method::POST,
            "/papi/v1/margin/order",
            Some(params),
            Security::Signed,
            true,
        )
        .await
    }

    /// `DELETE /papi/v1/um/order`(签名)——UM 撤单。
    ///
    /// `-2011`/`-2013` = 订单未知/不存在 → 转 Reconciler 按 client_order_id
    /// 裁决(注意 -2013 只在下单后 3 天内可信)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn cancel_um_order(
        &self,
        params: &PmOrderRefParams,
    ) -> BinancePmHttpResult<PmUmOrder> {
        self.request(
            Method::DELETE,
            "/papi/v1/um/order",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `DELETE /papi/v1/margin/order`(签名)——margin 撤单。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn cancel_margin_order(
        &self,
        params: &PmOrderRefParams,
    ) -> BinancePmHttpResult<PmMarginOrder> {
        self.request(
            Method::DELETE,
            "/papi/v1/margin/order",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/um/order`(签名)——UM 查单(Reconciler 裁决路径)。
    ///
    /// ⚠️ 3 天保留期:CANCELED/EXPIRED 且无成交且超 3 天的单查不到——
    /// 调用方必须携带下单时刻,只在 T+3d 内信任 `-2013`,超窗降级为
    /// userTrades 成交流水裁决。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn query_um_order(
        &self,
        params: &PmOrderRefParams,
    ) -> BinancePmHttpResult<PmUmOrder> {
        self.request(
            Method::GET,
            "/papi/v1/um/order",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/margin/order`(签名,权重 10)——margin 查单。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn query_margin_order(
        &self,
        params: &PmOrderRefParams,
    ) -> BinancePmHttpResult<PmMarginOrder> {
        self.request(
            Method::GET,
            "/papi/v1/margin/order",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/um/openOrders?symbol=`(签名,带 symbol 权重 1)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn um_open_orders(&self, symbol: &str) -> BinancePmHttpResult<Vec<PmUmOrder>> {
        let params = PmOpenOrdersParams {
            symbol: symbol.to_string(),
        };
        self.request(
            Method::GET,
            "/papi/v1/um/openOrders",
            Some(&params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/margin/openOrders?symbol=`(签名,权重 5)。
    ///
    /// symbol 强制:不带 symbol 按全市场 symbol 数计费(天价)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn margin_open_orders(
        &self,
        symbol: &str,
    ) -> BinancePmHttpResult<Vec<PmMarginOrder>> {
        let params = PmOpenOrdersParams {
            symbol: symbol.to_string(),
        };
        self.request(
            Method::GET,
            "/papi/v1/margin/openOrders",
            Some(&params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/um/allOrders`(签名,权重 5;时间跨度 <7 天)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn um_all_orders(
        &self,
        params: &PmAllOrdersParams,
    ) -> BinancePmHttpResult<Vec<PmUmOrder>> {
        self.request(
            Method::GET,
            "/papi/v1/um/allOrders",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/margin/allOrders`(签名,**权重 100**——每分钟最多 60 次,
    /// 严禁按 symbol 循环,须时间窗合并 + 缓存)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn margin_all_orders(
        &self,
        params: &PmAllOrdersParams,
    ) -> BinancePmHttpResult<Vec<PmMarginOrder>> {
        self.request(
            Method::GET,
            "/papi/v1/margin/allOrders",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/um/userTrades`(签名,权重 5;时间窗 ≤7 天,fromId 与
    /// 时间窗互斥)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn um_user_trades(
        &self,
        params: &PmTradesParams,
    ) -> BinancePmHttpResult<Vec<PmUmTrade>> {
        self.request(
            Method::GET,
            "/papi/v1/um/userTrades",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/margin/myTrades`(签名,权重 5;**时间窗 <24 小时**,
    /// 补历史须按天切片)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn margin_my_trades(
        &self,
        params: &PmTradesParams,
    ) -> BinancePmHttpResult<Vec<PmMarginTrade>> {
        self.request(
            Method::GET,
            "/papi/v1/margin/myTrades",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/um/positionSide/dual`(签名)——持仓模式。
    ///
    /// 启动强制校验:`dual_side_position` 必须为 false(one-way),否则
    /// reduceOnly 语义不成立,执行客户端应拒绝启动。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn um_position_mode(&self) -> BinancePmHttpResult<PmPositionMode> {
        self.request::<(), _>(
            Method::GET,
            "/papi/v1/um/positionSide/dual",
            None,
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/rateLimit/order`(签名,权重 1)——下单限速用量,接
    /// 健康检查。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn order_rate_limit(&self) -> BinancePmHttpResult<Vec<PmRateLimitOrder>> {
        self.request::<(), _>(
            Method::GET,
            "/papi/v1/rateLimit/order",
            None,
            Security::Signed,
            false,
        )
        .await
    }

    /// `GET /papi/v1/um/income`(签名,权重 30)——收益流水(FUNDING_FEE 归档;
    /// 仅保留 3 个月,长周期归档须落库;实时归因走 WS `ACCOUNT_UPDATE`)。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn um_income(&self, params: &PmIncomeParams) -> BinancePmHttpResult<Vec<PmIncome>> {
        self.request(
            Method::GET,
            "/papi/v1/um/income",
            Some(params),
            Security::Signed,
            false,
        )
        .await
    }

    /// `DELETE /papi/v1/um/allOpenOrders?symbol=`(签名)——UM 按 symbol 全撤。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn cancel_all_um_orders(&self, symbol: &str) -> BinancePmHttpResult<()> {
        let params = PmOpenOrdersParams {
            symbol: symbol.to_string(),
        };
        let _: serde_json::Value = self
            .request(
                Method::DELETE,
                "/papi/v1/um/allOpenOrders",
                Some(&params),
                Security::Signed,
                false,
            )
            .await?;
        Ok(())
    }

    /// `DELETE /papi/v1/margin/allOpenOrders?symbol=`(签名)——margin 按
    /// symbol 全撤。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn cancel_all_margin_orders(&self, symbol: &str) -> BinancePmHttpResult<()> {
        let params = PmOpenOrdersParams {
            symbol: symbol.to_string(),
        };
        let _: serde_json::Value = self
            .request(
                Method::DELETE,
                "/papi/v1/margin/allOpenOrders",
                Some(&params),
                Security::Signed,
                false,
            )
            .await?;
        Ok(())
    }

    /// `POST /papi/v1/listenKey`(仅 API-Key)——创建/续期用户流 listenKey。
    ///
    /// papi 语义:已有活跃 key 时返回同一个 key 并续期 60 分钟。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn create_listen_key(&self) -> BinancePmHttpResult<PmListenKey> {
        self.request::<(), _>(
            Method::POST,
            "/papi/v1/listenKey",
            None,
            Security::ApiKeyOnly,
            false,
        )
        .await
    }

    /// `PUT /papi/v1/listenKey`(仅 API-Key,**不带任何参数**)——续期 60 分钟。
    ///
    /// 失败返回 `-1125`(listenKey 不存在)时,调用方必须**重建**(重新 POST)
    /// 而不是重试本请求。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误(含 -1125)时报错。
    pub async fn keepalive_listen_key(&self) -> BinancePmHttpResult<()> {
        let _: serde_json::Value = self
            .request::<(), _>(
                Method::PUT,
                "/papi/v1/listenKey",
                None,
                Security::ApiKeyOnly,
                false,
            )
            .await?;
        Ok(())
    }

    /// `DELETE /papi/v1/listenKey`(仅 API-Key)——立即失效用户流。
    ///
    /// # Errors
    ///
    /// 未配置凭证、传输失败或业务错误时报错。
    pub async fn close_listen_key(&self) -> BinancePmHttpResult<()> {
        let _: serde_json::Value = self
            .request::<(), _>(
                Method::DELETE,
                "/papi/v1/listenKey",
                None,
                Security::ApiKeyOnly,
                false,
            )
            .await?;
        Ok(())
    }

    /// 通用请求:按安全模式处理鉴权,`use_order_quota` 决定是否额外占用下单配额桶。
    async fn request<P, T>(
        &self,
        method: Method,
        path: &str,
        params: Option<&P>,
        security: Security,
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

        match security {
            Security::Public => {}
            Security::ApiKeyOnly => {
                let cred = self
                    .credential
                    .as_ref()
                    .ok_or(BinancePmHttpError::MissingCredentials)?;
                headers.insert(
                    BINANCE_API_KEY_HEADER.to_string(),
                    cred.api_key().to_string(),
                );
            }
            Security::Signed => {
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
