//! papi HTTP client 注入测试(axum mock;L0 验证方案第 2/3 项)。
//!
//! 覆盖写路径三分类在真实 HTTP 语义下的行为:
//! - GTX 穿价 `-5022` → Rejected 且 post-only 拒单可识别(确定未达,可改价重发);
//! - margin `-2010 Duplicate` → Rejected 且「原单可能存在」信号(转查单收敛);
//! - 官方 503 "Unknown error…" 文案 → Unknown(严禁盲重发);
//! - 传输超时 → Unknown;
//! - 查单 `-2013` → 调用方可判「不存在」(三天窗语义在执行层);
//! - listenKey PUT `-1125` → Rejected(重建而非重试)。

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use nautilus_binance_pm::common::error::{
    BinancePmHttpError, CODE_GTX_REJECT, CODE_LISTEN_KEY_NOT_EXIST, CODE_ORDER_DOES_NOT_EXIST,
    MSG_503_UNKNOWN, PmOutcome,
};
use nautilus_binance_pm::http::client::BinancePmHttpClient;
use nautilus_binance_pm::http::query::{PmOrderRefParams, PmUmNewOrderParams};

async fn start_mock(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn client_for(addr: SocketAddr, timeout_secs: u64) -> BinancePmHttpClient {
    BinancePmHttpClient::new(
        Some("test-key".to_string()),
        Some("test-secret".to_string()),
        Some(format!("http://{addr}")),
        Some(5000),
        Some(timeout_secs),
        None,
    )
    .unwrap()
}

fn um_order_params() -> PmUmNewOrderParams {
    PmUmNewOrderParams {
        symbol: "SOLUSDC".to_string(),
        side: "SELL".to_string(),
        order_type: "LIMIT".to_string(),
        time_in_force: Some("GTX".to_string()),
        quantity: Some("1.34".to_string()),
        price: Some("180.50".to_string()),
        reduce_only: Some(false),
        position_side: None,
        new_client_order_id: Some("ft01TEST0000000000000000000A".to_string()),
        new_order_resp_type: Some("RESULT".to_string()),
        price_match: None,
        self_trade_prevention_mode: None,
        good_till_date: None,
    }
}

#[tokio::test]
async fn gtx_cross_rejection_is_definitively_rejected() {
    let router = Router::new().route(
        "/papi/v1/um/order",
        post(|| async {
            (
                StatusCode::BAD_REQUEST,
                r#"{"code":-5022,"msg":"Due to the order could not be executed as maker, the Post Only order will be rejected."}"#,
            )
        }),
    );
    let addr = start_mock(router).await;
    let client = client_for(addr, 5);

    let err = client
        .submit_um_order(&um_order_params())
        .await
        .unwrap_err();
    // -5022 = 确定未达且不留痕迹:必须是明确拒绝,绝不能降级 Unknown 走查单。
    assert_eq!(err.outcome_for_write(), PmOutcome::Rejected);
    match &err {
        BinancePmHttpError::BinanceError { code, .. } => assert_eq!(*code, CODE_GTX_REJECT),
        other => panic!("期望 BinanceError,得到 {other:?}"),
    }
    assert!(!err.implies_order_may_exist());
}

#[tokio::test]
async fn margin_duplicate_order_signals_order_may_exist() {
    let router = Router::new().route(
        "/papi/v1/margin/order",
        post(|| async {
            (
                StatusCode::BAD_REQUEST,
                r#"{"code":-2010,"msg":"Duplicate order sent."}"#,
            )
        }),
    );
    let addr = start_mock(router).await;
    let client = client_for(addr, 5);

    let mut params = nautilus_binance_pm::http::query::PmMarginNewOrderParams::new(
        "SOLUSDC".to_string(),
        "BUY".to_string(),
        "LIMIT_MAKER".to_string(),
    );
    params.quantity = Some("1.34".to_string());
    params.price = Some("179.80".to_string());

    let err = client.submit_margin_order(&params).await.unwrap_err();
    // 幂等重发的正常结局:分类 Rejected,但必须携带「原单存在」强信号转查单。
    assert_eq!(err.outcome_for_write(), PmOutcome::Rejected);
    assert!(err.implies_order_may_exist());
}

#[tokio::test]
async fn official_503_unknown_message_is_unknown() {
    let router = Router::new().route(
        "/papi/v1/um/order",
        post(|| async {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!(r#"{{"code":-1000,"msg":"{MSG_503_UNKNOWN}"}}"#),
            )
        }),
    );
    let addr = start_mock(router).await;
    let client = client_for(addr, 5);

    let err = client
        .submit_um_order(&um_order_params())
        .await
        .unwrap_err();
    // 官方原文:执行状态 UNKNOWN,严禁当失败——必须走查单裁决。
    assert_eq!(err.outcome_for_write(), PmOutcome::Unknown);
}

#[tokio::test]
async fn transport_timeout_is_unknown_never_blind_resend() {
    let router = Router::new().route(
        "/papi/v1/um/order",
        post(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            (StatusCode::OK, "{}")
        }),
    );
    let addr = start_mock(router).await;
    let client = client_for(addr, 1); // 1s 超时 < 5s 延迟

    let err = client
        .submit_um_order(&um_order_params())
        .await
        .unwrap_err();
    // 请求已发出、无确定应答:事故根源场景,必须 Unknown。
    assert_eq!(err.outcome_for_write(), PmOutcome::Unknown);
}

#[tokio::test]
async fn query_order_2013_means_not_found() {
    let router = Router::new().route(
        "/papi/v1/um/order",
        get(|| async {
            (
                StatusCode::BAD_REQUEST,
                r#"{"code":-2013,"msg":"Order does not exist."}"#,
            )
        }),
    );
    let addr = start_mock(router).await;
    let client = client_for(addr, 5);

    let params =
        PmOrderRefParams::by_client_order_id("SOLUSDC".to_string(), "ft01TEST".to_string());
    let err = client.query_um_order(&params).await.unwrap_err();
    match &err {
        BinancePmHttpError::BinanceError { code, .. } => {
            assert_eq!(*code, CODE_ORDER_DOES_NOT_EXIST);
        }
        other => panic!("期望 BinanceError,得到 {other:?}"),
    }
}

#[tokio::test]
async fn listen_key_put_1125_is_rebuild_not_retry() {
    let router = Router::new().route(
        "/papi/v1/listenKey",
        put(|| async {
            (
                StatusCode::BAD_REQUEST,
                r#"{"code":-1125,"msg":"This listenKey does not exist."}"#,
            )
        }),
    );
    let addr = start_mock(router).await;
    let client = client_for(addr, 5);

    let err = client.keepalive_listen_key().await.unwrap_err();
    match &err {
        BinancePmHttpError::BinanceError { code, .. } => {
            assert_eq!(*code, CODE_LISTEN_KEY_NOT_EXIST);
        }
        other => panic!("期望 BinanceError,得到 {other:?}"),
    }
    // 分类为明确拒绝:生命周期状态机据此走「作废+重建」而非重试 PUT。
    assert_eq!(err.outcome_for_write(), PmOutcome::Rejected);
}

#[tokio::test]
async fn successful_um_submit_parses_result() {
    let router = Router::new().route(
        "/papi/v1/um/order",
        post(|| async {
            (
                StatusCode::OK,
                r#"{"symbol":"SOLUSDC","orderId":9988776655,
                    "clientOrderId":"ft01TEST0000000000000000000A","status":"NEW",
                    "price":"180.50","avgPrice":"0.00","origQty":"1.34","executedQty":"0",
                    "cumQuote":"0","timeInForce":"GTX","type":"LIMIT","reduceOnly":false,
                    "side":"SELL","positionSide":"BOTH","updateTime":1786500000000}"#,
            )
        }),
    );
    let addr = start_mock(router).await;
    let client = client_for(addr, 5);

    let order = client.submit_um_order(&um_order_params()).await.unwrap();
    assert_eq!(order.order_id, 9988776655);
    assert_eq!(order.status, "NEW");
    assert_eq!(order.time_in_force, "GTX");
}
