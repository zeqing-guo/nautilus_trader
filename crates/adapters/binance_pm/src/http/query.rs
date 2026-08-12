//! papi REST 查询参数(第一切片:只读端点)。

use serde::Serialize;

/// `GET /papi/v1/balance` 参数。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmBalanceParams {
    /// 指定资产(缺省返回全部)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,
}

/// `GET /papi/v1/um/positionRisk` 参数。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PmPositionRiskParams {
    /// 指定交易对(缺省返回全部)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}
