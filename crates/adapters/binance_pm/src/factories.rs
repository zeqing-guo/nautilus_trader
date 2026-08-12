//! PM 执行客户端工厂(挂进 `LiveNodeBuilder::add_exec_client`)。
//!
//! 行情侧无需本 crate 工厂:papi 无公共行情端点,直接复用上游
//! `nautilus_binance::factories::BinanceDataClientFactory`(spot + futures 公共流,
//! instrument 也是同一批)。

use nautilus_common::cache::CacheView;
use nautilus_common::clients::ExecutionClient;
use nautilus_common::factories::{ClientConfig, ExecutionClientFactory};
use nautilus_live::ExecutionClientCore;
use nautilus_model::enums::{AccountType, OmsType};
use nautilus_model::identifiers::ClientId;

use nautilus_binance::common::consts::BINANCE_VENUE;

use crate::config::BinancePmExecClientConfig;
use crate::execution::BinancePmExecutionClient;

/// PM 执行客户端工厂。
#[derive(Debug, Clone)]
pub struct BinancePmExecutionClientFactory;

impl ExecutionClientFactory for BinancePmExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let pm_config = config
            .as_any()
            .downcast_ref::<BinancePmExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!("工厂配置类型不匹配:期望 BinancePmExecClientConfig,实际 {config:?}")
            })?
            .clone();

        // PM 统一账户:Margin 账户模型(负债=负余额)+ one-way 净持仓。
        // venue 恒 BINANCE——一个 venue 一个 exec client,本 client 管两条腿。
        let core = ExecutionClientCore::new(
            pm_config.trader_id,
            ClientId::from(name),
            *BINANCE_VENUE,
            OmsType::Netting,
            pm_config.account_id,
            AccountType::Margin,
            None, // base_currency:多资产抵押,无单一基准币
            cache,
        );

        let client = BinancePmExecutionClient::new(core, pm_config)?;
        Ok(Box::new(client))
    }

    #[allow(clippy::unnecessary_literal_bound)] // trait 签名固定 &str
    fn name(&self) -> &str {
        "BINANCE_PM"
    }

    #[allow(clippy::unnecessary_literal_bound)] // trait 签名固定 &str
    fn config_type(&self) -> &str {
        "BinancePmExecClientConfig"
    }
}
