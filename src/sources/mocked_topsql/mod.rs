use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
};

use crate::sources::mocked_topsql::controller::Controller;

mod controller;
pub mod shutdown;

/// PLACEHOLDER
#[configurable_component(source("mocked_topsql"))]
#[derive(Debug, Clone)]
pub struct MockedTopSQLConfig {
    /// Top N queries to collect
    #[serde(default = "default_top_n")]
    pub top_n: usize,

    /// Downsampling interval
    #[serde(default = "default_downsampling_interval")]
    pub downsampling_interval: u32,

    /// TiDB node number
    pub tidb_number: usize,

    /// TiKV node number
    pub tikv_number: usize,

    /// Extra column number
    #[serde(default = "default_extra_column_number")]
    pub extra_column_number: u32,
}

pub const fn default_top_n() -> usize {
    0
}

pub const fn default_downsampling_interval() -> u32 {
    0
}

pub const fn default_tidb_number() -> usize {
    5
}

pub const fn default_tikv_number() -> usize {
    5
}

pub const fn default_extra_column_number() -> u32 {
    0
}

impl GenerateConfig for MockedTopSQLConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            top_n: default_top_n(),
            downsampling_interval: default_downsampling_interval(),
            tidb_number: default_tidb_number(),
            tikv_number: default_tikv_number(),
            extra_column_number: default_extra_column_number(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "mocked_topsql")]
impl SourceConfig for MockedTopSQLConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        let top_n = self.top_n;
        let downsampling_interval = self.downsampling_interval;
        let tidb_number = self.tidb_number;
        let tikv_number = self.tikv_number;
        let extra_column_number = self.extra_column_number;

        Ok(Box::pin(async move {
            let controller = Controller::new(
                top_n,
                downsampling_interval,
                tidb_number,
                tikv_number,
                extra_column_number,
                cx.out,
            )
            .await
            .map_err(|error| error!(message = "Source failed.", %error))?;

            controller.run(cx.shutdown).await;

            Ok(())
        }))
    }

    fn outputs(&self, _: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput {
            port: None,
            ty: DataType::Log,
            schema_definition: None,
        }]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}
