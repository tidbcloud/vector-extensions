//! Alicloud OSS sink — Vector `aws_s3`-compatible surface with RRSA/OIDC STS auth.
//!
//! Uses the S3-compatible PutObject API against OSS endpoints, but authenticates
//! via Aliyun `AssumeRoleWithOIDC` (ACK RRSA) so long-lived static AK/SK are not required.

mod request_builder;

use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use vector::aws::RegionOrEndpoint;
use vector::codecs::{Encoder, EncodingConfigWithFraming, SinkType};
use vector::config::{GenerateConfig, SinkConfig, SinkContext};
use vector::sinks::s3_common::{
    self,
    config::{RetryStrategy, S3Options},
    partitioner::S3KeyPartitioner,
    service::{S3Request, S3Service},
    sink::S3Sink,
};
use vector::sinks::util::{
    timezone_to_offset, BatchConfig, BulkSizeBasedDefaultBatchSettings, Compression,
    ServiceBuilderExt, TowerRequestConfig,
};
use vector::sinks::Healthcheck;
use vector::template::Template;
use vector_lib::codecs::encoding::{Framer, FramingConfig};
use vector_lib::codecs::TextSerializerConfig;
use vector_lib::config::proxy::ProxyConfig;
use vector_lib::config::{AcknowledgementsConfig, Input};
use vector_lib::configurable::configurable_component;
use vector_lib::sink::VectorSink;
use vector_lib::tls::TlsConfig;
use vector_lib::TimeZone;

use crate::common::aliyun_sts::AliyunOidcCredentialsProvider;
use crate::sinks::alicloud_oss::request_builder::OssRequestOptions;

fn default_key_prefix() -> String {
    "date=%F".to_string()
}

fn default_filename_time_format() -> String {
    "%s".to_string()
}

fn default_force_path_style() -> bool {
    // Match existing Alicloud OSS virtual-hosted-style configs in app-delivery.
    false
}

/// Optional auth overrides for RRSA. Defaults come from `ALIBABA_CLOUD_*` env vars.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AlicloudOssAuthConfig {
    /// RAM role ARN (`acs:ram::...:role/...`). Defaults to `ALIBABA_CLOUD_ROLE_ARN`.
    #[configurable(metadata(docs::examples = "acs:ram::123456789012:role/o11y-vector"))]
    pub role_arn: Option<String>,
}

/// Configuration for the `alicloud_oss` sink.
#[configurable_component(sink(
    "alicloud_oss",
    "Store observability events in Alibaba Cloud Object Storage Service (OSS)."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct AlicloudOssSinkConfig {
    /// The OSS bucket name (without `oss://` prefix or trailing `/`).
    #[configurable(metadata(docs::examples = "my-bucket"))]
    pub bucket: String,

    /// Prefix applied to all object keys. Supports Vector templates.
    #[serde(default = "default_key_prefix")]
    #[configurable(metadata(docs::templateable))]
    #[configurable(metadata(docs::examples = "0/next-gen/{{ o11y.cluster_id }}/logs/"))]
    pub key_prefix: String,

    /// Timestamp format appended to the object key (`strftime`). Empty disables it.
    #[serde(default = "default_filename_time_format")]
    pub filename_time_format: String,

    /// Whether to append a UUID v4 token to the object key.
    #[serde(default = "vector::serde::default_true")]
    pub filename_append_uuid: bool,

    /// Optional filename extension override (otherwise derived from compression).
    #[configurable(metadata(docs::examples = "log"))]
    pub filename_extension: Option<String>,

    #[configurable(derived)]
    #[serde(flatten)]
    pub options: S3Options,

    #[configurable(derived)]
    #[serde(flatten)]
    pub region: RegionOrEndpoint,

    #[configurable(derived)]
    #[serde(flatten)]
    pub encoding: EncodingConfigWithFraming,

    /// Compression for uploaded objects.
    #[configurable(derived)]
    #[serde(default = "Compression::gzip_default")]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<BulkSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    /// RRSA auth overrides. When omitted, uses `ALIBABA_CLOUD_*` environment variables.
    #[configurable(derived)]
    #[serde(default)]
    pub auth: AlicloudOssAuthConfig,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub timezone: Option<TimeZone>,

    /// Use path-style addressing (`bucket` in URL path). Default `false` for OSS virtual-host style.
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,

    /// Extra HTTP status codes that should be retried.
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "vector_lib::serde::is_default")]
    pub retry_strategy: RetryStrategy,
}

impl GenerateConfig for AlicloudOssSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            bucket: String::new(),
            key_prefix: default_key_prefix(),
            filename_time_format: default_filename_time_format(),
            filename_append_uuid: true,
            filename_extension: None,
            options: S3Options::default(),
            region: RegionOrEndpoint::default(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            compression: Compression::gzip_default(),
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            tls: Some(TlsConfig::default()),
            auth: AlicloudOssAuthConfig::default(),
            acknowledgements: Default::default(),
            timezone: Default::default(),
            force_path_style: default_force_path_style(),
            retry_strategy: Default::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "alicloud_oss")]
impl SinkConfig for AlicloudOssSinkConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let service = self.create_service(&cx.proxy).await?;
        let healthcheck = self.build_healthcheck(service.client())?;
        let sink = self.build_processor(service, cx)?;
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().1.input_type())
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl AlicloudOssSinkConfig {
    pub fn build_processor(
        &self,
        service: S3Service,
        cx: SinkContext,
    ) -> vector::Result<VectorSink> {
        let request_limits = self.request.into_settings();
        let retry_strategy = self.retry_strategy.clone();
        // Use the same tower ServiceBuilder type that Vector's ServiceBuilderExt impl targets.
        let service = tower::ServiceBuilder::new()
            .settings::<RetryStrategy, S3Request>(request_limits, retry_strategy)
            .service(service);

        let offset = self
            .timezone
            .or(cx.globals.timezone)
            .and_then(timezone_to_offset);

        let batch_settings = self.batch.into_batcher_settings()?;
        let key_prefix = Template::try_from(self.key_prefix.clone())?.with_tz_offset(offset);

        let ssekms_key_id = self
            .options
            .ssekms_key_id
            .as_ref()
            .cloned()
            .map(|ssekms_key_id| Template::try_from(ssekms_key_id.as_str()))
            .transpose()?;

        let partitioner = S3KeyPartitioner::new(key_prefix, ssekms_key_id, None);

        let transformer = self.encoding.transformer();
        let (framer, serializer) = self.encoding.build(SinkType::MessageBased)?;
        let encoder = Encoder::<Framer>::new(framer, serializer);

        let request_options = OssRequestOptions {
            bucket: self.bucket.clone(),
            api_options: self.options.clone(),
            filename_extension: self.filename_extension.clone(),
            filename_time_format: self.filename_time_format.clone(),
            filename_append_uuid: self.filename_append_uuid,
            encoder: (transformer, encoder),
            compression: self.compression,
            filename_tz_offset: offset,
        };

        let sink = S3Sink::new(service, request_options, partitioner, batch_settings);
        Ok(VectorSink::from_event_streamsink(sink))
    }

    pub fn build_healthcheck(&self, client: S3Client) -> vector::Result<Healthcheck> {
        s3_common::config::build_healthcheck(self.bucket.clone(), client)
    }

    pub async fn create_service(&self, _proxy: &ProxyConfig) -> vector::Result<S3Service> {
        let region_str = self
            .region
            .region()
            .map(|r| r.to_string())
            .unwrap_or_else(|| "cn-hangzhou".to_string());

        let provider = AliyunOidcCredentialsProvider::from_rrsa_env(
            self.auth.role_arn.as_deref(),
            Some(region_str.clone()),
            "vector-alicloud-oss",
        )?;

        let mut endpoint = self.region.endpoint();
        if let Some(ref ep) = endpoint {
            if !ep.starts_with("http://") && !ep.starts_with("https://") {
                let scheme = if ep.contains("-internal") {
                    "http"
                } else {
                    "https"
                };
                endpoint = Some(format!("{scheme}://{ep}"));
            }
        }

        let mut config_builder = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .credentials_provider(SharedCredentialsProvider::new(provider))
            .region(Region::new(region_str))
            .force_path_style(self.force_path_style);

        if let Some(endpoint) = endpoint {
            config_builder = config_builder.endpoint_url(endpoint);
        }

        let client = S3Client::from_conf(config_builder.build());
        Ok(S3Service::new(client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<AlicloudOssSinkConfig>();
    }
}
