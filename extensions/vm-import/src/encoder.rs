use serde_json::Value;
use vector::event::Event;
use vector::sinks::util::http::HttpEventEncoder;
use vector::sinks::util::PartitionInnerBuffer;
use vector::template::Template;

use crate::partition::PartitionKey;

pub struct VMImportSinkEventEncoder {
    endpoint_template: Template,
}

impl VMImportSinkEventEncoder {
    pub fn new(endpoint_template: Template) -> Self {
        Self { endpoint_template }
    }
}

impl HttpEventEncoder<PartitionInnerBuffer<serde_json::Value, PartitionKey>>
    for VMImportSinkEventEncoder
{
    fn encode_event(
        &mut self,
        event: Event,
    ) -> Option<PartitionInnerBuffer<serde_json::Value, PartitionKey>> {
        let endpoint = self
            .endpoint_template
            .render_string(&event)
            .map_err(|error| {
                warn!(message = "Failed to render endpoint template.", %error);
            })
            .ok()?;
        let json = Self::encode_log(event)?;
        Some(PartitionInnerBuffer::new(json, PartitionKey::new(endpoint)))
    }
}

impl VMImportSinkEventEncoder {
    fn encode_log(event: Event) -> Option<serde_json::Value> {
        let mut log = event.try_into_log()?;
        let labels = log.remove("labels")?;
        let metric = Self::encode_metric(labels)?;

        let timestamps = log.remove("timestamps")?;
        let timestamps = Self::encode_timestamps(timestamps)?;

        let values = log.remove("values")?;
        let values = Self::encode_values(values)?;

        let mut target_map = serde_json::Map::with_capacity(3);
        target_map.insert("metric".to_owned(), metric);
        target_map.insert("timestamps".to_owned(), timestamps);
        target_map.insert("values".to_owned(), values);
        Some(Value::Object(target_map))
    }

    fn encode_metric(v: vector::event::Value) -> Option<Value> {
        let labels = v.into_object()?;
        let metric = labels
            .into_iter()
            .map(|(key, value)| {
                let value = String::from_utf8_lossy(value.as_bytes()?);
                let value = Value::String(value.to_string());
                Some((key, value))
            })
            .collect::<Option<_>>()?;
        Some(Value::Object(metric))
    }

    fn encode_timestamps(v: vector::event::Value) -> Option<Value> {
        let timestamps = v.as_array()?;
        let timestamps = timestamps
            .iter()
            .map(|t| {
                let ts = t.as_timestamp()?.timestamp_millis();
                let num = serde_json::Number::from(ts);
                Some(Value::Number(num))
            })
            .collect::<Option<_>>()?;
        Some(Value::Array(timestamps))
    }

    fn encode_values(v: vector::event::Value) -> Option<Value> {
        let values = v.as_array()?;
        let values = values
            .iter()
            .map(|value| {
                let value = value.as_float()?;
                let num = serde_json::Number::from_f64(*value)?;
                Some(Value::Number(num))
            })
            .collect::<Option<_>>()?;
        Some(Value::Array(values))
    }
}

#[cfg(test)]
mod tests {
    use topsql::parser::Buf;

    use super::*;

    #[test]
    fn topsql_event() {
        let event = Buf::default()
            .label_name("topsql_cpu_time_ms")
            .instance("db:10080")
            .instance_type("tidb")
            .sql_digest("DEAD")
            .plan_digest("BEEF")
            .points([(1661396787, 80.0), (1661396788, 443.0)].into_iter())
            .build_event()
            .unwrap();

        let value = VMImportSinkEventEncoder::encode_log(event.into()).unwrap();

        let expected = serde_json::json!({
            "metric": {
                "__name__": "topsql_cpu_time_ms",
                "instance": "db:10080",
                "instance_type": "tidb",
                "sql_digest": "DEAD",
                "plan_digest": "BEEF",
                "tag_label": "",
            },
            "timestamps": [1661396787000u64, 1661396788000u64],
            "values": [80.0, 443.0],
        });
        assert_eq!(value, expected);
    }

    #[test]
    fn partition_by_cluster_id() {
        use bytes::Bytes;
        use vector::event::Value;

        let routine = |tmp_str: &str| {
            let tmp = tmp_str.try_into().unwrap();
            let mut encoder = VMImportSinkEventEncoder::new(tmp);

            let mut event = Buf::default()
                .label_name("topsql_cpu_time_ms")
                .instance("db:10080")
                .instance_type("tidb")
                .sql_digest("DEAD")
                .plan_digest("BEEF")
                .points([(1661396787, 80.0), (1661396788, 443.0)].into_iter())
                .build_event()
                .unwrap();
            let labels = event.get_mut("labels").unwrap();
            labels
                .insert("cluster_id".to_owned(), Value::Bytes(Bytes::from("10086")))
                .unwrap();

            let value = encoder.encode_event(event.into()).unwrap();
            let (json, key) = value.into_parts();

            assert_eq!(key.endpoint, "http://localhost:8080/metrics/10086");

            let expected_json = serde_json::json!({
                "metric": {
                    "__name__": "topsql_cpu_time_ms",
                    "instance": "db:10080",
                    "instance_type": "tidb",
                    "sql_digest": "DEAD",
                    "plan_digest": "BEEF",
                    "tag_label": "",
                    "cluster_id": "10086",
                },
                "timestamps": [1661396787000u64, 1661396788000u64],
                "values": [80.0, 443.0],
            });
            assert_eq!(json, expected_json);
        };

        routine("http://localhost:8080/metrics/{{ .labels.cluster_id }}");
        routine("http://localhost:8080/metrics/{{ labels.cluster_id }}");
    }
}
