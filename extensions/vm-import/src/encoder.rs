use serde_json::Value;
use vector::event::Event;
use vector::sinks::util::http::HttpEventEncoder;

pub struct VMImportSinkEventEncoder;

impl HttpEventEncoder<Value> for VMImportSinkEventEncoder {
    fn encode_event(&mut self, event: Event) -> Option<Value> {
        event.try_into_log().and_then(|mut log| {
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
        })
    }
}

impl VMImportSinkEventEncoder {
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
    use super::*;

    use topsql::parser::Buf;

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

        let mut encoder = VMImportSinkEventEncoder;
        let value = encoder.encode_event(event.into()).unwrap();

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
}
