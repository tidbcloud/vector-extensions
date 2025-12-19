use arrow::datatypes::{DataType, Field, TimeUnit};
use deltalake::kernel::{DataType as DeltaDataType, StructField};
use vector_lib::event::Value as LogValue;

/// Type converter for MySQL, Arrow, and Delta Lake types
pub struct TypeConverter;

impl TypeConverter {
    pub fn new() -> Self {
        Self
    }

    /// Convert MySQL type string to Arrow DataType
    pub fn mysql_to_arrow(&self, mysql_type: &str) -> DataType {
        let mysql_type_lower = mysql_type.to_lowercase();

        if mysql_type_lower.contains("tinyint(1)") {
            DataType::Boolean
        } else if mysql_type_lower.contains("bigint") {
            if mysql_type_lower.contains("unsigned") {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        } else if mysql_type_lower.contains("tinyint") {
            DataType::Int8
        } else if mysql_type_lower.contains("smallint") {
            DataType::Int16
        } else if mysql_type_lower.contains("mediumint") || mysql_type_lower.contains("int") {
            if mysql_type_lower.contains("unsigned") {
                DataType::UInt32
            } else {
                DataType::Int32
            }
        } else if mysql_type_lower.contains("float") {
            DataType::Float32
        } else if mysql_type_lower.contains("double") || mysql_type_lower.contains("real") {
            DataType::Float64
        } else if mysql_type_lower.contains("decimal") || mysql_type_lower.contains("numeric") {
            // For decimal, we'll use Float64 as a reasonable approximation
            DataType::Float64
        } else if mysql_type_lower.contains("timestamp") {
            // MySQL TIMESTAMP -> Arrow Timestamp with timezone (UTC)
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string().into()))
        } else if mysql_type_lower.contains("datetime") {
            // MySQL DATETIME -> Arrow Timestamp without timezone (naive timestamp)
            DataType::Timestamp(TimeUnit::Microsecond, None)
        } else if mysql_type_lower.contains("date") {
            DataType::Date32
        } else if mysql_type_lower.contains("time") {
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        } else if mysql_type_lower.contains("longtext")
            || mysql_type_lower.contains("mediumtext")
            || mysql_type_lower.contains("text")
            || mysql_type_lower.contains("varchar")
            || mysql_type_lower.contains("char")
            || mysql_type_lower.contains("blob")
            || mysql_type_lower.contains("longblob")
            || mysql_type_lower.contains("mediumblob")
        {
            // Handle all text and blob types as Utf8
            DataType::Utf8
        } else {
            // Default to Utf8 for any unknown types
            DataType::Utf8
        }
    }

    /// Convert Arrow DataType to Delta DataType
    pub fn arrow_to_delta(&self, arrow_type: &DataType) -> DeltaDataType {
        match arrow_type {
            DataType::Boolean => DeltaDataType::BOOLEAN,
            DataType::Int8 => DeltaDataType::BYTE,
            DataType::Int16 => DeltaDataType::SHORT,
            DataType::Int32 => DeltaDataType::INTEGER,
            DataType::Int64 => DeltaDataType::LONG,
            DataType::UInt32 => DeltaDataType::INTEGER, // Delta Lake doesn't have unsigned types
            DataType::UInt64 => DeltaDataType::LONG,
            DataType::Float32 => DeltaDataType::FLOAT,
            DataType::Float64 => DeltaDataType::DOUBLE,
            DataType::Utf8 => DeltaDataType::STRING,
            DataType::LargeUtf8 => DeltaDataType::STRING,
            DataType::Binary => DeltaDataType::BINARY,
            DataType::LargeBinary => DeltaDataType::BINARY,
            DataType::Timestamp(_, _) => DeltaDataType::TIMESTAMP,
            DataType::Date32 => DeltaDataType::DATE,
            DataType::Date64 => DeltaDataType::DATE,
            _ => DeltaDataType::STRING, // Default fallback
        }
    }

    /// Convert Arrow Field to Delta StructField
    pub fn arrow_field_to_delta(&self, field: &Field) -> StructField {
        let delta_type = self.arrow_to_delta(field.data_type());
        StructField::new(field.name().clone(), delta_type, field.is_nullable())
    }

    /// Convert LogValue to Arrow DataType (value-based inference)
    pub fn infer_from_value(&self, value: &LogValue) -> DataType {
        match value {
            LogValue::Bytes(_) => DataType::Utf8,
            LogValue::Integer(_) => DataType::Int64,
            LogValue::Float(_) => DataType::Float64,
            LogValue::Boolean(_) => DataType::Boolean,
            LogValue::Null => DataType::Utf8, // Default for null values
            _ => DataType::Utf8,
        }
    }

    /// Infer Arrow data type from field name and value
    /// This function relies primarily on _schema_metadata for type inference
    pub fn infer_arrow_type(&self, _field_name: &str, value: &LogValue) -> DataType {
        // If we have a concrete value, use its type
        if !matches!(value, LogValue::Null) {
            return self.infer_from_value(value);
        }

        // For null values, we should rely on _schema_metadata
        // If no schema metadata is available, default to Utf8
        // This is a fallback that should rarely be used with proper schema metadata
        DataType::Utf8
    }
}

impl Default for TypeConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_to_arrow_int_types() {
        let converter = TypeConverter::new();

        assert_eq!(converter.mysql_to_arrow("bigint"), DataType::Int64);
        assert_eq!(
            converter.mysql_to_arrow("bigint unsigned"),
            DataType::UInt64
        );
        assert_eq!(converter.mysql_to_arrow("int"), DataType::Int32);
        assert_eq!(converter.mysql_to_arrow("int unsigned"), DataType::UInt32);
        assert_eq!(converter.mysql_to_arrow("tinyint"), DataType::Int8);
        assert_eq!(converter.mysql_to_arrow("tinyint(1)"), DataType::Boolean);
        assert_eq!(converter.mysql_to_arrow("smallint"), DataType::Int16);
    }

    #[test]
    fn test_mysql_to_arrow_float_types() {
        let converter = TypeConverter::new();

        assert_eq!(converter.mysql_to_arrow("float"), DataType::Float32);
        assert_eq!(converter.mysql_to_arrow("double"), DataType::Float64);
        assert_eq!(converter.mysql_to_arrow("decimal(10,2)"), DataType::Float64);
    }

    #[test]
    fn test_mysql_to_arrow_string_types() {
        let converter = TypeConverter::new();

        assert_eq!(converter.mysql_to_arrow("varchar(255)"), DataType::Utf8);
        assert_eq!(converter.mysql_to_arrow("text"), DataType::Utf8);
        assert_eq!(converter.mysql_to_arrow("longtext"), DataType::Utf8);
        assert_eq!(converter.mysql_to_arrow("char(10)"), DataType::Utf8);
    }

    #[test]
    fn test_mysql_to_arrow_timestamp_types() {
        let converter = TypeConverter::new();

        assert_eq!(
            converter.mysql_to_arrow("timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string().into()))
        );
        assert_eq!(
            converter.mysql_to_arrow("datetime"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(converter.mysql_to_arrow("date"), DataType::Date32);
    }

    #[test]
    fn test_arrow_to_delta_conversions() {
        let converter = TypeConverter::new();

        assert_eq!(
            converter.arrow_to_delta(&DataType::Int64),
            DeltaDataType::LONG
        );
        assert_eq!(
            converter.arrow_to_delta(&DataType::Int32),
            DeltaDataType::INTEGER
        );
        assert_eq!(
            converter.arrow_to_delta(&DataType::Float64),
            DeltaDataType::DOUBLE
        );
        assert_eq!(
            converter.arrow_to_delta(&DataType::Utf8),
            DeltaDataType::STRING
        );
        assert_eq!(
            converter.arrow_to_delta(&DataType::Boolean),
            DeltaDataType::BOOLEAN
        );
        assert_eq!(
            converter.arrow_to_delta(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            DeltaDataType::TIMESTAMP
        );
    }

    #[test]
    fn test_infer_from_value() {
        let converter = TypeConverter::new();

        assert_eq!(
            converter.infer_from_value(&LogValue::Integer(123)),
            DataType::Int64
        );
        assert_eq!(
            converter.infer_from_value(&LogValue::Float(
                ordered_float::NotNan::new(123.45).unwrap()
            )),
            DataType::Float64
        );
        assert_eq!(
            converter.infer_from_value(&LogValue::Boolean(true)),
            DataType::Boolean
        );
        assert_eq!(
            converter.infer_from_value(&LogValue::Bytes("test".into())),
            DataType::Utf8
        );
        assert_eq!(converter.infer_from_value(&LogValue::Null), DataType::Utf8);
    }
}
