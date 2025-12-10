use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

// Import the necessary types from the library
use vector_extensions::sources::system_tables::data_collector::DataCollector;
use vector_extensions::CollectorConfig;
use vector_extensions::CoprocessorCollector;
use vector_extensions::TableConfig;

// Import MySQL client
use sqlx::{Column, MySqlPool, Row};

#[tokio::test]
async fn test_mysql_coprocessor_one_to_one_comparison() {
    println!("🚀 开始 MySQL 与 CoprocessorCollector 一对一数据对比测试");

    // 测试配置
    let config = ComparisonTestConfig {
        host: "127.0.0.1".to_string(),
        mysql_port: 4000,
        username: "root".to_string(),
        password: "".to_string(),
        database: "information_schema".to_string(),
    };

    let schema = "information_schema";
    let table = "cluster_statements_summary";

    println!("📋 测试表: {}.{}", schema, table);

    // 1. 先通过 CoprocessorCollector 获取数据
    println!("🔍 步骤 1: 通过 CoprocessorCollector 获取数据...");
    let coprocessor_data = match collect_data_via_coprocessor(&config, schema, table).await {
        Ok(data) => {
            println!("✅ CoprocessorCollector 数据获取成功: {} 行", data.len());
            data
        }
        Err(e) => {
            println!("❌ CoprocessorCollector 数据获取失败: {}", e);
            return;
        }
    };

    if coprocessor_data.is_empty() {
        println!("⚠️ CoprocessorCollector 返回空数据，跳过测试");
        return;
    }

    // 2. 再通过 Rust MySQL 客户端获取数据
    println!("🔍 步骤 2: 通过 Rust MySQL 客户端获取数据...");
    let mysql_data = match collect_data_via_rust_mysql(&config, schema, table).await {
        Ok(data) => {
            println!("✅ MySQL 查询成功: {} 行", data.len());
            data
        }
        Err(e) => {
            println!("❌ MySQL 查询失败: {}", e);
            return;
        }
    };

    if mysql_data.is_empty() {
        println!("⚠️ MySQL 查询返回空数据，跳过测试");
        return;
    }

    // 3. 标准化 timestamp 字段
    println!("🔍 步骤 3: 标准化 timestamp 字段...");
    let normalized_mysql_data = normalize_timestamp_fields(&mysql_data);
    let normalized_coprocessor_data = normalize_timestamp_fields(&coprocessor_data);

    // 4. 按 digest 分组对比数据
    println!("🔍 步骤 4: 按 digest 分组对比数据...");
    let comparison_result =
        compare_data_by_digest(&normalized_mysql_data, &normalized_coprocessor_data);

    // 4. 输出对比结果
    println!("📊 对比结果:");
    println!(
        "  总行数对比: CoprocessorCollector={}, MySQL={}",
        coprocessor_data.len(),
        mysql_data.len()
    );
    println!("  完全匹配的行数: {}", comparison_result.exact_matches);
    println!("  部分匹配的行数: {}", comparison_result.partial_matches);
    println!("  完全不匹配的行数: {}", comparison_result.no_matches);
    println!(
        "  总字段差异数: {}",
        comparison_result.total_field_differences
    );

    // 5. 详细差异报告
    if !comparison_result.field_differences.is_empty() {
        println!("📊 字段差异详情:");
        let mut sorted_fields: Vec<_> = comparison_result.field_differences.iter().collect();
        sorted_fields.sort_by(|a, b| b.1.cmp(a.1));

        for (field_name, count) in sorted_fields {
            println!("  {}: {} 次差异", field_name, count);
        }
    }

    // 6. 逐行对比详情
    println!("📊 逐行对比详情:");
    for (row_index, row_result) in comparison_result.row_results.iter().enumerate() {
        println!("  第 {} 行:", row_index + 1);
        println!("    匹配状态: {:?}", row_result.match_status);
        println!("    字段差异数: {}", row_result.field_differences.len());

        if !row_result.field_differences.is_empty() {
            println!("    差异字段:");
            for diff in &row_result.field_differences {
                println!(
                    "      {}: CoprocessorCollector={:?}, MySQL={:?}",
                    diff.field_name, diff.coprocessor_value, diff.mysql_value
                );
            }
        }
        println!("");
    }

    // 7. 输出 CSV 格式的字段对比
    println!("🔍 步骤 5: 生成 CSV 格式的字段对比...");
    output_csv_comparison_by_digest(
        &normalized_coprocessor_data,
        &normalized_mysql_data,
        &comparison_result,
    );

    // 8. 总结
    if comparison_result.exact_matches == mysql_data.len() {
        println!("🎉 所有数据完全匹配！");
    } else {
        println!("⚠️ 发现数据差异，需要进一步分析");
    }
}

// 对比结果结构
#[derive(Debug)]
struct ComparisonResult {
    exact_matches: usize,
    partial_matches: usize,
    no_matches: usize,
    total_field_differences: usize,
    field_differences: HashMap<String, usize>,
    row_results: Vec<RowComparisonResult>,
}

#[derive(Debug)]
struct RowComparisonResult {
    match_status: MatchStatus,
    field_differences: Vec<FieldDifference>,
}

#[derive(Debug)]
enum MatchStatus {
    ExactMatch,
    PartialMatch,
    NoMatch,
}

#[derive(Debug)]
struct FieldDifference {
    field_name: String,
    mysql_value: Option<String>,
    coprocessor_value: Option<String>,
}

// 标准化 timestamp 字段
fn normalize_timestamp_fields(
    data: &[HashMap<String, serde_json::Value>],
) -> Vec<HashMap<String, serde_json::Value>> {
    let timestamp_fields = [
        "SUMMARY_BEGIN_TIME",
        "SUMMARY_END_TIME",
        "FIRST_SEEN",
        "LAST_SEEN",
    ];

    data.iter()
        .map(|row| {
            let mut normalized_row = row.clone();

            for field_name in &timestamp_fields {
                if let Some(value) = normalized_row.get(*field_name) {
                    if let Some(timestamp_str) = value.as_str() {
                        // 尝试解析不同的时间格式
                        if let Some(normalized_timestamp) =
                            parse_timestamp_to_microseconds(timestamp_str)
                        {
                            // 保存微秒时间戳用于对比
                            normalized_row.insert(
                                field_name.to_string(),
                                serde_json::Value::String(normalized_timestamp.clone()),
                            );

                            // 额外保存本地时间字符串用于查看
                            let local_time_str = format!("{}_LOCAL", field_name);
                            if let Some(local_time) =
                                microseconds_to_local_time_string(&normalized_timestamp)
                            {
                                normalized_row
                                    .insert(local_time_str, serde_json::Value::String(local_time));
                            }
                        }
                    }
                }
            }

            normalized_row
        })
        .collect()
}

// 解析时间戳为微秒格式
fn parse_timestamp_to_microseconds(timestamp_str: &str) -> Option<String> {
    // 如果已经是微秒时间戳格式 (纯数字)
    if timestamp_str.chars().all(|c| c.is_ascii_digit()) {
        return Some(timestamp_str.to_string());
    }

    // 尝试解析 MySQL 时间格式: "2025-10-24 09:30:00"
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
        // 转换为 UTC 时间戳 (微秒)
        let utc_dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
        let microseconds = utc_dt.timestamp_micros();
        return Some(microseconds.to_string());
    }

    // 尝试解析其他时间格式
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(timestamp_str) {
        let microseconds = dt.timestamp_micros();
        return Some(microseconds.to_string());
    }

    // 如果无法解析，返回原值
    Some(timestamp_str.to_string())
}

// 将微秒时间戳转换为本地时间字符串
fn microseconds_to_local_time_string(microseconds_str: &str) -> Option<String> {
    if let Ok(microseconds) = microseconds_str.parse::<i64>() {
        // 转换为 UTC 时间
        let utc_dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(microseconds)?;

        // 转换为本地时间 (使用系统时区)
        let local_dt = utc_dt.with_timezone(&chrono::Local);

        // 格式化为本地时间字符串
        Some(local_dt.format("%Y-%m-%d %H:%M:%S %Z").to_string())
    } else {
        None
    }
}

// 按 digest 分组输出 CSV 格式的字段对比到文件（宽表格式）
fn output_csv_comparison_by_digest(
    coprocessor_data: &[HashMap<String, serde_json::Value>],
    mysql_data: &[HashMap<String, serde_json::Value>],
    comparison_result: &ComparisonResult,
) {
    use std::fs::File;
    use std::io::Write;

    let filename = "mysql_coprocessor_comparison_by_digest.csv";

    match File::create(filename) {
        Ok(mut file) => {
            // 获取所有字段名并按字母顺序排序
            let mut all_fields = std::collections::HashSet::new();
            for row in coprocessor_data.iter().chain(mysql_data.iter()) {
                all_fields.extend(row.keys());
            }
            let mut sorted_fields: Vec<_> = all_fields.into_iter().collect();
            sorted_fields.sort();

            // 按 digest 分组数据
            let coprocessor_by_digest = group_data_by_digest(coprocessor_data);
            let mysql_by_digest = group_data_by_digest(mysql_data);

            // 写入 CSV 头部
            write!(file, "Source,Digest").unwrap();
            for field_name in &sorted_fields {
                write!(file, ",{}", field_name).unwrap();
            }
            writeln!(file).unwrap();

            // 按 digest 输出所有行
            for digest in coprocessor_by_digest.keys() {
                let coprocessor_rows = coprocessor_by_digest.get(digest).unwrap();
                let mysql_rows = mysql_by_digest
                    .get(digest)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                // 输出 CoprocessorCollector 数据
                for coprocessor_row in coprocessor_rows.iter() {
                    write!(file, "CoprocessorCollector").unwrap();
                    write!(file, ",{}", digest).unwrap();
                    for field_name in &sorted_fields {
                        let value = coprocessor_row
                            .get(field_name.as_str())
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "NULL".to_string());
                        let escaped = escape_csv_value(&value);
                        write!(file, ",{}", escaped).unwrap();
                    }
                    writeln!(file).unwrap();
                }

                // 输出 MySQL 数据
                for mysql_row in mysql_rows.iter() {
                    write!(file, "MySQL").unwrap();
                    write!(file, ",{}", digest).unwrap();
                    for field_name in &sorted_fields {
                        let value = mysql_row
                            .get(field_name.as_str())
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "NULL".to_string());
                        let escaped = escape_csv_value(&value);
                        write!(file, ",{}", escaped).unwrap();
                    }
                    writeln!(file).unwrap();
                }
            }

            // 输出 MySQL 独有的 digest
            for digest in mysql_by_digest.keys() {
                if !coprocessor_by_digest.contains_key(digest) {
                    let mysql_rows = mysql_by_digest.get(digest).unwrap();
                    for mysql_row in mysql_rows.iter() {
                        write!(file, "MySQL").unwrap();
                        write!(file, ",{}", digest).unwrap();
                        for field_name in &sorted_fields {
                            let value = mysql_row
                                .get(field_name.as_str())
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "NULL".to_string());
                            let escaped = escape_csv_value(&value);
                            write!(file, ",{}", escaped).unwrap();
                        }
                        writeln!(file).unwrap();
                    }
                }
            }

            println!("✅ CSV 文件已生成: {}", filename);
        }
        Err(e) => {
            println!("❌ 无法创建 CSV 文件: {}", e);
        }
    }
}

// 按 digest 分组数据
fn group_data_by_digest(
    data: &[HashMap<String, serde_json::Value>],
) -> std::collections::HashMap<String, Vec<HashMap<String, serde_json::Value>>> {
    let mut grouped = std::collections::HashMap::new();

    for row in data {
        if let Some(digest) = row.get("DIGEST") {
            if let Some(digest_str) = digest.as_str() {
                grouped
                    .entry(digest_str.to_string())
                    .or_insert_with(Vec::new)
                    .push(row.clone());
            }
        }
    }

    grouped
}

// 转义 CSV 值中的特殊字符
fn escape_csv_value(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        // 如果包含特殊字符，用双引号包围并转义内部的双引号
        format!("\"{}\"", value.replace("\"", "\"\""))
    } else {
        value.to_string()
    }
}

// 一对一数据对比函数
fn compare_data_one_to_one(
    mysql_data: &[HashMap<String, serde_json::Value>],
    coprocessor_data: &[HashMap<String, serde_json::Value>],
) -> ComparisonResult {
    let mut result = ComparisonResult {
        exact_matches: 0,
        partial_matches: 0,
        no_matches: 0,
        total_field_differences: 0,
        field_differences: HashMap::new(),
        row_results: Vec::new(),
    };

    // 获取所有字段名
    let mut all_fields = Vec::new();
    for row in mysql_data.iter().chain(coprocessor_data.iter()) {
        all_fields.extend(row.keys());
    }
    all_fields.sort();
    all_fields.dedup();

    // 逐行对比
    for (row_index, (mysql_row, coprocessor_row)) in
        mysql_data.iter().zip(coprocessor_data.iter()).enumerate()
    {
        let mut row_result = RowComparisonResult {
            match_status: MatchStatus::ExactMatch,
            field_differences: Vec::new(),
        };

        let mut field_differences_count = 0;

        // 对比每个字段
        for field_name in &all_fields {
            let mysql_value = mysql_row.get(*field_name).map(|v| format_value(v));
            let coprocessor_value = coprocessor_row.get(*field_name).map(|v| format_value(v));

            if mysql_value != coprocessor_value {
                field_differences_count += 1;
                result.total_field_differences += 1;

                row_result.field_differences.push(FieldDifference {
                    field_name: field_name.to_string(),
                    mysql_value,
                    coprocessor_value,
                });

                // 记录字段差异统计
                *result
                    .field_differences
                    .entry(field_name.to_string())
                    .or_insert(0) += 1;
            }
        }

        // 确定匹配状态
        if field_differences_count == 0 {
            row_result.match_status = MatchStatus::ExactMatch;
            result.exact_matches += 1;
        } else if field_differences_count < all_fields.len() {
            row_result.match_status = MatchStatus::PartialMatch;
            result.partial_matches += 1;
        } else {
            row_result.match_status = MatchStatus::NoMatch;
            result.no_matches += 1;
        }

        result.row_results.push(row_result);
    }

    result
}

// 格式化值的辅助函数
fn format_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => value.to_string(),
    }
}

// 通过 MySQL 客户端获取数据
async fn collect_data_via_mysql(
    config: &ComparisonTestConfig,
    schema: &str,
    table: &str,
) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error + Send + Sync>> {
    println!("🔍 通过 MySQL 客户端获取 {}.{} 的数据", schema, table);

    let query = format!("SELECT * FROM {}.{} LIMIT 3", schema, table);
    let mysql_cmd = format!(
        "mysql -h{} -P{} -u{} -p{} {} -e '{}' --batch --raw",
        config.host, config.mysql_port, config.username, config.password, config.database, query
    );

    let output = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(&mysql_cmd)
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !stderr.is_empty() {
        println!("MySQL stderr: {}", stderr);
    }

    if !output.status.success() {
        return Err(format!("MySQL command failed: {}", stderr).into());
    }

    // 解析 MySQL 输出 - 使用更智能的解析方法
    let stdout_str = stdout.trim();
    if stdout_str.is_empty() {
        return Ok(vec![]);
    }

    // 按制表符分割第一行获取列名
    let lines: Vec<&str> = stdout_str.split('\n').collect();
    if lines.is_empty() {
        return Ok(vec![]);
    }

    let headers: Vec<&str> = lines[0].split('\t').collect();
    let mut rows = Vec::new();

    // 重新组合数据行，处理包含换行符的字段
    let mut current_line = String::new();
    let mut in_data_section = false;

    for line in lines.iter().skip(1) {
        if !in_data_section {
            // 检查是否是数据行的开始（以 IP 地址开头）
            if line.starts_with("127.0.0.1:") {
                current_line = line.to_string();
                in_data_section = true;
            }
        } else {
            // 如果当前行以制表符开头，说明是上一行的继续
            if line.starts_with('\t') {
                current_line.push('\n');
                current_line.push_str(line);
            } else if line.starts_with("127.0.0.1:") {
                // 新的数据行开始，先处理上一行
                if !current_line.is_empty() {
                    if let Some(row) = parse_mysql_row(&current_line, &headers) {
                        rows.push(row);
                    }
                }
                current_line = line.to_string();
            } else {
                // 其他情况，可能是字段的继续
                current_line.push('\n');
                current_line.push_str(line);
            }
        }
    }

    // 处理最后一行
    if !current_line.is_empty() && in_data_section {
        if let Some(row) = parse_mysql_row(&current_line, &headers) {
            rows.push(row);
        }
    }

    // 如果上面的逻辑没有解析到任何行，使用简单的解析方法作为后备
    if rows.is_empty() {
        println!("⚠️ 使用简单解析方法作为后备");
        for line in lines.iter().skip(1) {
            if line.starts_with("127.0.0.1:") {
                if let Some(row) = parse_mysql_row(line, &headers) {
                    rows.push(row);
                }
            }
        }
    }

    println!("📊 MySQL 解析完成: {} 行数据", rows.len());
    Ok(rows)
}

// 通过 Rust MySQL 客户端获取数据
async fn collect_data_via_rust_mysql(
    config: &ComparisonTestConfig,
    schema: &str,
    table: &str,
) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error + Send + Sync>> {
    println!("🔍 通过 Rust MySQL 客户端获取 {}.{} 的数据", schema, table);

    // 创建数据库连接池
    let pool = MySqlPool::connect(&config.database_url()).await?;
    println!("✅ MySQL 连接池创建成功");

    // 执行查询，不使用 LIMIT
    let query = format!("SELECT * FROM `{}`.`{}`", schema, table);
    println!("🔍 执行查询: {}", query);

    let rows = sqlx::query(&query).fetch_all(&pool).await?;

    println!("📊 MySQL 查询完成: {} 行数据", rows.len());

    // 获取列名
    if rows.is_empty() {
        return Ok(vec![]);
    }

    let mut result = Vec::new();
    for row in rows {
        let mut row_data = HashMap::new();

        // 遍历所有列
        for (i, column) in row.columns().iter().enumerate() {
            let column_name = column.name();
            let value = match row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(i) {
                Ok(Some(dt)) => {
                    serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
                }
                Ok(None) => serde_json::Value::Null,
                Err(_) => {
                    // 尝试其他类型
                    if let Ok(s) = row.try_get::<String, _>(i) {
                        serde_json::Value::String(s)
                    } else if let Ok(Some(s)) = row.try_get::<Option<String>, _>(i) {
                        serde_json::Value::String(s)
                    } else if let Ok(None) = row.try_get::<Option<String>, _>(i) {
                        serde_json::Value::Null
                    } else if let Ok(Some(n)) = row.try_get::<Option<u64>, _>(i) {
                        serde_json::Value::Number(serde_json::Number::from(n))
                    } else if let Ok(Some(n)) = row.try_get::<Option<i64>, _>(i) {
                        serde_json::Value::Number(serde_json::Number::from(n))
                    } else if let Ok(Some(f)) = row.try_get::<Option<f64>, _>(i) {
                        serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap())
                    } else if let Ok(Some(dt)) = row.try_get::<Option<chrono::NaiveDateTime>, _>(i)
                    {
                        serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    } else if let Ok(Some(d)) = row.try_get::<Option<chrono::NaiveDate>, _>(i) {
                        serde_json::Value::String(d.format("%Y-%m-%d").to_string())
                    } else if let Ok(Some(t)) = row.try_get::<Option<chrono::NaiveTime>, _>(i) {
                        serde_json::Value::String(t.format("%H:%M:%S").to_string())
                    } else {
                        serde_json::Value::String("".to_string())
                    }
                }
            };
            row_data.insert(column_name.to_string(), value);
        }
        result.push(row_data);
    }

    println!("📊 MySQL 解析完成: {} 行数据", result.len());
    Ok(result)
}

// 解析 MySQL 行的辅助函数
fn parse_mysql_row(line: &str, headers: &[&str]) -> Option<HashMap<String, serde_json::Value>> {
    let values: Vec<&str> = line.split('\t').collect();
    if values.len() != headers.len() {
        return None;
    }

    let mut row = HashMap::new();
    for (i, header) in headers.iter().enumerate() {
        let value = if i < values.len() {
            let raw_value = values[i];
            if raw_value == "NULL" {
                serde_json::Value::Null
            } else if raw_value.is_empty() {
                serde_json::Value::String("".to_string())
            } else {
                // 尝试解析为数字
                if let Ok(num) = raw_value.parse::<i64>() {
                    serde_json::Value::Number(serde_json::Number::from(num))
                } else if let Ok(num) = raw_value.parse::<f64>() {
                    serde_json::Value::Number(serde_json::Number::from_f64(num).unwrap())
                } else {
                    serde_json::Value::String(raw_value.to_string())
                }
            }
        } else {
            serde_json::Value::Null
        };
        row.insert(header.to_string(), value);
    }
    Some(row)
}

// 按 digest 分组对比数据
fn compare_data_by_digest(
    mysql_data: &[HashMap<String, serde_json::Value>],
    coprocessor_data: &[HashMap<String, serde_json::Value>],
) -> ComparisonResult {
    println!("🔍 开始按 digest 分组对比数据...");

    // 按 digest 分组 MySQL 数据
    let mut mysql_by_digest: HashMap<String, Vec<&HashMap<String, serde_json::Value>>> =
        HashMap::new();
    for row in mysql_data {
        if let Some(digest) = row.get("DIGEST").and_then(|v| v.as_str()) {
            mysql_by_digest
                .entry(digest.to_string())
                .or_insert_with(Vec::new)
                .push(row);
        }
    }

    // 按 digest 分组 CoprocessorCollector 数据
    let mut coprocessor_by_digest: HashMap<String, Vec<&HashMap<String, serde_json::Value>>> =
        HashMap::new();
    for row in coprocessor_data {
        if let Some(digest) = row.get("DIGEST").and_then(|v| v.as_str()) {
            coprocessor_by_digest
                .entry(digest.to_string())
                .or_insert_with(Vec::new)
                .push(row);
        }
    }

    println!(
        "📊 CoprocessorCollector 数据按 digest 分组: {} 个不同的 digest",
        coprocessor_by_digest.len()
    );
    println!(
        "📊 MySQL 数据按 digest 分组: {} 个不同的 digest",
        mysql_by_digest.len()
    );

    let mut total_exact_matches = 0;
    let mut total_partial_matches = 0;
    let mut total_no_matches = 0;
    let mut total_field_differences = 0;
    let mut field_differences: HashMap<String, usize> = HashMap::new();
    let mut row_details = Vec::new();

    // 对比每个 digest
    for (digest, mysql_rows) in &mysql_by_digest {
        println!("🔍 对比 digest: {}", digest);

        if let Some(coprocessor_rows) = coprocessor_by_digest.get(digest) {
            println!(
                "  CoprocessorCollector 行数: {}, MySQL 行数: {}",
                coprocessor_rows.len(),
                mysql_rows.len()
            );

            // 对比相同 digest 的行
            for (i, mysql_row) in mysql_rows.iter().enumerate() {
                if i < coprocessor_rows.len() {
                    let coprocessor_row = coprocessor_rows[i];
                    let row_result = compare_single_row(mysql_row, coprocessor_row);

                    match row_result.match_status {
                        MatchStatus::ExactMatch => total_exact_matches += 1,
                        MatchStatus::PartialMatch => total_partial_matches += 1,
                        MatchStatus::NoMatch => total_no_matches += 1,
                    }

                    total_field_differences += row_result.field_differences.len();

                    for diff in &row_result.field_differences {
                        *field_differences
                            .entry(diff.field_name.clone())
                            .or_insert(0) += 1;
                    }

                    row_details.push(row_result);
                } else {
                    println!("  ⚠️ CoprocessorCollector 有额外行，digest: {}", digest);
                    total_no_matches += 1;
                }
            }

            if mysql_rows.len() > coprocessor_rows.len() {
                println!("  ⚠️ MySQL 有额外行，digest: {}", digest);
            }
        } else {
            println!("  ❌ CoprocessorCollector 中没有找到 digest: {}", digest);
            total_no_matches += mysql_rows.len();
        }
    }

    // 检查 MySQL 中独有的 digest
    for (digest, mysql_rows) in &mysql_by_digest {
        if !coprocessor_by_digest.contains_key(digest) {
            println!("  ❌ MySQL 中没有找到 digest: {}", digest);
            total_no_matches += mysql_rows.len();
        }
    }

    ComparisonResult {
        exact_matches: total_exact_matches,
        partial_matches: total_partial_matches,
        no_matches: total_no_matches,
        total_field_differences,
        field_differences,
        row_results: row_details,
    }
}

// 对比单行数据
fn compare_single_row(
    mysql_row: &HashMap<String, serde_json::Value>,
    coprocessor_row: &HashMap<String, serde_json::Value>,
) -> RowComparisonResult {
    let mut field_differences = Vec::new();
    let mut exact_match = true;
    let mut partial_match = false;

    // 获取所有字段名
    let mut all_fields = std::collections::HashSet::new();
    for field_name in mysql_row.keys() {
        all_fields.insert(field_name);
    }
    for field_name in coprocessor_row.keys() {
        all_fields.insert(field_name);
    }

    // 对比每个字段
    for field_name in &all_fields {
        let mysql_value = mysql_row.get(field_name.as_str());
        let coprocessor_value = coprocessor_row.get(field_name.as_str());

        if mysql_value != coprocessor_value {
            exact_match = false;
            partial_match = true;

            field_differences.push(FieldDifference {
                field_name: field_name.to_string(),
                coprocessor_value: coprocessor_value.cloned().map(|v| v.to_string()),
                mysql_value: mysql_value.cloned().map(|v| v.to_string()),
            });
        }
    }

    let match_status = if exact_match {
        MatchStatus::ExactMatch
    } else if partial_match {
        MatchStatus::PartialMatch
    } else {
        MatchStatus::NoMatch
    };

    RowComparisonResult {
        match_status,
        field_differences,
    }
}

// 通过 CoprocessorCollector 获取数据
async fn collect_data_via_coprocessor(
    config: &ComparisonTestConfig,
    schema: &str,
    table: &str,
) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "🔍 通过 CoprocessorCollector 获取 {}.{} 的数据",
        schema, table
    );

    let collector_config = CollectorConfig::for_coprocessor(
        "comparison_test_instance".to_string(),
        config.host.clone(),
        config.mysql_port,
        Some(60),
        Some(3),
        None,
    );

    let mut collector = CoprocessorCollector::new(collector_config)?;
    println!("🔍 初始化 CoprocessorCollector...");
    collector.initialize().await?;
    println!("✅ CoprocessorCollector 初始化完成");

    let table_config = TableConfig {
        source_schema: schema.to_string(),
        source_table: table.to_string(),
        dest_table: format!("comparison_test_{}", table),
        collection_interval: "short".to_string(),
        where_clause: None,
        enabled: true,
    };

    let result = timeout(
        Duration::from_secs(60),
        collector.collect_table_data(&table_config),
    )
    .await??;

    println!(
        "📊 CoprocessorCollector 收集完成: {} 行数据，耗时 {}ms",
        result.data.len(),
        result.metadata.duration_ms
    );

    Ok(result.data)
}

#[derive(Debug)]
struct ComparisonTestConfig {
    host: String,
    mysql_port: u16,
    username: String,
    password: String,
    database: String,
}

impl ComparisonTestConfig {
    fn database_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.mysql_port, self.database
        )
    }
}
