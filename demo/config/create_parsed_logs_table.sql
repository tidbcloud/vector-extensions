-- 供 sync-logs-to-mysql 使用的表：file_list 按行解析后 tidb sink 写入
-- 列名与事件字段一致（tidb sink 按列名做 case-insensitive 映射）
-- 内置解析：line_type, log_timestamp, logger, level, tag, message_body（Python）/ client_ip, method, path, status 等（HTTP）
-- 自定义正则：列名与 (?P<name>...) 中的 name 一致

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS parsed_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- 原始行与类型
    message TEXT,
    line_type VARCHAR(32),
    -- 内置 Python 日志
    log_timestamp VARCHAR(64),
    logger VARCHAR(255),
    level VARCHAR(32),
    tag VARCHAR(255),
    message_body TEXT,
    -- 内置 HTTP access
    client_ip VARCHAR(64),
    request_date VARCHAR(128),
    method VARCHAR(16),
    path VARCHAR(1024),
    protocol VARCHAR(32),
    status VARCHAR(16),
    response_size VARCHAR(32),
    -- 文件元数据
    file_path VARCHAR(1024),
    component VARCHAR(128),
    hour_partition VARCHAR(16),
    file_size BIGINT,
    last_modified VARCHAR(64),
    bucket VARCHAR(255),
    full_path VARCHAR(2048),
    -- 事件时间（Vector 字段名为 @timestamp，MySQL 用反引号）
    `@timestamp` VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_line_type (line_type),
    INDEX idx_level (level),
    INDEX idx_component (component),
    INDEX idx_hour (hour_partition),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
