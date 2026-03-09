-- Table for sync-logs-to-mysql: file_list line parsing + tidb sink write
-- Column names match event fields (tidb sink case-insensitive mapping)
-- Built-in: line_type, log_timestamp, logger, level, tag, message_body (Python) / client_ip, method, path, status (HTTP)
-- Custom regex: column names match (?P<name>...) capture groups

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS parsed_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- Raw line and type
    message TEXT,
    line_type VARCHAR(32),
    -- Built-in Python log
    log_timestamp VARCHAR(64),
    logger VARCHAR(255),
    level VARCHAR(32),
    tag VARCHAR(255),
    message_body TEXT,
    -- Built-in HTTP access
    client_ip VARCHAR(64),
    request_date VARCHAR(128),
    method VARCHAR(16),
    path VARCHAR(1024),
    protocol VARCHAR(32),
    status VARCHAR(16),
    response_size VARCHAR(32),
    -- File metadata
    file_path VARCHAR(1024),
    component VARCHAR(128),
    hour_partition VARCHAR(16),
    file_size BIGINT,
    last_modified VARCHAR(64),
    bucket VARCHAR(255),
    full_path VARCHAR(2048),
    -- Event time (Vector field @timestamp; backtick for MySQL reserved word)
    `@timestamp` VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_line_type (line_type),
    INDEX idx_level (level),
    INDEX idx_component (component),
    INDEX idx_hour (hour_partition),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
