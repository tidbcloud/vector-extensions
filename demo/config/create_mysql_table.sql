-- 创建用于存储 slowlogs 的 MySQL 表
-- 使用前请根据实际需求调整表结构

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS slowlogs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    log_line TEXT NOT NULL,
    log_timestamp DATETIME,
    task_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_task_id (task_id),
    INDEX idx_timestamp (log_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
