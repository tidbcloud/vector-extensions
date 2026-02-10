-- Create MySQL table for storing slowlogs
-- Table structure matches Delta Lake fields to enable direct mapping without transform
-- This allows tidb sink to automatically map Delta Lake fields to MySQL columns

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS slowlogs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    time BIGINT,           -- matches Delta Lake 'time' field (Unix timestamp)
    db VARCHAR(255),       -- matches Delta Lake 'db' field
    user VARCHAR(255),     -- matches Delta Lake 'user' field
    host VARCHAR(255),     -- matches Delta Lake 'host' field
    query_time FLOAT,      -- matches Delta Lake 'query_time' field
    result_rows INT,       -- matches Delta Lake 'result_rows' field
    prev_stmt TEXT,        -- matches Delta Lake 'prev_stmt' field
    digest VARCHAR(255),   -- matches Delta Lake 'digest' field
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_time (time),
    INDEX idx_db (db),
    INDEX idx_user (user)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
