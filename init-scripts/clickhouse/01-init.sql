-- Create database
CREATE DATABASE IF NOT EXISTS benchmark;

USE benchmark;

-- Sellers table
CREATE TABLE IF NOT EXISTS sellers (
    id UUID,
    name String,
    email String,
    phone String,
    created_at DateTime64 (3),
    active UInt8
) ENGINE = MergeTree ()
ORDER BY id SETTINGS index_granularity = 8192;

-- Buyers table
CREATE TABLE IF NOT EXISTS buyers (
    id UUID,
    name String,
    phone String,
    platform_id String,
    platform LowCardinality (String),
    created_at DateTime64 (3)
) ENGINE = MergeTree ()
ORDER BY id SETTINGS index_granularity = 8192;

-- Conversations table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID,
    seller_id UUID,
    buyer_id UUID,
    platform LowCardinality (String),
    created_at DateTime64 (3),
    last_message_at DateTime64 (3),
    status LowCardinality (String),
    message_count UInt32
) ENGINE = MergeTree ()
ORDER BY (seller_id, created_at)
PARTITION BY
    toYYYYMM (created_at) SETTINGS index_granularity = 8192;

-- Messages table - optimized for time-series analytics
CREATE TABLE IF NOT EXISTS messages (
    id UUID,
    conversation_id UUID,
    sender_type LowCardinality (String),
    sender_id UUID,
    message_text String,
    message_type LowCardinality (String),
    metadata String,
    timestamp DateTime64 (3)
) ENGINE = MergeTree ()
ORDER BY (conversation_id, timestamp)
PARTITION BY
    toYYYYMM (timestamp) SETTINGS index_granularity = 8192;

-- Messages by time - for time-based analytics
CREATE TABLE IF NOT EXISTS messages_by_time (
    timestamp DateTime64 (3),
    conversation_id UUID,
    id UUID,
    sender_type LowCardinality (String),
    sender_id UUID,
    message_text String,
    message_type LowCardinality (String),
    metadata String
) ENGINE = MergeTree ()
ORDER BY (timestamp, conversation_id)
PARTITION BY
    toYYYYMMDD (timestamp) SETTINGS index_granularity = 8192;

-- Materialized view for real-time conversation updates
CREATE MATERIALIZED VIEW IF NOT EXISTS conversation_stats 
ENGINE = SummingMergeTree()
ORDER BY (conversation_id, date) AS
SELECT
    conversation_id,
    toDate(timestamp) as date,
    count() as message_count,
    max(timestamp) as last_message_at,
    uniqExact(sender_id) as unique_senders
FROM messages
GROUP BY
    conversation_id,
    toDate(timestamp);

-- Materialized view for hourly message statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_message_stats 
ENGINE = SummingMergeTree()
ORDER BY (hour, sender_type, message_type) AS
SELECT
    toStartOfHour(timestamp) as hour,
    sender_type,
    message_type,
    count() as message_count,
    avg(length(message_text)) as avg_message_length
FROM messages m
LEFT JOIN conversations c ON m.conversation_id = c.id
GROUP BY
    hour,
    sender_type,
    message_type;

-- Materialized view for recent messages per conversation (last 30 days)
CREATE MATERIALIZED VIEW IF NOT EXISTS recent_messages_by_conversation
ENGINE = ReplacingMergeTree()
ORDER BY (conversation_id, timestamp)
AS SELECT 
    conversation_id,
    timestamp,
    id,
    sender_type,
    sender_id,
    message_text,
    message_type,
    metadata
FROM messages
WHERE timestamp >= now() - INTERVAL 30 DAY;

-- Create indexes for faster queries
ALTER TABLE conversations
ADD INDEX idx_last_message_at last_message_at TYPE minmax GRANULARITY 1;

ALTER TABLE conversations
ADD INDEX idx_seller_id seller_id TYPE bloom_filter GRANULARITY 1;

ALTER TABLE messages
ADD INDEX idx_sender_id sender_id TYPE bloom_filter GRANULARITY 1;

ALTER TABLE messages
ADD INDEX idx_message_type message_type TYPE set(100) GRANULARITY 1;