-- Create keyspace for benchmark
CREATE KEYSPACE IF NOT EXISTS benchmark
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE benchmark;

-- Conversations table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY,
    seller_id UUID,
    buyer_id UUID,
    platform TEXT,
    created_at TIMESTAMP,
    last_message_at TIMESTAMP,
    status TEXT,
    message_count INT
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
AND compression = {'sstable_compression': 'LZ4Compressor'};

-- Messages table partitioned by conversation_id and time bucket
CREATE TABLE IF NOT EXISTS messages (
    conversation_id UUID,
    time_bucket TEXT,  -- Format: YYYY-MM-DD-HH for hourly buckets
    timestamp TIMESTAMP,
    id UUID,
    sender_type TEXT,
    sender_id UUID,
    message_text TEXT,
    message_type TEXT,
    metadata TEXT,  -- JSON as text
    PRIMARY KEY ((conversation_id, time_bucket), timestamp, id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS', 'compaction_window_size': 24}
AND compression = {'sstable_compression': 'LZ4Compressor'};

-- Messages by timestamp for time-based queries
CREATE TABLE IF NOT EXISTS messages_by_time (
    time_bucket TEXT,  -- Format: YYYY-MM-DD-HH
    timestamp TIMESTAMP,
    conversation_id UUID,
    id UUID,
    sender_type TEXT,
    sender_id UUID,
    message_text TEXT,
    message_type TEXT,
    metadata TEXT,
    PRIMARY KEY (time_bucket, timestamp, conversation_id, id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS', 'compaction_window_size': 24}
AND compression = {'sstable_compression': 'LZ4Compressor'};

-- Sellers table
CREATE TABLE IF NOT EXISTS sellers (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    created_at TIMESTAMP,
    active BOOLEAN
);

-- Buyers table  
CREATE TABLE IF NOT EXISTS buyers (
    id UUID PRIMARY KEY,
    name TEXT,
    phone TEXT,
    platform_id TEXT,  -- WhatsApp/Instagram ID
    platform TEXT,
    created_at TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS ON conversations (seller_id);
CREATE INDEX IF NOT EXISTS ON conversations (buyer_id);
CREATE INDEX IF NOT EXISTS ON conversations (last_message_at);
CREATE INDEX IF NOT EXISTS ON messages (sender_id);
CREATE INDEX IF NOT EXISTS ON messages_by_time (conversation_id);
