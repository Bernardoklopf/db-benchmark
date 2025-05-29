-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create database objects
\c benchmark;

-- Sellers table
CREATE TABLE IF NOT EXISTS sellers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    active BOOLEAN DEFAULT true
);

-- Buyers table
CREATE TABLE IF NOT EXISTS buyers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    name VARCHAR(255),
    phone VARCHAR(50),
    platform_id VARCHAR(255) NOT NULL,
    platform VARCHAR(20) NOT NULL CHECK (
        platform IN ('whatsapp', 'instagram')
    ),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (platform_id, platform)
);

-- Conversations table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    seller_id UUID NOT NULL REFERENCES sellers (id),
    buyer_id UUID NOT NULL REFERENCES buyers (id),
    platform VARCHAR(20) NOT NULL CHECK (
        platform IN ('whatsapp', 'instagram')
    ),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_message_at TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'active' CHECK (
        status IN (
            'active',
            'closed',
            'archived'
        )
    ),
    message_count INTEGER DEFAULT 0,
    UNIQUE (seller_id, buyer_id, platform)
);

-- Messages table (will be converted to hypertable)
CREATE TABLE IF NOT EXISTS messages (
    id UUID DEFAULT gen_random_uuid (),
    conversation_id UUID NOT NULL REFERENCES conversations (id),
    sender_type VARCHAR(10) NOT NULL CHECK (
        sender_type IN ('seller', 'buyer')
    ),
    sender_id UUID NOT NULL,
    message_text TEXT,
    message_type VARCHAR(20) DEFAULT 'text' CHECK (
        message_type IN (
            'text',
            'image',
            'audio',
            'video',
            'document',
            'location'
        )
    ),
    metadata JSONB,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

-- Create hypertable for time-series data
SELECT create_hypertable (
        'messages', 'timestamp', chunk_time_interval => INTERVAL '1 day'
    );

-- Create indexes for optimal query performance
CREATE INDEX IF NOT EXISTS idx_conversations_seller_id ON conversations (seller_id);
CREATE INDEX IF NOT EXISTS idx_conversations_buyer_id ON conversations (buyer_id);
CREATE INDEX IF NOT EXISTS idx_conversations_platform ON conversations (platform);
CREATE INDEX IF NOT EXISTS idx_conversations_status ON conversations (status);
CREATE INDEX IF NOT EXISTS idx_conversations_last_message ON conversations (last_message_at DESC);

-- Messaging-specific indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_messages_conversation_time 
ON messages (conversation_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_messages_sender_time 
ON messages (sender_id, timestamp DESC) 
WHERE sender_type = 'seller';

CREATE INDEX IF NOT EXISTS idx_messages_conversation_sender 
ON messages (conversation_id, sender_type, timestamp DESC);

-- Covering index for common message queries
CREATE INDEX IF NOT EXISTS idx_messages_covering 
ON messages (conversation_id, timestamp DESC) 
INCLUDE (sender_type, message_text, message_type, metadata);

-- Materialized view for conversation summaries
CREATE MATERIALIZED VIEW IF NOT EXISTS conversation_summaries AS
SELECT 
    conversation_id,
    COUNT(*) as message_count,
    MAX(timestamp) as last_message_at,
    MIN(timestamp) as first_message_at,
    COUNT(CASE WHEN sender_type = 'seller' THEN 1 END) as seller_messages,
    COUNT(CASE WHEN sender_type = 'buyer' THEN 1 END) as buyer_messages,
    AVG(LENGTH(message_text)) as avg_message_length
FROM messages
GROUP BY conversation_id;

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_conversation_summaries_last_message 
ON conversation_summaries (last_message_at DESC);

-- Refresh policy for materialized view (refresh every hour)
SELECT add_continuous_aggregate_policy('conversation_summaries',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Compression policy for older data (compress data older than 7 days)
ALTER TABLE messages SET(
    timescaledb.compress,
    timescaledb.compress_segmentby = 'conversation_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy ('messages', INTERVAL '7 days');

-- Retention policy (drop data older than 1 year)
SELECT add_retention_policy ('messages', INTERVAL '1 year');

-- Continuous aggregates for real-time analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_message_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket ('1 hour', timestamp) AS hour,
    conversation_id,
    sender_type,
    message_type,
    COUNT(*) as message_count,
    AVG(LENGTH(message_text)) as avg_message_length,
    MAX(timestamp) as last_message_time
FROM messages
GROUP BY
    hour,
    conversation_id,
    sender_type,
    message_type
WITH
    NO DATA;

-- Refresh policy for continuous aggregate
SELECT
    add_continuous_aggregate_policy (
        'hourly_message_stats',
        start_offset => INTERVAL '3 hours',
        end_offset => INTERVAL '1 hour',
        schedule_interval => INTERVAL '1 hour'
    );

-- Daily conversation statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_conversation_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket ('1 day', timestamp) AS day,
    conversation_id,
    COUNT(*) as message_count,
    COUNT(DISTINCT sender_id) as unique_senders,
    MAX(timestamp) as last_activity
FROM messages
GROUP BY
    day,
    conversation_id
WITH
    NO DATA;

SELECT
    add_continuous_aggregate_policy (
        'daily_conversation_stats',
        start_offset => INTERVAL '3 days',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day'
    );

-- Function to update conversation last_message_at
CREATE OR REPLACE FUNCTION update_conversation_last_message()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE conversations 
    SET 
        last_message_at = NEW.timestamp,
        message_count = message_count + 1
    WHERE id = NEW.conversation_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update conversation metadata
CREATE TRIGGER trigger_update_conversation_last_message
    AFTER INSERT ON messages
    FOR EACH ROW
    EXECUTE FUNCTION update_conversation_last_message();

-- Create view for inactive conversations (no messages in last 5 minutes)
CREATE OR REPLACE VIEW inactive_conversations AS
SELECT
    c.*,
    EXTRACT(
        EPOCH
        FROM (NOW() - c.last_message_at)
    ) / 60 as minutes_since_last_message
FROM conversations c
WHERE
    c.last_message_at < NOW() - INTERVAL '5 minutes'
    AND c.status = 'active';

-- Create view for conversation with full context
CREATE OR REPLACE VIEW conversation_details AS
SELECT
    c.*,
    s.name as seller_name,
    s.email as seller_email,
    b.name as buyer_name,
    b.platform_id as buyer_platform_id
FROM
    conversations c
    JOIN sellers s ON c.seller_id = s.id
    JOIN buyers b ON c.buyer_id = b.id;