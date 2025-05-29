-- CockroachDB initialization script for WhatsApp Database Benchmark
-- Creates database, user, and schema for benchmark testing

-- Create database
CREATE DATABASE IF NOT EXISTS benchmark;

-- Create user and grant privileges
CREATE USER IF NOT EXISTS benchmark_user;
GRANT ALL ON DATABASE benchmark TO benchmark_user;

-- Switch to benchmark database
USE benchmark;

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO benchmark_user;

-- Create sellers table
CREATE TABLE IF NOT EXISTS sellers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name STRING NOT NULL,
    email STRING UNIQUE NOT NULL,
    phone STRING,
    business_type STRING,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Create buyers table  
CREATE TABLE IF NOT EXISTS buyers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name STRING NOT NULL,
    email STRING UNIQUE NOT NULL,
    phone STRING,
    platform_id STRING,
    platform STRING,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Create conversations table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seller_id UUID NOT NULL REFERENCES sellers(id) ON DELETE CASCADE,
    buyer_id UUID NOT NULL REFERENCES buyers(id) ON DELETE CASCADE,
    platform STRING NOT NULL,
    conversation_id STRING NOT NULL,
    status STRING DEFAULT 'active',
    message_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    last_message_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(platform, conversation_id)
);

-- Create messages table (without partitioning for CockroachDB compatibility)
CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    sender_type STRING NOT NULL CHECK (sender_type IN ('seller', 'buyer')),
    sender_id UUID NOT NULL,
    message_type STRING NOT NULL,
    content STRING,
    metadata JSONB,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_sellers_email ON sellers(email);
CREATE INDEX IF NOT EXISTS idx_sellers_active ON sellers(active);
CREATE INDEX IF NOT EXISTS idx_buyers_email ON buyers(email);
CREATE INDEX IF NOT EXISTS idx_buyers_platform ON buyers(platform_id, platform);
CREATE INDEX IF NOT EXISTS idx_buyers_active ON buyers(active);
CREATE INDEX IF NOT EXISTS idx_conversations_seller ON conversations(seller_id);
CREATE INDEX IF NOT EXISTS idx_conversations_buyer ON conversations(buyer_id);
CREATE INDEX IF NOT EXISTS idx_conversations_platform ON conversations(platform, conversation_id);
CREATE INDEX IF NOT EXISTS idx_conversations_last_message ON conversations(last_message_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_type, sender_id);

-- Grant table privileges to benchmark user
GRANT ALL ON ALL TABLES IN SCHEMA public TO benchmark_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO benchmark_user;

-- Create views for analytics (CockroachDB supports views but not materialized views)
-- Hourly message stats view
CREATE VIEW IF NOT EXISTS hourly_message_stats AS
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as message_count,
    COUNT(DISTINCT conversation_id) as active_conversations,
    COUNT(DISTINCT CASE WHEN sender_type = 'seller' THEN sender_id END) as active_sellers,
    COUNT(DISTINCT CASE WHEN sender_type = 'buyer' THEN sender_id END) as active_buyers
FROM messages 
GROUP BY date_trunc('hour', timestamp);

-- Daily conversation stats view
CREATE VIEW IF NOT EXISTS daily_conversation_stats AS
SELECT 
    date_trunc('day', created_at) as day,
    platform,
    COUNT(*) as new_conversations,
    COUNT(DISTINCT seller_id) as active_sellers,
    COUNT(DISTINCT buyer_id) as active_buyers
FROM conversations 
GROUP BY date_trunc('day', created_at), platform;

-- Message type distribution view
CREATE VIEW IF NOT EXISTS message_type_stats AS
SELECT 
    message_type,
    sender_type,
    COUNT(*) as count,
    AVG(length(content)) as avg_content_length
FROM messages 
WHERE content IS NOT NULL
GROUP BY message_type, sender_type;

-- Grant view privileges
GRANT SELECT ON ALL TABLES IN SCHEMA public TO benchmark_user;

-- Show completion message
SELECT 'CockroachDB benchmark schema initialized successfully' as status;
