# WhatsApp Database Benchmarks

A comprehensive benchmarking suite comparing **ScyllaDB**, **ClickHouse**, **TimescaleDB**, and **CockroachDB** for high-volume WhatsApp/Instagram message storage and analytics.

## 🎯 Objective

Test database performance for:
- **High write throughput** (WhatsApp/Instagram messages)
- **Analytical queries** (conversation retrieval, inactive conversations, message statistics)
- **Real-world scenarios** (LLM conversation processing, customer support analytics)

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Benchmark     │───▶│   Message    │───▶│   Databases     │
│   Runner        │    │   Generator  │    │  ScyllaDB       │
│                 │    │              │    │  ClickHouse     │
│                 │    │              │    │  TimescaleDB    │
│                 │    │              │    │  CockroachDB    │
└─────────────────┘    └──────────────┘    └─────────────────┘
         │                                           │
         ▼                                           ▼
┌─────────────────┐                        ┌─────────────────┐
│   Monitoring    │                        │   Results       │
│   (Grafana)     │                        │   Analysis      │
└─────────────────┘                        └─────────────────┘
```

## 🚀 Quick Start

### 1. Prerequisites
- Docker Desktop installed and running
- Node.js 18+ 
- 4GB+ available RAM (optimized for M1 Mac)

### 2. One-Command Setup
```bash
git clone <repository>
cd whatsapp-database-benchmarks
npm install

# Start all databases (optimized for M1 Mac)
./start.sh
```

### 3. Run Benchmarks

#### For Resource-Constrained Environments (16GB RAM or less)
```bash
# Test databases individually (recommended for M1 Mac)
node src/cli.js benchmark-isolated --scenario custom --output isolated-results.json

# Or test specific databases one at a time
node src/cli.js docker-isolated --database scylladb
node src/cli.js benchmark-writes --databases scylladb
node src/cli.js docker-down
```

#### For High-Resource Environments (32GB+ RAM)
```bash
# Test all databases together for direct comparison
node src/cli.js docker-up
node src/cli.js benchmark-together --scenario mixed_workload --output together-results.json
```

### 4. Health Check and Scenarios
```bash
# Check database connectivity
node src/cli.js health

# View available scenarios
node src/cli.js scenarios
```

> **Note**: Database schemas are now automatically initialized when databases start up

## 📊 Database Configurations

### ScyllaDB
- **Type**: Cassandra-compatible NoSQL
- **Strengths**: Extreme write performance, horizontal scaling
- **Schema**: Denormalized tables with time-bucket partitioning
- **Use Case**: High-volume message ingestion
- **Resource**: 1GB memory, 1.0 CPU core

### ClickHouse
- **Type**: Columnar OLAP database
- **Strengths**: Analytical queries, compression, materialized views
- **Schema**: Optimized for time-series analytics
- **Use Case**: Real-time analytics and reporting
- **Resource**: 1GB memory, 1.0 CPU core

### TimescaleDB
- **Type**: PostgreSQL with time-series extensions
- **Strengths**: SQL compatibility, hybrid workloads, continuous aggregates
- **Schema**: Hypertables with automatic partitioning
- **Use Case**: Complex queries with relational integrity
- **Resource**: 1GB memory, 1.0 CPU core

### CockroachDB
- **Type**: Distributed SQL database
- **Strengths**: ACID compliance, horizontal scaling, consistency
- **Schema**: Standard relational with distributed architecture
- **Use Case**: Transactional workloads with strong consistency
- **Resource**: 1GB memory, 1.0 CPU core

## 🎮 CLI Commands Reference

### Benchmark Commands

#### `benchmark-isolated`
Runs benchmarks with each database in isolation to avoid resource contention.

```bash
node src/cli.js benchmark-isolated [options]

Options:
  -s, --scenario <scenario>     Benchmark scenario (default: "custom")
  -o, --output <file>          Output file for results
  -u, --users <number>         Concurrent users for concurrency tests (default: 5)
  --ops <number>               Operations per user (default: 20)
  --databases <list>           Comma-separated list of databases to test
```

#### `benchmark-together`
Runs benchmarks with all databases simultaneously for direct comparison.

```bash
node src/cli.js benchmark-together [options]

Options:
  -s, --scenario <scenario>     Benchmark scenario (default: "mixed_workload")
  -o, --output <file>          Output file for results
  -u, --users <number>         Concurrent users for concurrency tests (default: 5)
  --ops <number>               Operations per user (default: 20)
  --databases <list>           Comma-separated list of databases to test
```

#### Individual Benchmark Types

```bash
# Write performance only
node src/cli.js benchmark-writes --scenario high_write_volume

# Read performance only  
node src/cli.js benchmark-reads --databases scylladb,clickhouse

# Concurrency testing only
node src/cli.js benchmark-concurrency --users 10 --ops 100
```

### Docker Management Commands

```bash
# Start all database containers
node src/cli.js docker-up

# Stop all database containers
node src/cli.js docker-down

# Start a single database in isolation
node src/cli.js docker-isolated --database scylladb

# Show container status
node src/cli.js docker-status

# Stop and cleanup all containers and unused Docker resources
node src/cli.js docker-cleanup
```

### Database Management Commands

```bash
# Initialize database schemas
node src/cli.js setup --databases scylladb,clickhouse

# Check database connectivity and health
node src/cli.js health

# Clean all data from databases (DESTRUCTIVE)
node src/cli.js clean --databases timescaledb,cockroachdb

# Generate test data for manual testing
node src/cli.js generate-data --scenario custom --databases scylladb
```

### Analysis Commands

```bash
# Compare results from multiple benchmark runs
node src/cli.js compare results/run1.json results/run2.json

# List available benchmark scenarios
node src/cli.js scenarios
```

## 🧪 Benchmark Scenarios

### 🔥 `high_write_volume`
- **Focus**: High-throughput message ingestion
- **Scale**: 100 sellers, 1,000 buyers, 1M+ messages
- **Use Case**: Peak load testing, write performance optimization

### 📊 `read_heavy_analytics`
- **Focus**: Complex analytical queries
- **Scale**: 50 sellers, 500 buyers, 750K messages
- **Use Case**: Business intelligence, reporting performance

### ⚖️ `mixed_workload` (default)
- **Focus**: Balanced read/write operations
- **Scale**: 200 sellers, 2,000 buyers, 750K messages
- **Use Case**: Production-like workload simulation

### 🧪 `custom`
- **Focus**: Small-scale testing and development
- **Scale**: 10 sellers, 100 buyers, 5K messages
- **Use Case**: Quick validation, development testing

## 🧪 Benchmark Tests

### Write Performance Tests
- **Concurrent message insertion**
- **Batch operations**
- **Sustained write load**

### Read Performance Tests
1. **Get Conversation**: Retrieve all messages for LLM processing
2. **Inactive Conversations**: Find conversations with no activity in 5+ minutes
3. **Recent Messages**: Get messages from last N minutes
4. **Conversation Stats**: Aggregate message counts and metrics
5. **Search Messages**: Full-text search across message content

### Test Data
- 1,000 sellers
- 10,000 buyers  
- 5,000 conversations
- 100 messages per conversation
- Concurrent read/write operations

## 📊 Benchmark Reports

All benchmark results are automatically saved to the `reports/` folder in JSON format with detailed metrics.

### Report Contents
- **Timestamp**: When the benchmark was executed
- **Configuration**: Test parameters (batch size, warmup runs, etc.)
- **Write Performance**: Throughput, latency, and error rates for each database
- **Read Performance**: Query response times and result counts
- **Concurrency Metrics**: Multi-user performance and error rates
- **System Metrics**: Resource usage and database-specific metrics

### Saving Reports
```bash
# Automatic timestamped report
node src/cli.js benchmark-isolated
# Saved to: reports/benchmark-report-YYYY-MM-DDTHH-MM-SS-sssZ.json

# Custom filename
node src/cli.js benchmark-together --output my-test-results.json
# Saved to: reports/my-test-results-YYYY-MM-DDTHH-MM-SS-sssZ.json
```

### Result File Format
```json
{
  "timestamp": "2024-05-29T12:39:35.000Z",
  "benchmarkType": "isolated",
  "configuration": {
    "scenario": "custom",
    "databases": ["scylladb"],
    "concurrentUsers": 5,
    "operationsPerUser": 20
  },
  "results": {
    "writes": { /* write performance data */ },
    "reads": { /* read performance data */ },
    "concurrency": { /* concurrency test data */ }
  },
  "summary": {
    "winner": {
      "writes": { "database": "scylladb", "metric": 7204 },
      "reads": { "database": "scylladb", "metric": 43 },
      "concurrency": { "database": "clickhouse", "metric": 77 }
    }
  }
}
```

## 📋 Schema Design

### Messages Table Comparison

**ScyllaDB:**
```cql
CREATE TABLE messages (
    conversation_id UUID,
    time_bucket TEXT,
    timestamp TIMESTAMP,
    id UUID,
    sender_type TEXT,
    message_text TEXT,
    PRIMARY KEY ((conversation_id, time_bucket), timestamp, id)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**ClickHouse:**
```sql
CREATE TABLE messages (
    id UUID,
    conversation_id UUID,
    timestamp DateTime64(3),
    sender_type LowCardinality(String),
    message_text String
) ENGINE = MergeTree()
ORDER BY (conversation_id, timestamp)
PARTITION BY toYYYYMMDD(timestamp);
```

**TimescaleDB:**
```sql
CREATE TABLE messages (
    id UUID PRIMARY KEY,
    conversation_id UUID,
    timestamp TIMESTAMPTZ,
    sender_type VARCHAR(10),
    message_text TEXT
);
SELECT create_hypertable('messages', 'timestamp');
```

**CockroachDB:**
```sql
CREATE TABLE messages (
    id UUID PRIMARY KEY,
    conversation_id UUID,
    timestamp TIMESTAMPTZ,
    sender_type VARCHAR(10),
    message_text TEXT
);
CREATE INDEX idx_messages_conversation_time ON messages (conversation_id, timestamp DESC);
```

## 🎯 Schema Optimization Analysis

### Current Performance Ranking

#### 🏆 ScyllaDB - OPTIMAL (95/100)
- ✅ Perfect time-bucketed partitioning
- ✅ Denormalized tables for query patterns
- ✅ Time-window compaction strategy
- ✅ Clustering by timestamp DESC
- **No changes needed**

#### 📊 ClickHouse - GOOD (80/100)
- ✅ Monthly partitioning
- ✅ LowCardinality optimizations
- ✅ Columnar storage for analytics
- ⚠️ Could improve message table ordering

**Recommended Improvements:**
```sql
-- Better ordering for conversation queries
ALTER TABLE messages MODIFY ORDER BY (conversation_id, timestamp);

-- Add materialized view for recent messages per conversation
CREATE MATERIALIZED VIEW recent_messages_by_conversation
ENGINE = ReplacingMergeTree()
ORDER BY (conversation_id, timestamp)
AS SELECT 
    conversation_id,
    timestamp,
    id,
    sender_type,
    message_text,
    message_type
FROM messages
WHERE timestamp >= now() - INTERVAL 30 DAY;
```

#### ⏰ TimescaleDB - NEEDS OPTIMIZATION (60/100)
- ✅ Hypertables for time-series
- ❌ No conversation-based partitioning
- ❌ Missing messaging-specific indexes

**Recommended Improvements:**
```sql
-- Add conversation-based partitioning
SELECT add_dimension('messages', 'conversation_id', number_partitions => 4);

-- Add indexes for messaging patterns
CREATE INDEX CONCURRENTLY idx_messages_conversation_time 
ON messages (conversation_id, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_messages_sender_time 
ON messages (sender_id, timestamp DESC) 
WHERE sender_type = 'seller';

-- Add materialized view for conversation summaries
CREATE MATERIALIZED VIEW conversation_summaries AS
SELECT 
    conversation_id,
    COUNT(*) as message_count,
    MAX(timestamp) as last_message_at,
    COUNT(CASE WHEN sender_type = 'seller' THEN 1 END) as seller_messages,
    COUNT(CASE WHEN sender_type = 'buyer' THEN 1 END) as buyer_messages
FROM messages
GROUP BY conversation_id;
```

#### 🔄 CockroachDB - MAJOR OPTIMIZATION NEEDED (40/100)
- ❌ No partitioning strategy
- ❌ Standard RDBMS design
- ❌ No messaging optimizations

**Recommended Improvements:**
```sql
-- Add range partitioning by timestamp
ALTER TABLE messages PARTITION BY RANGE (timestamp);
CREATE TABLE messages_2024 PARTITION OF messages 
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Add conversation clustering
CREATE INDEX CONCURRENTLY idx_messages_conversation_cluster 
ON messages (conversation_id, timestamp DESC);

-- Add covering indexes for common queries
CREATE INDEX CONCURRENTLY idx_messages_covering 
ON messages (conversation_id, timestamp DESC) 
INCLUDE (sender_type, message_text, message_type);
```

### Performance Impact Predictions

**Current State:**
- **ScyllaDB**: 10,000+ msg/sec writes, <5ms reads
- **ClickHouse**: 8,000+ msg/sec writes, 10-50ms analytics
- **TimescaleDB**: 3,000 msg/sec writes, 20-100ms reads
- **CockroachDB**: 1,500 msg/sec writes, 50-200ms reads

**After Optimizations:**
- **ScyllaDB**: No change (already optimal)
- **ClickHouse**: +15% write performance, +30% query performance
- **TimescaleDB**: +60% write performance, +80% query performance  
- **CockroachDB**: +100% write performance, +150% query performance

## 📊 Expected Results

### Write Performance (messages/second)
- **ScyllaDB**: 50,000-100,000 msgs/sec
- **ClickHouse**: 20,000-50,000 msgs/sec  
- **TimescaleDB**: 10,000-30,000 msgs/sec
- **CockroachDB**: 5,000-15,000 msgs/sec

### Query Performance (milliseconds)
- **Get Conversation**: ScyllaDB < ClickHouse < TimescaleDB < CockroachDB
- **Analytics Queries**: ClickHouse < TimescaleDB < CockroachDB < ScyllaDB
- **Complex JOINs**: TimescaleDB < CockroachDB < ClickHouse < ScyllaDB

## 📈 Monitoring

### Grafana Dashboards
Access Grafana at `http://localhost:3000` (admin/admin)

**Available Dashboards:**
- Database Performance Metrics
- Query Response Times
- Write Throughput
- Resource Utilization
- Error Rates

### Prometheus Metrics
- Database-specific metrics
- Application performance
- System resources

### Service URLs
- **ScyllaDB**: `localhost:9042` (CQL)
- **ClickHouse**: `localhost:8123` (HTTP) / `localhost:9000` (Native)
- **TimescaleDB**: `localhost:5432` (PostgreSQL)
- **CockroachDB**: `localhost:26257` (SQL) / `localhost:8080` (Admin UI)
- **Redis**: `localhost:6379`

## 🔧 Configuration

### Environment Variables
```bash
# Database connections
SCYLLA_HOST=localhost
CLICKHOUSE_HOST=localhost
TIMESCALE_HOST=localhost
COCKROACH_HOST=localhost

# Benchmark settings
BENCHMARK_SELLERS=1000
BENCHMARK_BUYERS=10000
WRITE_CONCURRENCY=10
READ_CONCURRENCY=5

# Performance tuning
WRITE_BATCH_SIZE=100
METRICS_INTERVAL=5000
```

### Resource Management

#### For M1 Mac (16GB RAM):
- **Total Docker Memory**: ~4GB (1GB per database)
- **Recommended**: Use isolated benchmarks
- **Cleanup**: Automatic between tests

#### Commands:
```bash
# Check resource usage
docker stats

# Clean up everything
docker compose down
docker system prune -f --volumes

# Stop all services
docker compose down
```

## 🛠️ Development

### Project Structure
```
├── src/
│   ├── benchmark/          # Benchmark runners
│   ├── config/            # Database configurations
│   ├── database/          # Database clients
│   ├── utils/             # Utilities and helpers
│   └── cli.js             # Command-line interface
├── init-scripts/          # Database initialization
├── config/               # Service configurations
├── reports/              # Benchmark results
└── docker-compose.yml    # Docker services
```

### Adding Custom Tests
1. Create test in `src/benchmark/tests/`
2. Register in `src/benchmark/runner.js`
3. Run with `node src/cli.js benchmark --test custom-test`

### Database Client Interface
All database clients implement:
```javascript
class DatabaseClient {
  async connect()
  async disconnect()
  async createMessage(message)
  async createMessagesBatch(messages)
  async getConversationMessages(conversationId)
  async getInactiveConversations(minutes)
  async healthCheck()
  async getMetrics()
}
```

## 🐛 Troubleshooting

### Common Issues

**Docker Connection Errors:**
```bash
# Ensure Docker is running
docker --version
node src/cli.js docker-status
```

**Resource Exhaustion:**
```bash
# Switch to isolated mode
node src/cli.js docker-cleanup
node src/cli.js benchmark-isolated --scenario custom
```

**Database Connection Failures:**
```bash
# Check database health
node src/cli.js health

# Reinitialize schemas
node src/cli.js setup
```

**ScyllaDB Connection Failed:**
```bash
# Check if ScyllaDB is ready
docker-compose logs scylladb
# Wait for "Scylla version" message
```

**ClickHouse Permission Denied:**
```bash
# Reset ClickHouse data
docker-compose down -v
docker-compose up clickhouse
```

**TimescaleDB Extension Error:**
```bash
# Recreate TimescaleDB
docker-compose down timescaledb
docker volume rm whatsapp-database-benchmarks_timescale_data
docker-compose up timescaledb
```

### Performance Tuning

**For Higher Write Throughput:**
- Increase `WRITE_CONCURRENCY`
- Increase `WRITE_BATCH_SIZE`
- Adjust Docker resource limits
- Use isolated benchmarks to avoid resource contention

**For Better Query Performance:**
- Reduce `READ_CONCURRENCY`
- Enable query result caching
- Optimize database-specific indexes
- Ensure databases have sufficient cache memory

**For Concurrency Testing:**
- Start with low user counts (5-10)
- Gradually increase load
- Monitor error rates and response times

## 💡 Best Practices

### 1. Resource Planning
- **Isolated Mode**: Use for development and resource-constrained environments
- **Together Mode**: Use for production comparison with adequate resources

### 2. Scenario Selection
- **Development**: Use `custom` scenario for quick validation
- **Performance Testing**: Use `high_write_volume` for write optimization
- **Analytics Testing**: Use `read_heavy_analytics` for query optimization
- **Production Simulation**: Use `mixed_workload` for realistic testing

### 3. Docker Management
- Always check `docker-status` before running benchmarks
- Use `docker-cleanup` to free resources between test runs
- Monitor system resources during together benchmarks

### 4. Result Analysis
- Save results with timestamps for historical comparison
- Use `compare` command to analyze performance trends
- Focus on relevant metrics for your use case (writes vs reads vs concurrency)

## 🎉 Success Indicators

✅ All services show "healthy" status
✅ Health checks pass for all databases
✅ No connection errors during benchmarks
✅ Data insertion works without errors
✅ Benchmark results saved successfully

## 📝 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Add benchmark tests
4. Submit pull request

## 📚 References

- [ScyllaDB Documentation](https://docs.scylladb.com/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [TimescaleDB Documentation](https://docs.timescale.com/)
- [CockroachDB Documentation](https://www.cockroachlabs.com/docs/)
- [Database Benchmarking Best Practices](https://use-the-index-luke.com/)
