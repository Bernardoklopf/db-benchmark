import dotenv from 'dotenv';

dotenv.config();

export const databaseConfig = {
  scylladb: {
    contactPoints: [process.env.SCYLLA_HOST || 'localhost'],
    localDataCenter: 'datacenter1',
    keyspace: 'benchmark',
    port: Number.parseInt(process.env.SCYLLA_PORT) || 9042,
    credentials: {
      username: process.env.SCYLLA_USER || '',
      password: process.env.SCYLLA_PASSWORD || ''
    },
    pooling: {
      maxRequestsPerConnection: 32768,
      coreConnectionsPerHost: {
        [2]: 2, // LOCAL
        [3]: 1  // REMOTE
      }
    },
    socketOptions: {
      connectTimeout: 30000,
      readTimeout: 30000
    }
  },

  clickhouse: {
    host: process.env.CLICKHOUSE_HOST || 'localhost',
    port: Number.parseInt(process.env.CLICKHOUSE_PORT) || 8123,
    database: process.env.CLICKHOUSE_DB || 'benchmark',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
    compression: {
      request: true,
      response: true
    },
    max_open_connections: 10,
    request_timeout: 30000,
    session_timeout: 300000
  },

  timescaledb: {
    host: process.env.TIMESCALE_HOST || 'localhost',
    port: Number.parseInt(process.env.TIMESCALE_PORT) || 5432,
    database: process.env.TIMESCALE_DB || 'benchmark',
    user: process.env.TIMESCALE_USER || 'benchmark_user',
    password: process.env.TIMESCALE_PASSWORD || 'benchmark_pass',
    max: 10, // reduced max number of clients in the pool
    min: 2, // minimum number of clients in the pool
    idleTimeoutMillis: 10000, // reduced idle timeout
    connectionTimeoutMillis: 5000, // increased connection timeout
    acquireTimeoutMillis: 10000, // timeout for acquiring connection from pool
    statement_timeout: 30000,
    query_timeout: 30000,
    ssl: false,
    // Additional connection options to prevent resets
    keepAlive: true,
    keepAliveInitialDelayMillis: 10000
  },

  cockroachdb: {
    host: process.env.COCKROACH_HOST || 'localhost',
    port: Number.parseInt(process.env.COCKROACH_PORT) || 26257,
    database: process.env.COCKROACH_DB || 'benchmark',
    user: process.env.COCKROACH_USER || 'root',
    password: process.env.COCKROACH_PASSWORD || '',
    max: 10, // max number of clients in the pool
    min: 2, // minimum number of clients in the pool
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 5000,
    acquireTimeoutMillis: 10000,
    statement_timeout: 30000,
    query_timeout: 30000,
    ssl: false, // Disabled for single-node insecure mode
    keepAlive: true,
    keepAliveInitialDelayMillis: 10000,
    application_name: 'benchmark_app'
  },

  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: Number.parseInt(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number.parseInt(process.env.REDIS_DB) || 0,
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3,
    lazyConnect: true
  }
};

export const benchmarkConfig = {
  // Data generation settings
  dataGeneration: {
    sellers: Number.parseInt(process.env.BENCHMARK_SELLERS) || 1000,
    buyers: Number.parseInt(process.env.BENCHMARK_BUYERS) || 10000,
    conversations: Number.parseInt(process.env.BENCHMARK_CONVERSATIONS) || 5000,
    messagesPerConversation: Number.parseInt(process.env.BENCHMARK_MESSAGES_PER_CONV) || 100,
    batchSize: Number.parseInt(process.env.BENCHMARK_BATCH_SIZE) || 1000,
    platforms: ['whatsapp', 'instagram']
  },

  // Benchmark test settings
  performance: {
    writeTests: {
      concurrency: Number.parseInt(process.env.WRITE_CONCURRENCY) || 10,
      duration: Number.parseInt(process.env.WRITE_DURATION) || 60, // seconds
      batchSize: Number.parseInt(process.env.WRITE_BATCH_SIZE) || 100
    },
    readTests: {
      concurrency: Number.parseInt(process.env.READ_CONCURRENCY) || 5,
      iterations: Number.parseInt(process.env.READ_ITERATIONS) || 1000,
      queryTypes: [
        'get_conversation',
        'get_inactive_conversations',
        'get_recent_messages',
        'get_conversation_stats',
        'search_messages'
      ]
    }
  },

  // Monitoring and logging
  monitoring: {
    metricsInterval: Number.parseInt(process.env.METRICS_INTERVAL) || 5000, // ms
    enableDetailedLogging: process.env.DETAILED_LOGGING === 'true',
    outputFormat: process.env.OUTPUT_FORMAT || 'table', // table, json, csv
    resultsPath: process.env.RESULTS_PATH || './results'
  }
};
