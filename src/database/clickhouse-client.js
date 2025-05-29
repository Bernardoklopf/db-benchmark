import { createClient } from '@clickhouse/client';
import { databaseConfig } from '../config/database.js';

export class ClickHouseClient {
  constructor() {
    this.client = null;
    this.isConnected = false;
  }

  async connect() {
    try {
      this.client = createClient({
        url: `http://${databaseConfig.clickhouse.host}:${databaseConfig.clickhouse.port}`,
        username: databaseConfig.clickhouse.username,
        password: databaseConfig.clickhouse.password,
        database: databaseConfig.clickhouse.database,
        compression: databaseConfig.clickhouse.compression,
        request_timeout: databaseConfig.clickhouse.request_timeout,
        session_timeout: databaseConfig.clickhouse.session_timeout,
        max_open_connections: databaseConfig.clickhouse.max_open_connections
      });

      // Test connection
      await this.client.ping();
      this.isConnected = true;
      console.log('✅ ClickHouse connected successfully');
    } catch (error) {
      console.error('❌ ClickHouse connection failed:', error);
      throw error;
    }
  }

  async disconnect() {
    if (this.client) {
      await this.client.close();
      this.isConnected = false;
      console.log('🔌 ClickHouse disconnected');
    }
  }

  // Helper function to format timestamps for ClickHouse
  formatTimestamp(timestamp) {
    if (!(timestamp instanceof Date)) {
      return timestamp;
    }
    // ClickHouse expects YYYY-MM-DD HH:MM:SS format for JSONEachRow
    return timestamp.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, '');
  }

  // Helper method to clean metadata for serialization
  cleanMetadataForSerialization(metadata) {
    const cleaned = {};
    for (const [key, value] of Object.entries(metadata)) {
      if (value instanceof Date) {
        cleaned[key] = value.toISOString();
      } else if (typeof value === 'object' && value !== null) {
        cleaned[key] = this.cleanMetadataForSerialization(value);
      } else {
        cleaned[key] = value;
      }
    }
    return cleaned;
  }

  // Seller operations
  async createSeller(seller) {
    return await this.client.insert({
      table: 'sellers',
      values: [{
        id: seller.id,
        name: seller.name,
        email: seller.email,
        phone: seller.phone,
        created_at: this.formatTimestamp(seller.created_at),
        active: seller.active ? 1 : 0
      }],
      format: 'JSONEachRow'
    });
  }

  async createSellersBatch(sellers) {
    const values = sellers.map(seller => ({
      id: seller.id,
      name: seller.name,
      email: seller.email,
      phone: seller.phone,
      created_at: this.formatTimestamp(seller.created_at),
      active: seller.active ? 1 : 0
    }));

    return await this.client.insert({
      table: 'sellers',
      values,
      format: 'JSONEachRow'
    });
  }

  // Buyer operations
  async createBuyer(buyer) {
    return await this.client.insert({
      table: 'buyers',
      values: [{
        id: buyer.id,
        name: buyer.name,
        phone: buyer.phone,
        platform_id: buyer.platform_id,
        platform: buyer.platform,
        created_at: this.formatTimestamp(buyer.created_at)
      }],
      format: 'JSONEachRow'
    });
  }

  async createBuyersBatch(buyers) {
    const values = buyers.map(buyer => ({
      id: buyer.id,
      name: buyer.name,
      phone: buyer.phone,
      platform_id: buyer.platform_id,
      platform: buyer.platform,
      created_at: this.formatTimestamp(buyer.created_at)
    }));

    return await this.client.insert({
      table: 'buyers',
      values,
      format: 'JSONEachRow'
    });
  }

  // Conversation operations
  async createConversation(conversation) {
    return await this.client.insert({
      table: 'conversations',
      values: [{
        id: conversation.id,
        seller_id: conversation.seller_id,
        buyer_id: conversation.buyer_id,
        platform: conversation.platform,
        created_at: this.formatTimestamp(conversation.created_at),
        last_message_at: this.formatTimestamp(conversation.last_message_at),
        status: conversation.status,
        message_count: conversation.message_count || 0
      }],
      format: 'JSONEachRow'
    });
  }

  async createConversationsBatch(conversations) {
    const values = conversations.map(conversation => ({
      id: conversation.id,
      seller_id: conversation.seller_id,
      buyer_id: conversation.buyer_id,
      platform: conversation.platform,
      created_at: this.formatTimestamp(conversation.created_at),
      last_message_at: this.formatTimestamp(conversation.last_message_at),
      status: conversation.status,
      message_count: conversation.message_count || 0
    }));

    return await this.client.insert({
      table: 'conversations',
      values,
      format: 'JSONEachRow'
    });
  }

  // Message operations
  async createMessage(message) {
    // Convert Date objects in metadata to ISO strings before JSON.stringify
    const cleanMetadata = this.cleanMetadataForSerialization(message.metadata || {});
    
    const messageData = {
      id: message.id,
      conversation_id: message.conversation_id,
      sender_type: message.sender_type,
      sender_id: message.sender_id,
      message_text: message.message_text,
      message_type: message.message_type,
      metadata: JSON.stringify(cleanMetadata),
      timestamp: this.formatTimestamp(message.timestamp)
    };

    // Insert into both tables for different query patterns
    await Promise.all([
      this.client.insert({
        table: 'messages',
        values: [messageData],
        format: 'JSONEachRow'
      }),
      this.client.insert({
        table: 'messages_by_time',
        values: [messageData],
        format: 'JSONEachRow'
      })
    ]);
  }

  async createMessagesBatch(messages) {
    // ClickHouse has smaller batch size limits, so we chunk the messages
    const chunkSize = 1000; // Increased batch size for better performance
    
    const chunks = [];
    
    for (let i = 0; i < messages.length; i += chunkSize) {
      chunks.push(messages.slice(i, i + chunkSize));
    }

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      try {
        const values = chunk.map(message => {
          // Convert Date objects in metadata to ISO strings before JSON.stringify
          const cleanMetadata = this.cleanMetadataForSerialization(message.metadata || {});
          
          return {
            id: message.id,
            conversation_id: message.conversation_id,
            sender_type: message.sender_type,
            sender_id: message.sender_id,
            message_text: message.message_text,
            message_type: message.message_type,
            metadata: JSON.stringify(cleanMetadata),
            timestamp: this.formatTimestamp(message.timestamp)
          };
        });

        // Insert into both tables for different query patterns
        await Promise.all([
          this.client.insert({
            table: 'messages',
            values,
            format: 'JSONEachRow'
          }),
          this.client.insert({
            table: 'messages_by_time',
            values,
            format: 'JSONEachRow'
          })
        ]);
      } catch (error) {
        console.error(`Error writing batch ${i + 1}/${chunks.length}:`, error.message);
      }
    }
  }

  // Query operations for benchmarking
  async getConversationMessages(conversationId, limit = 100) {
    const query = `
      SELECT * FROM messages 
      WHERE conversation_id = {conversationId: UUID}
      ORDER BY timestamp DESC 
      LIMIT {limit: UInt32}
    `;
    
    const result = await this.client.query({
      query,
      query_params: {
        conversationId,
        limit
      },
      format: 'JSONEachRow'
    });
    
    return await result.json();
  }

  async getRecentMessages(minutes = 5, limit = 1000) {
    const cutoffTime = new Date(Date.now() - minutes * 60 * 1000);
    
    const query = `
      SELECT * FROM messages_by_time 
      WHERE timestamp >= {cutoffTime: DateTime64(3)}
      ORDER BY timestamp DESC 
      LIMIT {limit: UInt32}
    `;
    
    const result = await this.client.query({
      query,
      query_params: {
        cutoffTime: this.formatTimestamp(cutoffTime),
        limit
      },
      format: 'JSONEachRow'
    });
    
    return await result.json();
  }

  async getInactiveConversations(minutes = 5) {
    const cutoffTime = new Date(Date.now() - minutes * 60 * 1000);
    
    const query = `
      SELECT * FROM conversations 
      WHERE last_message_at < {cutoffTime: DateTime64(3)} 
      AND status = 'active'
    `;
    
    const result = await this.client.query({
      query,
      query_params: {
        cutoffTime: this.formatTimestamp(cutoffTime)
      },
      format: 'JSONEachRow'
    });
    
    return await result.json();
  }

  async getConversationStats(conversationId) {
    const query = `
      SELECT COUNT(*) as message_count 
      FROM messages 
      WHERE conversation_id = {conversationId: UUID}
    `;
    
    const result = await this.client.query({
      query,
      query_params: {
        conversationId
      },
      format: 'JSONEachRow'
    });
    
    return await result.json();
  }

  async getHourlyMessageStats(hours = 24) {
    const query = `
      SELECT 
        hour,
        sender_type,
        message_type,
        sum(message_count) as total_messages,
        avg(avg_message_length) as avg_length
      FROM hourly_message_stats 
      WHERE hour >= now() - INTERVAL {hours: UInt32} HOUR
      GROUP BY hour, sender_type, message_type
      ORDER BY hour DESC
    `;
    
    const result = await this.client.query({
      query,
      query_params: { hours },
      format: 'JSONEachRow'
    });
    
    return await result.json();
  }

  async searchMessages(searchTerm, limit = 100) {
    const query = `
      SELECT * FROM messages 
      WHERE positionCaseInsensitive(message_text, {searchTerm: String}) > 0
      ORDER BY timestamp DESC 
      LIMIT {limit: UInt32}
    `;
    
    const result = await this.client.query({
      query,
      query_params: {
        searchTerm,
        limit
      },
      format: 'JSONEachRow'
    });
    
    return await result.json();
  }

  async healthCheck() {
    try {
      const result = await this.client.query({
        query: 'SELECT now() as timestamp',
        format: 'JSONEachRow'
      });
      const data = await result.json();
      return { status: 'healthy', timestamp: data[0].timestamp };
    } catch (error) {
      return { status: 'unhealthy', error: error.message };
    }
  }

  async getMetrics() {
    try {
      const queries = [
        'SELECT COUNT(*) as seller_count FROM sellers',
        'SELECT COUNT(*) as buyer_count FROM buyers', 
        'SELECT COUNT(*) as conversation_count FROM conversations',
        'SELECT COUNT(*) as message_count FROM messages'
      ];

      const results = await Promise.all(
        queries.map(async query => {
          const result = await this.client.query({ query });
          return result;
        })
      );

      return {
        sellers: results[0]?.data?.[0]?.seller_count || 0,
        buyers: results[1]?.data?.[0]?.buyer_count || 0,
        conversations: results[2]?.data?.[0]?.conversation_count || 0,
        messages: results[3]?.data?.[0]?.message_count || 0
      };
    } catch (error) {
      console.error('Error getting ClickHouse metrics:', error);
      return {
        sellers: 0,
        buyers: 0,
        conversations: 0,
        messages: 0
      };
    }
  }

  async getSystemMetrics() {
    try {
      const query = `
        SELECT 
          formatReadableSize(sum(bytes_on_disk)) as disk_usage,
          sum(rows) as total_rows,
          count() as table_count
        FROM system.parts 
        WHERE database = 'benchmark' AND active = 1
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      return await result.json();
    } catch (error) {
      console.error('Error getting ClickHouse system metrics:', error);
      return null;
    }
  }

  async initializeSchema() {
    try {
      console.log('🏗️  Initializing ClickHouse schema...');
      
      // Create database
      await this.client.command({
        query: 'CREATE DATABASE IF NOT EXISTS benchmark'
      });
      
      await this.client.command({
        query: 'USE benchmark'
      });
      
      // Create tables
      const tables = [
        // Sellers table
        `CREATE TABLE IF NOT EXISTS sellers (
          id UUID,
          name String,
          email String,
          phone String,
          created_at DateTime64(3),
          active UInt8
        ) ENGINE = MergeTree()
        ORDER BY id SETTINGS index_granularity = 8192`,
        
        // Buyers table
        `CREATE TABLE IF NOT EXISTS buyers (
          id UUID,
          name String,
          phone String,
          platform_id String,
          platform LowCardinality(String),
          created_at DateTime64(3)
        ) ENGINE = MergeTree()
        ORDER BY id SETTINGS index_granularity = 8192`,
        
        // Conversations table
        `CREATE TABLE IF NOT EXISTS conversations (
          id UUID,
          seller_id UUID,
          buyer_id UUID,
          platform LowCardinality(String),
          created_at DateTime64(3),
          last_message_at DateTime64(3),
          status LowCardinality(String),
          message_count UInt32
        ) ENGINE = MergeTree()
        ORDER BY (seller_id, created_at)
        PARTITION BY toYYYYMM(created_at) SETTINGS index_granularity = 8192`,
        
        // Messages table - optimized for time-series analytics
        `CREATE TABLE IF NOT EXISTS messages (
          id UUID,
          conversation_id UUID,
          sender_type LowCardinality(String),
          sender_id UUID,
          message_text String,
          message_type LowCardinality(String),
          metadata String,
          timestamp DateTime64(3)
        ) ENGINE = MergeTree()
        ORDER BY (conversation_id, timestamp)
        PARTITION BY toYYYYMMDD(timestamp) SETTINGS index_granularity = 8192`,
        
        // Messages by time - for time-based analytics
        `CREATE TABLE IF NOT EXISTS messages_by_time (
          timestamp DateTime64(3),
          conversation_id UUID,
          id UUID,
          sender_type LowCardinality(String),
          sender_id UUID,
          message_text String,
          message_type LowCardinality(String),
          metadata String
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, conversation_id)
        PARTITION BY toYYYYMMDD(timestamp) SETTINGS index_granularity = 8192`
      ];
      
      // Execute table creation queries
      for (const tableQuery of tables) {
        await this.client.command({ query: tableQuery });
      }
      
      // Create materialized views
      const views = [
        // Materialized view for real-time conversation updates
        `CREATE MATERIALIZED VIEW IF NOT EXISTS conversation_stats 
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
          toDate(timestamp)`,
        
        // Materialized view for hourly message statistics
        `CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_message_stats 
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
          message_type`
      ];
      
      // Execute materialized view creation queries
      for (const viewQuery of views) {
        try {
          await this.client.command({ query: viewQuery });
        } catch (error) {
          console.log(`⚠️  Could not create materialized view: ${error.message}`);
        }
      }
      
      // Create indexes
      const indexes = [
        'ALTER TABLE conversations ADD INDEX idx_last_message_at last_message_at TYPE minmax GRANULARITY 1',
        'ALTER TABLE conversations ADD INDEX idx_seller_id seller_id TYPE bloom_filter GRANULARITY 1',
        'ALTER TABLE messages ADD INDEX idx_sender_id sender_id TYPE bloom_filter GRANULARITY 1',
        'ALTER TABLE messages ADD INDEX idx_message_type message_type TYPE set(100) GRANULARITY 1'
      ];
      
      // Execute index creation queries
      for (const indexQuery of indexes) {
        try {
          await this.client.command({ query: indexQuery });
        } catch (error) {
          console.log(`⚠️  Could not create index: ${error.message}`);
        }
      }
      
      console.log('✅ ClickHouse schema initialized successfully');
    } catch (error) {
      console.error('❌ ClickHouse schema initialization failed:', error);
      throw error;
    }
  }

  // Database cleanup method
  async truncateAllTables() {
    try {
      console.log('🧹 Cleaning ClickHouse tables...');
      
      // Instead of TRUNCATE, drop and recreate tables to avoid memory issues
      const tables = ['messages', 'messages_by_time', 'conversations', 'sellers', 'buyers'];
      
      for (const table of tables) {
        try {
          // Drop table if exists
          await this.client.command({
            query: `DROP TABLE IF EXISTS ${table}`
          });
          console.log(`🗑️  Dropped ${table}`);
        } catch (error) {
          console.log(`⚠️  Could not drop ${table}: ${error.message}`);
        }
      }
      
      // Recreate tables by calling initializeSchema
      await this.initializeSchema();
      console.log('✅ ClickHouse cleanup completed');
    } catch (error) {
      console.error('❌ ClickHouse cleanup failed:', error);
      throw error;
    }
  }

  async getRecentConversations(limit = 10) {
    const query = `
      SELECT id, seller_id, buyer_id, platform, last_message_at, status, message_count
      FROM conversations
      ORDER BY last_message_at DESC
      LIMIT {limit: UInt32}
    `;
    
    const result = await this.client.query({
      query,
      query_params: { limit },
      format: 'JSONEachRow'
    });
    
    return await result.json();
  }
}
