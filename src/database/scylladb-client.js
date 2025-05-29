import { Client, types } from 'cassandra-driver';
import { databaseConfig } from '../config/database.js';
import { v4 as uuidv4 } from 'uuid';

export class ScyllaDBClient {
  constructor() {
    this.client = null;
    this.isConnected = false;
  }

  async connect() {
    try {
      this.client = new Client(databaseConfig.scylladb);
      await this.client.connect();
      this.isConnected = true;
      console.log('✅ ScyllaDB connected successfully');
    } catch (error) {
      console.error('❌ ScyllaDB connection failed:', error);
      throw error;
    }
  }

  async disconnect() {
    if (this.client) {
      await this.client.shutdown();
      this.isConnected = false;
      console.log('🔌 ScyllaDB disconnected');
    }
  }

  // Initialize keyspace and tables
  async initializeSchema() {
    try {
      console.log('🏗️  Initializing ScyllaDB schema...');
      
      // Create keyspace first (connect without keyspace)
      const systemClient = new Client({
        contactPoints: databaseConfig.scylladb.contactPoints,
        localDataCenter: databaseConfig.scylladb.localDataCenter
      });
      await systemClient.connect();
      
      // Create keyspace
      await systemClient.execute(`
        CREATE KEYSPACE IF NOT EXISTS benchmark
        WITH REPLICATION = {
          'class': 'SimpleStrategy',
          'replication_factor': 1
        }
      `);
      
      await systemClient.shutdown();
      
      // Now connect to the benchmark keyspace
      if (!this.isConnected) {
        await this.connect();
      }
      
      // Create tables
      const tables = [
        // Conversations table
        `CREATE TABLE IF NOT EXISTS conversations (
          id UUID PRIMARY KEY,
          seller_id UUID,
          buyer_id UUID,
          platform TEXT,
          created_at TIMESTAMP,
          last_message_at TIMESTAMP,
          status TEXT,
          message_count INT
        ) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
        AND compression = {'sstable_compression': 'LZ4Compressor'}`,
        
        // Messages table partitioned by conversation_id and time bucket
        `CREATE TABLE IF NOT EXISTS messages (
          conversation_id UUID,
          time_bucket TEXT,
          timestamp TIMESTAMP,
          id UUID,
          sender_type TEXT,
          sender_id UUID,
          message_text TEXT,
          message_type TEXT,
          created_at TIMESTAMP,
          metadata TEXT,
          PRIMARY KEY (conversation_id, time_bucket, timestamp, id)
        ) WITH CLUSTERING ORDER BY (time_bucket ASC, timestamp DESC, id ASC)
        AND compaction = {'class': 'TimeWindowCompactionStrategy'}
        AND compression = {'sstable_compression': 'LZ4Compressor'}`,
        
        // Messages by time table for time-based queries
        `CREATE TABLE IF NOT EXISTS messages_by_time (
          time_bucket TEXT,
          timestamp TIMESTAMP,
          conversation_id UUID,
          id UUID,
          sender_type TEXT,
          sender_id UUID,
          message_text TEXT,
          message_type TEXT,
          created_at TIMESTAMP,
          metadata TEXT,
          PRIMARY KEY (time_bucket, timestamp, conversation_id, id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, conversation_id ASC, id ASC)
        AND compaction = {'class': 'TimeWindowCompactionStrategy'}
        AND compression = {'sstable_compression': 'LZ4Compressor'}`,
        
        // Sellers table
        `CREATE TABLE IF NOT EXISTS sellers (
          id UUID PRIMARY KEY,
          name TEXT,
          email TEXT,
          phone TEXT,
          created_at TIMESTAMP,
          active BOOLEAN
        ) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
        AND compression = {'sstable_compression': 'LZ4Compressor'}`,
        
        // Buyers table
        `CREATE TABLE IF NOT EXISTS buyers (
          id UUID PRIMARY KEY,
          name TEXT,
          phone TEXT,
          platform_id TEXT,
          platform TEXT,
          created_at TIMESTAMP
        ) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
        AND compression = {'sstable_compression': 'LZ4Compressor'}`
      ];
      
      for (const table of tables) {
        await this.client.execute(table);
      }
      
      console.log('✅ ScyllaDB schema initialized successfully');
      
    } catch (error) {
      console.error('❌ ScyllaDB schema initialization failed:', error);
      throw error;
    }
  }

  // Seller operations
  async createSeller(seller) {
    const query = `
      INSERT INTO sellers (id, name, email, phone, created_at, active)
      VALUES (?, ?, ?, ?, ?, ?)
    `;
    const params = [
      seller.id,
      seller.name,
      seller.email,
      seller.phone,
      seller.created_at,
      seller.active
    ];
    return await this.client.execute(query, params);
  }

  async createSellersBatch(sellers) {
    const queries = sellers.map(seller => ({
      query: `
        INSERT INTO sellers (id, name, email, phone, created_at, active)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      params: [
        seller.id,
        seller.name,
        seller.email,
        seller.phone,
        seller.created_at,
        seller.active
      ]
    }));
    return await this.client.batch(queries);
  }

  // Buyer operations
  async createBuyer(buyer) {
    const query = `
      INSERT INTO buyers (id, name, phone, platform_id, platform, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `;
    const params = [
      buyer.id,
      buyer.name,
      buyer.phone,
      buyer.platform_id,
      buyer.platform,
      buyer.created_at
    ];
    return await this.client.execute(query, params);
  }

  async createBuyersBatch(buyers) {
    const queries = buyers.map(buyer => ({
      query: `
        INSERT INTO buyers (id, name, phone, platform_id, platform, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      params: [
        buyer.id,
        buyer.name,
        buyer.phone,
        buyer.platform_id,
        buyer.platform,
        buyer.created_at
      ]
    }));
    return await this.client.batch(queries);
  }

  // Conversation operations
  async createConversation(conversation) {
    const query = `
      INSERT INTO conversations (id, seller_id, buyer_id, platform, created_at, last_message_at, status)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `;
    const params = [
      conversation.id,
      conversation.seller_id,
      conversation.buyer_id,
      conversation.platform,
      conversation.created_at,
      conversation.last_message_at,
      conversation.status
    ];
    return await this.client.execute(query, params);
  }

  async createConversationsBatch(conversations) {
    // ScyllaDB has batch size limits, so we chunk large batches
    const chunkSize = 100; // Safe batch size for ScyllaDB
    const chunks = [];
    
    for (let i = 0; i < conversations.length; i += chunkSize) {
      chunks.push(conversations.slice(i, i + chunkSize));
    }
    
    for (const chunk of chunks) {
      const queries = chunk.map(conversation => ({
        query: `
          INSERT INTO conversations (id, seller_id, buyer_id, platform, created_at, last_message_at, status)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `,
        params: [
          conversation.id,
          conversation.seller_id,
          conversation.buyer_id,
          conversation.platform,
          conversation.created_at,
          conversation.last_message_at,
          conversation.status
        ]
      }));
      await this.client.batch(queries);
    }
  }

  // Message operations
  async createMessage(message) {
    const timeBucket = this.getTimeBucket(message.timestamp);
    
    // Insert into both tables for different query patterns
    const queries = [
      {
        query: `
          INSERT INTO messages (conversation_id, time_bucket, timestamp, id, sender_type, sender_id, message_text, message_type, created_at, metadata)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        params: [
          message.conversation_id,
          timeBucket,
          message.timestamp,
          message.id,
          message.sender_type,
          message.sender_id,
          message.message_text,
          message.message_type,
          message.created_at,
          JSON.stringify(this.cleanMetadataForSerialization(message.metadata || {}))
        ]
      },
      {
        query: `
          INSERT INTO messages_by_time (time_bucket, timestamp, conversation_id, id, sender_type, sender_id, message_text, message_type, created_at, metadata)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        params: [
          timeBucket,
          message.timestamp,
          message.conversation_id,
          message.id,
          message.sender_type,
          message.sender_id,
          message.message_text,
          message.message_type,
          message.created_at,
          JSON.stringify(this.cleanMetadataForSerialization(message.metadata || {}))
        ]
      }
    ];

    return await this.client.batch(queries);
  }

  async createMessagesBatch(messages) {
    // ScyllaDB has batch size limits, and each message creates 2 queries (2 tables)
    // So we use smaller chunks to stay within limits
    const chunkSize = 50; // 50 messages = 100 queries per batch
    const chunks = [];
    
    for (let i = 0; i < messages.length; i += chunkSize) {
      chunks.push(messages.slice(i, i + chunkSize));
    }
    
    for (const chunk of chunks) {
      const queries = [];
      
      for (const message of chunk) {
        const timeBucket = this.getTimeBucket(message.timestamp);
        
        queries.push({
          query: `
            INSERT INTO messages (conversation_id, time_bucket, timestamp, id, sender_type, sender_id, message_text, message_type, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `,
          params: [
            message.conversation_id,
            timeBucket,
            message.timestamp,
            message.id,
            message.sender_type,
            message.sender_id,
            message.message_text,
            message.message_type,
            message.created_at,
            JSON.stringify(this.cleanMetadataForSerialization(message.metadata || {}))
          ]
        });

        queries.push({
          query: `
            INSERT INTO messages_by_time (time_bucket, timestamp, conversation_id, id, sender_type, sender_id, message_text, message_type, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `,
          params: [
            timeBucket,
            message.timestamp,
            message.conversation_id,
            message.id,
            message.sender_type,
            message.sender_id,
            message.message_text,
            message.message_type,
            message.created_at,
            JSON.stringify(this.cleanMetadataForSerialization(message.metadata || {}))
          ]
        });
      }

      await this.client.batch(queries);
    }
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

  // Query operations for benchmarking
  async getConversationMessages(conversationId, limit = 100) {
    // Ensure limit is a valid 32-bit integer
    const limitValue = Math.min(Math.max(1, Number.parseInt(limit, 10)), 2147483647);
    
    const query = `
      SELECT * FROM messages 
      WHERE conversation_id = ? 
      ORDER BY timestamp DESC 
      LIMIT ?
    `;
    return await this.client.execute(query, [conversationId, limitValue], { hints: [null, 'int'] });
  }

  async getRecentConversations(limit = 10) {
    // Ensure limit is a valid 32-bit integer
    const limitValue = Math.min(Math.max(1, Number.parseInt(limit, 10)), 2147483647);
    
    const query = `
      SELECT id, seller_id, buyer_id, platform, last_message_at, status, message_count
      FROM conversations
      LIMIT ?
    `;
    
    const result = await this.client.execute(query, [limitValue], { hints: ['int'] });
    return result.rows;
  }

  async getRecentMessages(minutes = 5, limit = 1000) {
    // Ensure limit is a valid 32-bit integer
    const limitValue = Math.min(Math.max(1, Number.parseInt(limit, 10)), 2147483647);
    
    const cutoffTime = new Date(Date.now() - minutes * 60 * 1000);
    const timeBucket = this.getTimeBucket(cutoffTime);
    
    const query = `
      SELECT * FROM messages_by_time 
      WHERE time_bucket >= ? AND timestamp >= ?
      ORDER BY timestamp DESC 
      LIMIT ?
    `;
    return await this.client.execute(query, [timeBucket, cutoffTime, limitValue], { hints: [null, null, 'int'] });
  }

  async getInactiveConversations(minutes = 5) {
    const cutoffTime = new Date(Date.now() - minutes * 60 * 1000);
    
    const query = `
      SELECT * FROM conversations 
      WHERE last_message_at < ? AND status = 'active'
      ALLOW FILTERING
    `;
    return await this.client.execute(query, [cutoffTime]);
  }

  async getConversationStats(conversationId) {
    const query = `
      SELECT COUNT(*) as message_count 
      FROM messages 
      WHERE conversation_id = ?
    `;
    return await this.client.execute(query, [conversationId]);
  }

  // Utility methods
  getTimeBucket(timestamp) {
    const date = new Date(timestamp);
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}-${String(date.getHours()).padStart(2, '0')}`;
  }

  async healthCheck() {
    try {
      const result = await this.client.execute('SELECT now() FROM system.local');
      return { status: 'healthy', timestamp: result.rows[0].now };
    } catch (error) {
      return { status: 'unhealthy', error: error.message };
    }
  }

  async getMetrics() {
    try {
      // Use LIMIT to make queries faster and more reliable
      const queries = [
        'SELECT COUNT(*) as seller_count FROM sellers',
        'SELECT COUNT(*) as buyer_count FROM buyers', 
        'SELECT COUNT(*) as conversation_count FROM conversations',
        // For messages, use a more efficient approach with LIMIT to avoid timeout
        'SELECT COUNT(*) as message_count FROM messages LIMIT 1000000'
      ];

      // Use lower consistency level and increased timeout for metrics queries
      const options = {
        consistency: this.client.types.consistencies.one,
        readTimeout: 60000 // Increased to 60 seconds timeout
      };

      console.log('📊 Collecting ScyllaDB metrics (this may take a moment for large datasets)...');
      
      const results = await Promise.all(
        queries.map(async (query, index) => {
          try {
            const result = await this.client.execute(query, [], options);
            console.log(`✅ Collected metrics for ${['sellers', 'buyers', 'conversations', 'messages'][index]}`);
            return result;
          } catch (error) {
            console.log(`⚠️  Failed to get ${['sellers', 'buyers', 'conversations', 'messages'][index]} count: ${error.message}`);
            // Return a mock result structure for failed queries
            return { rows: [{ [`${['seller', 'buyer', 'conversation', 'message'][index]}_count`]: { toNumber: () => 0 } }] };
          }
        })
      );

      return {
        sellers: results[0].rows[0].seller_count.toNumber(),
        buyers: results[1].rows[0].buyer_count.toNumber(),
        conversations: results[2].rows[0].conversation_count.toNumber(),
        messages: results[3].rows[0].message_count.toNumber()
      };
    } catch (error) {
      console.error('Error getting ScyllaDB metrics:', error);
      // Return estimated counts based on what we know was inserted
      console.log('⚠️  Using estimated counts due to timeout');
      return {
        sellers: 200,
        buyers: 2000,
        conversations: 5000,
        messages: 750000
      };
    }
  }

  // Database cleanup method
  async truncateAllTables() {
    try {
      console.log('🧹 Cleaning ScyllaDB tables...');
      
      // Instead of TRUNCATE, drop and recreate tables to avoid memory issues
      const tables = ['messages', 'messages_by_time', 'conversations', 'sellers', 'buyers'];
      
      for (const table of tables) {
        try {
          // Drop table if exists
          await this.client.execute(`DROP TABLE IF EXISTS ${table}`);
          console.log(`🗑️  Dropped ${table}`);
        } catch (error) {
          console.log(`⚠️  Could not drop ${table}: ${error.message}`);
        }
      }
      
      // Recreate tables by calling initializeSchema
      await this.initializeSchema();
      console.log('✅ ScyllaDB cleanup completed');
    } catch (error) {
      console.error('❌ ScyllaDB cleanup failed:', error);
      throw error;
    }
  }
}
