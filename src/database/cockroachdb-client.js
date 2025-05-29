import { Pool, Client } from 'pg';
import { databaseConfig } from '../config/database.js';

export class CockroachDBClient {
  constructor() {
    this.pool = null;
    this.isConnected = false;
  }

  async connect() {
    try {
      this.pool = new Pool(databaseConfig.cockroachdb);
      
      // Test connection
      const client = await this.pool.connect();
      await client.query('SELECT NOW()');
      client.release();
      
      this.isConnected = true;
      console.log('✅ CockroachDB connected successfully');
    } catch (error) {
      // Provide a clean error message without the stack trace
      const errorMessage = error.message || 'Unknown error';
      console.error(`❌ CockroachDB connection failed: ${errorMessage}`);
      throw new Error('Connection failed - database may not be ready yet');
    }
  }

  async disconnect() {
    if (this.pool) {
      try {
        // Wait a moment for any pending operations to complete
        await new Promise(resolve => setTimeout(resolve, 100));
        
        // Gracefully end the pool
        await this.pool.end();
        this.pool = null;
        this.isConnected = false;
        console.log('🔌 CockroachDB disconnected');
      } catch (error) {
        // Ignore disconnect errors as they're not critical
        console.warn('⚠️  CockroachDB disconnect warning:', error.message);
        this.pool = null;
        this.isConnected = false;
      }
    }
  }

  // Seller operations
  async createSeller(seller) {
    const query = `
      INSERT INTO sellers (id, name, email, phone, created_at, active)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (id) DO NOTHING
    `;
    const values = [
      seller.id,
      seller.name,
      seller.email,
      seller.phone,
      seller.created_at,
      seller.active
    ];
    
    return await this.pool.query(query, values);
  }

  async createSellersBatch(sellers) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      const query = `
        INSERT INTO sellers (id, name, email, phone, created_at, active)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (id) DO NOTHING
      `;
      
      for (const seller of sellers) {
        const values = [
          seller.id,
          seller.name,
          seller.email,
          seller.phone,
          seller.created_at,
          seller.active
        ];
        await client.query(query, values);
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // Buyer operations
  async createBuyer(buyer) {
    const query = `
      INSERT INTO buyers (id, name, email, phone, platform_id, platform, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (id) DO NOTHING
    `;
    const values = [
      buyer.id,
      buyer.name,
      buyer.email,
      buyer.phone,
      buyer.platform_id,
      buyer.platform,
      buyer.created_at
    ];
    
    return await this.pool.query(query, values);
  }

  async createBuyersBatch(buyers) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      for (const buyer of buyers) {
        const query = `
          INSERT INTO buyers (id, name, email, phone, platform_id, platform, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT (id) DO NOTHING
        `;
        const values = [
          buyer.id,
          buyer.name,
          buyer.email,
          buyer.phone,
          buyer.platform_id,
          buyer.platform,
          buyer.created_at
        ];
        await client.query(query, values);
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // Conversation operations
  async createConversation(conversation) {
    const query = `
      INSERT INTO conversations (id, seller_id, buyer_id, platform, conversation_id, status, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (id) DO NOTHING
    `;
    const values = [
      conversation.id,
      conversation.seller_id,
      conversation.buyer_id,
      conversation.platform,
      conversation.conversation_id,
      conversation.status,
      conversation.created_at,
      conversation.updated_at
    ];
    
    return await this.pool.query(query, values);
  }

  async createConversationsBatch(conversations) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      const query = `
        INSERT INTO conversations (id, seller_id, buyer_id, platform, conversation_id, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO NOTHING
      `;
      
      for (const conversation of conversations) {
        const values = [
          conversation.id,
          conversation.seller_id,
          conversation.buyer_id,
          conversation.platform,
          conversation.conversation_id,
          conversation.status,
          conversation.created_at,
          conversation.updated_at
        ];
        await client.query(query, values);
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // Message operations
  async createMessage(message) {
    const query = `
      INSERT INTO messages (id, conversation_id, sender_type, sender_id, message_type, message_text, metadata, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (id) DO NOTHING
    `;
    const values = [
      message.id,
      message.conversation_id,
      message.sender_type,
      message.sender_id,
      message.message_type,
      message.message_text,
      JSON.stringify(message.metadata),
      message.timestamp
    ];
    
    return await this.pool.query(query, values);
  }

  async createMessagesBatch(messages) {
    const query = `
      INSERT INTO messages (id, conversation_id, sender_type, sender_id, message_text, message_type, metadata, timestamp)
      VALUES ${messages.map(() => '(?, ?, ?, ?, ?, ?, ?, ?)').join(', ')}
    `;
    
    const values = messages.flatMap(msg => [
      msg.id,
      msg.conversation_id,
      msg.sender_type,
      msg.sender_id,
      msg.message_text,
      msg.message_type,
      JSON.stringify(msg.metadata || {}),
      msg.timestamp
    ]);
    
    await this.pool.query(query, values);
  }

  async createMessagesBatchOptimized(messages) {
    const client = await this.pool.connect();
    try {
      // Use CockroachDB's IMPORT or bulk insert capabilities
      const values = messages.map(message => [
        message.id,
        message.conversation_id,
        message.sender_type,
        message.sender_id,
        message.message_type,
        message.message_text,
        JSON.stringify(message.metadata),
        message.timestamp
      ]);

      const query = `
        INSERT INTO messages (id, conversation_id, sender_type, sender_id, message_type, message_text, metadata, timestamp)
        SELECT * FROM UNNEST($1::TEXT[], $2::UUID[], $3::TEXT[], $4::UUID[], $5::TEXT[], $6::TEXT[], $7::JSONB[], $8::TIMESTAMPTZ[])
        ON CONFLICT (id) DO NOTHING
      `;

      // Transpose the values array for UNNEST
      const transposed = [
        values.map(v => v[0]), // ids
        values.map(v => v[1]), // conversation_ids
        values.map(v => v[2]), // sender_types
        values.map(v => v[3]), // sender_ids
        values.map(v => v[4]), // message_types
        values.map(v => v[5]), // message_texts
        values.map(v => v[6]), // metadata
        values.map(v => v[7])  // timestamps
      ];

      return await client.query(query, transposed);
    } catch (error) {
      // Fallback to regular batch insert
      console.warn('⚠️  Optimized batch insert failed, falling back to regular batch:', error.message);
      return await this.createMessagesBatch(messages);
    } finally {
      client.release();
    }
  }

  // Read operations
  async getConversationMessages(conversationId, limit = 100) {
    const query = `
      SELECT m.*, c.seller_id, c.buyer_id, c.platform
      FROM messages m
      JOIN conversations c ON m.conversation_id = c.id
      WHERE m.conversation_id = $1
      ORDER BY m.timestamp DESC
      LIMIT $2
    `;
    
    const result = await this.pool.query(query, [conversationId, limit]);
    return result.rows;
  }

  async getRecentConversations(limit = 10) {
    const query = `
      SELECT id, seller_id, buyer_id, platform, last_message_at, status, message_count
      FROM conversations
      ORDER BY last_message_at DESC
      LIMIT $1
    `;
    
    const result = await this.pool.query(query, [limit]);
    return result.rows;
  }

  async getRecentMessages(minutes = 5, limit = 1000) {
    const cutoffTime = new Date(Date.now() - minutes * 60 * 1000);
    const query = `
      SELECT * FROM messages 
      WHERE timestamp >= $1 
      ORDER BY timestamp DESC 
      LIMIT $2
    `;
    const result = await this.pool.query(query, [cutoffTime, limit]);
    return result.rows;
  }

  async getInactiveConversations(minutes = 5) {
    const cutoffTime = new Date(Date.now() - minutes * 60 * 1000);
    const query = `
      SELECT c.*, 
             MAX(m.timestamp) as last_message_time,
             COUNT(m.id) as message_count
      FROM conversations c
      LEFT JOIN messages m ON c.id = m.conversation_id
      GROUP BY c.id, c.seller_id, c.buyer_id, c.platform, c.status, c.created_at, c.updated_at
      HAVING MAX(m.timestamp) < $1 OR MAX(m.timestamp) IS NULL
      ORDER BY last_message_time DESC NULLS LAST
    `;
    const result = await this.pool.query(query, [cutoffTime]);
    return result.rows;
  }

  async getConversationStats(conversationId) {
    const query = `
      SELECT 
        conversation_id,
        COUNT(*) as message_count,
        MIN(timestamp) as first_message,
        MAX(timestamp) as last_message,
        COUNT(DISTINCT sender_type) as unique_senders
      FROM messages 
      WHERE conversation_id = $1
      GROUP BY conversation_id
    `;
    const result = await this.pool.query(query, [conversationId]);
    return result.rows[0] || null;
  }

  async getHourlyMessageStats(hours = 24) {
    const query = `
      SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as message_count,
        COUNT(DISTINCT conversation_id) as unique_conversations
      FROM messages 
      WHERE timestamp >= NOW() - INTERVAL '${hours} hours'
      GROUP BY hour
      ORDER BY hour DESC
    `;
    const result = await this.pool.query(query);
    return result.rows;
  }

  async getDailyConversationStats(days = 7) {
    const query = `
      SELECT 
        DATE_TRUNC('day', created_at) as day,
        COUNT(*) as conversation_count,
        COUNT(DISTINCT seller_id) as unique_sellers,
        COUNT(DISTINCT buyer_id) as unique_buyers
      FROM conversations 
      WHERE created_at >= NOW() - INTERVAL '${days} days'
      GROUP BY day
      ORDER BY day DESC
    `;
    const result = await this.pool.query(query);
    return result.rows;
  }

  async searchMessages(searchTerm, limit = 100) {
    const query = `
      SELECT m.*, c.seller_id, c.buyer_id, c.platform
      FROM messages m
      JOIN conversations c ON m.conversation_id = c.id
      WHERE m.message_text ILIKE $1
      ORDER BY m.timestamp DESC
      LIMIT $2
    `;
    const result = await this.pool.query(query, [`%${searchTerm}%`, limit]);
    return result.rows;
  }

  async searchMessagesFullText(searchTerm, limit = 100) {
    // CockroachDB supports full-text search with inverted indexes
    const query = `
      SELECT m.*, c.seller_id, c.buyer_id, c.platform,
             ts_rank(to_tsvector('english', m.message_text), plainto_tsquery('english', $1)) as rank
      FROM messages m
      JOIN conversations c ON m.conversation_id = c.id
      WHERE to_tsvector('english', m.message_text) @@ plainto_tsquery('english', $1)
      ORDER BY rank DESC, m.timestamp DESC
      LIMIT $2
    `;
    const result = await this.pool.query(query, [searchTerm, limit]);
    return result.rows;
  }

  async getConversationAnalytics(sellerId, days = 30) {
    const query = `
      SELECT 
        c.platform,
        COUNT(DISTINCT c.id) as conversation_count,
        COUNT(m.id) as total_messages,
        AVG(EXTRACT(EPOCH FROM (c.updated_at - c.created_at))/3600) as avg_duration_hours,
        COUNT(DISTINCT c.buyer_id) as unique_buyers
      FROM conversations c
      LEFT JOIN messages m ON c.id = m.conversation_id
      WHERE c.seller_id = $1 
        AND c.created_at >= NOW() - INTERVAL '${days} days'
      GROUP BY c.platform
      ORDER BY conversation_count DESC
    `;
    const result = await this.pool.query(query, [sellerId]);
    return result.rows;
  }

  async getMessageVolumeByHour(days = 7) {
    const query = `
      SELECT 
        EXTRACT(HOUR FROM timestamp) as hour_of_day,
        COUNT(*) as message_count,
        AVG(LENGTH(message_text)) as avg_message_length
      FROM messages 
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      GROUP BY hour_of_day
      ORDER BY hour_of_day
    `;
    const result = await this.pool.query(query);
    return result.rows;
  }

  // Health and monitoring
  async healthCheck() {
    try {
      const result = await this.pool.query('SELECT 1 as status');
      return {
        status: 'healthy',
        timestamp: new Date().toISOString(),
        response_time: Date.now(),
        details: result.rows[0]
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
        error: error.message
      };
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
        queries.map(query => this.pool.query(query))
      );

      return {
        sellers: Number.parseInt(results[0].rows[0]?.seller_count || 0),
        buyers: Number.parseInt(results[1].rows[0]?.buyer_count || 0),
        conversations: Number.parseInt(results[2].rows[0]?.conversation_count || 0),
        messages: Number.parseInt(results[3].rows[0]?.message_count || 0),
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('❌ Error getting CockroachDB metrics:', error);
      return {
        sellers: 0,
        buyers: 0,
        conversations: 0,
        messages: 0,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  async getSystemMetrics() {
    try {
      const queries = [
        `SELECT 
           node_id,
           address,
           build_tag,
           started_at,
           updated_at
         FROM crdb_internal.node_build_info`,
        `SELECT 
           database_name,
           table_name,
           approximate_disk_bytes
         FROM crdb_internal.table_sizes 
         WHERE database_name = 'benchmark'
         ORDER BY approximate_disk_bytes DESC
         LIMIT 10`
      ];

      const [nodeInfo, tableInfo] = await Promise.all(
        queries.map(query => this.pool.query(query).catch(() => ({ rows: [] })))
      );

      return {
        node_info: nodeInfo.rows,
        table_sizes: tableInfo.rows,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('❌ Error getting CockroachDB system metrics:', error);
      return {
        node_info: [],
        table_sizes: [],
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  async getClusterInfo() {
    try {
      const query = `
        SELECT 
          node_id,
          address,
          locality,
          build_tag,
          started_at
        FROM crdb_internal.node_build_info
      `;
      const result = await this.pool.query(query);
      return result.rows;
    } catch (error) {
      console.error('❌ Error getting CockroachDB cluster info:', error);
      return [];
    }
  }

  // Schema management
  async initializeSchema() {
    try {
      console.log('🏗️  Initializing CockroachDB schema...');
      
      const client = await this.pool.connect();
      
      try {
        // Create database if it doesn't exist
        await client.query('CREATE DATABASE IF NOT EXISTS benchmark');
        await client.query('USE benchmark');

        // Create sellers table
        await client.query(`
          CREATE TABLE IF NOT EXISTS sellers (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name STRING NOT NULL,
            email STRING UNIQUE NOT NULL,
            phone STRING,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            active BOOLEAN DEFAULT true,
            INDEX idx_sellers_email (email),
            INDEX idx_sellers_active (active),
            INDEX idx_sellers_created_at (created_at)
          )
        `);

        // Create buyers table
        await client.query(`
          CREATE TABLE IF NOT EXISTS buyers (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name STRING NOT NULL,
            email STRING,
            phone STRING,
            platform_id STRING NOT NULL,
            platform STRING NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            active BOOLEAN DEFAULT true,
            INDEX idx_buyers_platform (platform),
            INDEX idx_buyers_platform_id (platform_id),
            INDEX idx_buyers_active (active),
            INDEX idx_buyers_created_at (created_at),
            UNIQUE INDEX idx_buyers_seller_platform (platform_id, platform)
          )
        `);

        // Create conversations table
        await client.query(`
          CREATE TABLE IF NOT EXISTS conversations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            seller_id UUID NOT NULL,
            buyer_id UUID NOT NULL,
            platform STRING NOT NULL,
            conversation_id STRING NOT NULL,
            status STRING NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            FOREIGN KEY (seller_id) REFERENCES sellers(id) ON DELETE CASCADE,
            FOREIGN KEY (buyer_id) REFERENCES buyers(id) ON DELETE CASCADE,
            INDEX idx_conversations_seller_id (seller_id),
            INDEX idx_conversations_buyer_id (buyer_id),
            INDEX idx_conversations_platform (platform),
            INDEX idx_conversations_status (status),
            INDEX idx_conversations_created_at (created_at),
            INDEX idx_conversations_updated_at (updated_at),
            UNIQUE INDEX idx_conversations_unique (seller_id, buyer_id, platform)
          )
        `);

        // Create messages table
        await client.query(`
          CREATE TABLE IF NOT EXISTS messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_id UUID NOT NULL,
            sender_type STRING NOT NULL CHECK (sender_type IN ('seller', 'buyer')),
            sender_id UUID NOT NULL,
            message_text STRING,
            message_type STRING NOT NULL DEFAULT 'text' CHECK (
              message_type IN ('text', 'image', 'audio', 'video', 'document', 'location')
            ),
            metadata JSONB,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            INDEX idx_messages_conversation_time (conversation_id, timestamp DESC),
            INDEX idx_messages_sender_time (sender_id, timestamp DESC),
            INDEX idx_messages_conversation_sender (conversation_id, sender_type, timestamp DESC),
            INDEX idx_messages_covering (conversation_id, timestamp DESC) STORING (sender_type, message_text, message_type),
            INDEX idx_messages_timestamp (timestamp DESC),
            INVERTED INDEX idx_messages_metadata (metadata),
            FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
          )
        `);

        console.log('✅ CockroachDB schema initialized successfully');
      } finally {
        client.release();
      }
    } catch (error) {
      // Provide a clean error message without the stack trace
      const errorMessage = error.message || 'Unknown error';
      console.error(`❌ CockroachDB schema initialization failed: ${errorMessage}`);
      throw new Error(errorMessage);
    }
  }

  async truncateAllTables() {
    try {
      console.log('🗑️  Cleaning up CockroachDB tables...');
      
      const client = await this.pool.connect();
      
      try {
        // Drop tables in reverse dependency order
        const tables = ['messages', 'conversations', 'buyers', 'sellers'];
        
        for (const table of tables) {
          await client.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
          console.log(`🗑️  Dropped table: ${table}`);
        }
        
        // Recreate schema
        await this.initializeSchema();
        
        console.log('✅ CockroachDB cleanup completed');
      } finally {
        client.release();
      }
    } catch (error) {
      // Provide a clean error message without the stack trace
      const errorMessage = error.message || 'Unknown error';
      console.error(`❌ Error during CockroachDB cleanup: ${errorMessage}`);
      throw new Error(errorMessage);
    }
  }
}
