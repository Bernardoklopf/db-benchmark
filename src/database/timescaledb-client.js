import pkg from 'pg';
const { Pool } = pkg;
import { databaseConfig } from '../config/database.js';

export class TimescaleDBClient {
  constructor() {
    this.pool = null;
    this.isConnected = false;
  }

  async connect() {
    try {
      this.pool = new Pool(databaseConfig.timescaledb);
      
      // Test connection
      const client = await this.pool.connect();
      await client.query('SELECT NOW()');
      client.release();
      
      this.isConnected = true;
      console.log('✅ TimescaleDB connected successfully');
    } catch (error) {
      console.error('❌ TimescaleDB connection failed:', error);
      throw error;
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
        console.log('🔌 TimescaleDB disconnected');
      } catch (error) {
        // Ignore disconnect errors as they're not critical
        console.warn('⚠️  TimescaleDB disconnect warning:', error.message);
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
      INSERT INTO buyers (id, name, phone, platform_id, platform, created_at)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (platform_id, platform) DO NOTHING
    `;
    const values = [
      buyer.id,
      buyer.name,
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
      
      const query = `
        INSERT INTO buyers (id, name, phone, platform_id, platform, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (platform_id, platform) DO NOTHING
      `;
      
      for (const buyer of buyers) {
        const values = [
          buyer.id,
          buyer.name,
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
      INSERT INTO conversations (id, seller_id, buyer_id, platform, created_at, last_message_at, status, message_count)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (seller_id, buyer_id, platform) DO UPDATE SET
        last_message_at = EXCLUDED.last_message_at,
        status = EXCLUDED.status,
        message_count = EXCLUDED.message_count
    `;
    const values = [
      conversation.id,
      conversation.seller_id,
      conversation.buyer_id,
      conversation.platform,
      conversation.created_at,
      conversation.last_message_at,
      conversation.status,
      conversation.message_count || 0
    ];
    
    return await this.pool.query(query, values);
  }

  async createConversationsBatch(conversations) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      const query = `
        INSERT INTO conversations (id, seller_id, buyer_id, platform, created_at, last_message_at, status, message_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (seller_id, buyer_id, platform) DO UPDATE SET
          last_message_at = EXCLUDED.last_message_at,
          status = EXCLUDED.status,
          message_count = EXCLUDED.message_count
      `;
      
      for (const conversation of conversations) {
        const values = [
          conversation.id,
          conversation.seller_id,
          conversation.buyer_id,
          conversation.platform,
          conversation.created_at,
          conversation.last_message_at,
          conversation.status,
          conversation.message_count || 0
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
      INSERT INTO messages (id, conversation_id, sender_type, sender_id, message_text, message_type, metadata, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `;
    const values = [
      message.id,
      message.conversation_id,
      message.sender_type,
      message.sender_id,
      message.message_text,
      message.message_type,
      JSON.stringify(message.metadata || {}),
      message.timestamp
    ];
    
    return await this.pool.query(query, values);
  }

  async createMessagesBatch(messages) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      const query = `
        INSERT INTO messages (id, conversation_id, sender_type, sender_id, message_text, message_type, metadata, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `;
      
      for (const message of messages) {
        const values = [
          message.id,
          message.conversation_id,
          message.sender_type,
          message.sender_id,
          message.message_text,
          message.message_type,
          JSON.stringify(message.metadata || {}),
          message.timestamp
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

  // Optimized batch insert using COPY
  async createMessagesBatchOptimized(messages) {
    const client = await this.pool.connect();
    try {
      const copyQuery = `
        COPY messages (id, conversation_id, sender_type, sender_id, message_text, message_type, metadata, timestamp)
        FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '"')
      `;
      
      const csvData = messages.map(message => [
        message.id,
        message.conversation_id,
        message.sender_type,
        message.sender_id,
        message.message_text.replace(/"/g, '""'), // Escape quotes
        message.message_type,
        JSON.stringify(message.metadata || {}).replace(/"/g, '""'),
        message.timestamp.toISOString()
      ].map(field => `"${field}"`).join(',')).join('\n');
      
      await client.query(copyQuery);
      await client.query(csvData);
      await client.query('\\.');
    } finally {
      client.release();
    }
  }

  // Query operations for benchmarking
  async getConversationMessages(conversationId, limit = 100) {
    const query = `
      SELECT * FROM messages 
      WHERE conversation_id = $1 
      ORDER BY timestamp DESC 
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
    const query = `
      SELECT * FROM messages 
      WHERE timestamp >= NOW() - INTERVAL '${minutes} minutes'
      ORDER BY timestamp DESC 
      LIMIT $1
    `;
    
    const result = await this.pool.query(query, [limit]);
    return result.rows;
  }

  async getInactiveConversations(minutes = 5) {
    const query = `
      SELECT * FROM inactive_conversations 
      WHERE minutes_since_last_message >= $1
    `;
    
    const result = await this.pool.query(query, [minutes]);
    return result.rows;
  }

  async getConversationStats(conversationId) {
    const query = `
      SELECT 
        COUNT(*) as message_count,
        MIN(timestamp) as first_message,
        MAX(timestamp) as last_message,
        COUNT(DISTINCT sender_id) as unique_senders
      FROM messages 
      WHERE conversation_id = $1
    `;
    
    const result = await this.pool.query(query, [conversationId]);
    return result.rows[0];
  }

  async getHourlyMessageStats(hours = 24) {
    const query = `
      SELECT * FROM hourly_message_stats 
      WHERE hour >= NOW() - INTERVAL '${hours} hours'
      ORDER BY hour DESC
    `;
    
    const result = await this.pool.query(query);
    return result.rows;
  }

  async getDailyConversationStats(days = 7) {
    const query = `
      SELECT * FROM daily_conversation_stats 
      WHERE day >= NOW() - INTERVAL '${days} days'
      ORDER BY day DESC
    `;
    
    const result = await this.pool.query(query);
    return result.rows;
  }

  async searchMessages(searchTerm, limit = 100) {
    const query = `
      SELECT * FROM messages 
      WHERE message_text ILIKE $1
      ORDER BY timestamp DESC 
      LIMIT $2
    `;
    
    const result = await this.pool.query(query, [`%${searchTerm}%`, limit]);
    return result.rows;
  }

  async searchMessagesFullText(searchTerm, limit = 100) {
    const query = `
      SELECT *, ts_rank(to_tsvector('english', message_text), plainto_tsquery('english', $1)) as rank
      FROM messages 
      WHERE to_tsvector('english', message_text) @@ plainto_tsquery('english', $1)
      ORDER BY rank DESC, timestamp DESC 
      LIMIT $2
    `;
    
    const result = await this.pool.query(query, [searchTerm, limit]);
    return result.rows;
  }

  // Advanced analytics queries
  async getConversationAnalytics(sellerId, days = 30) {
    const query = `
      SELECT 
        c.id,
        c.platform,
        c.status,
        c.message_count,
        EXTRACT(EPOCH FROM (c.last_message_at - c.created_at))/3600 as conversation_duration_hours,
        b.name as buyer_name,
        b.platform_id as buyer_platform_id
      FROM conversations c
      JOIN buyers b ON c.buyer_id = b.id
      WHERE c.seller_id = $1 
      AND c.created_at >= NOW() - INTERVAL '${days} days'
      ORDER BY c.last_message_at DESC
    `;
    
    const result = await this.pool.query(query, [sellerId]);
    return result.rows;
  }

  async getMessageVolumeByHour(days = 7) {
    const query = `
      SELECT 
        EXTRACT(HOUR FROM timestamp) as hour,
        COUNT(*) as message_count,
        COUNT(DISTINCT conversation_id) as active_conversations
      FROM messages 
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      GROUP BY EXTRACT(HOUR FROM timestamp)
      ORDER BY hour
    `;
    
    const result = await this.pool.query(query);
    return result.rows;
  }

  async healthCheck() {
    try {
      const result = await this.pool.query('SELECT NOW() as timestamp, version() as version');
      return { 
        status: 'healthy', 
        timestamp: result.rows[0].timestamp,
        version: result.rows[0].version
      };
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

      // Execute queries sequentially to avoid overwhelming the connection pool
      const results = [];
      for (const query of queries) {
        try {
          const result = await this.pool.query(query);
          results.push(result);
        } catch (queryError) {
          console.warn(`Warning: Query failed: ${query}`, queryError.message);
          // Return a default result for failed queries
          results.push({ rows: [{ count: 0 }] });
        }
      }

      return {
        sellers: Number.parseInt(results[0].rows[0].seller_count || results[0].rows[0].count || 0),
        buyers: Number.parseInt(results[1].rows[0].buyer_count || results[1].rows[0].count || 0),
        conversations: Number.parseInt(results[2].rows[0].conversation_count || results[2].rows[0].count || 0),
        messages: Number.parseInt(results[3].rows[0].message_count || results[3].rows[0].count || 0)
      };
    } catch (error) {
      console.error('Error getting TimescaleDB metrics:', error);
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
      const queries = [
        `SELECT 
          schemaname,
          tablename,
          pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
         FROM pg_tables 
         WHERE schemaname = 'public'`,
        `SELECT 
          chunk_schema,
          chunk_name,
          pg_size_pretty(pg_total_relation_size(chunk_schema||'.'||chunk_name)) as size
         FROM timescaledb_information.chunks 
         WHERE hypertable_name = 'messages'
         ORDER BY chunk_name DESC
         LIMIT 10`
      ];

      const results = await Promise.all(
        queries.map(query => this.pool.query(query))
      );

      return {
        tables: results[0].rows,
        recent_chunks: results[1].rows
      };
    } catch (error) {
      console.error('Error getting TimescaleDB system metrics:', error);
      return null;
    }
  }

  async getHypertableInfo() {
    try {
      const query = `
        SELECT 
          hypertable_name,
          num_chunks,
          pg_size_pretty(total_bytes) as total_size,
          pg_size_pretty(index_bytes) as index_size,
          pg_size_pretty(toast_bytes) as toast_size,
          compression_enabled
        FROM timescaledb_information.hypertables h
        LEFT JOIN timescaledb_information.hypertable_compression_settings c 
          ON h.hypertable_name = c.hypertable_name
      `;
      
      const result = await this.pool.query(query);
      return result.rows;
    } catch (error) {
      console.error('Error getting hypertable info:', error);
      return null;
    }
  }

  async initializeSchema() {
    try {
      console.log('🏗️  Initializing TimescaleDB schema...');
      
      // Create tables
      const tables = [
        // Sellers table
        'CREATE TABLE IF NOT EXISTS sellers (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE, phone VARCHAR(50), created_at TIMESTAMPTZ DEFAULT NOW(), active BOOLEAN DEFAULT true)',
        
        // Buyers table
        'CREATE TABLE IF NOT EXISTS buyers (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name VARCHAR(255), phone VARCHAR(50), platform_id VARCHAR(255) NOT NULL, platform VARCHAR(20) NOT NULL CHECK (platform IN (\'whatsapp\', \'instagram\')), created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE (platform_id, platform))',
        
        // Conversations table
        'CREATE TABLE IF NOT EXISTS conversations (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), seller_id UUID NOT NULL REFERENCES sellers(id), buyer_id UUID NOT NULL REFERENCES buyers(id), platform VARCHAR(20) NOT NULL CHECK (platform IN (\'whatsapp\', \'instagram\')), created_at TIMESTAMPTZ DEFAULT NOW(), last_message_at TIMESTAMPTZ DEFAULT NOW(), status VARCHAR(20) DEFAULT \'active\' CHECK (status IN (\'active\', \'closed\', \'archived\')), message_count INTEGER DEFAULT 0, UNIQUE (seller_id, buyer_id, platform))',
        
        // Messages table
        'CREATE TABLE IF NOT EXISTS messages (id UUID DEFAULT gen_random_uuid(), conversation_id UUID NOT NULL REFERENCES conversations(id), sender_type VARCHAR(10) NOT NULL CHECK (sender_type IN (\'seller\', \'buyer\')), sender_id UUID NOT NULL, message_text TEXT, message_type VARCHAR(20) DEFAULT \'text\' CHECK (message_type IN (\'text\', \'image\', \'audio\', \'video\', \'document\', \'location\')), metadata JSONB, timestamp TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (id, timestamp))'
      ];
      
      for (const tableSQL of tables) {
        await this.pool.query(tableSQL);
      }
      
      // Convert messages to hypertable if not already done
      try {
        await this.pool.query('SELECT create_hypertable(\'messages\', \'timestamp\', chunk_time_interval => INTERVAL \'1 day\', if_not_exists => TRUE)');
      } catch (error) {
        // Ignore if hypertable already exists
        if (!error.message.includes('already a hypertable')) {
          console.warn('⚠️  Could not create hypertable:', error.message);
        }
      }
      
      // Create indexes
      const indexes = [
        'CREATE INDEX IF NOT EXISTS idx_conversations_seller_id ON conversations (seller_id)',
        'CREATE INDEX IF NOT EXISTS idx_conversations_buyer_id ON conversations (buyer_id)',
        'CREATE INDEX IF NOT EXISTS idx_conversations_last_message_at ON conversations (last_message_at)',
        'CREATE INDEX IF NOT EXISTS idx_conversations_platform ON conversations (platform)',
        'CREATE INDEX IF NOT EXISTS idx_conversations_status ON conversations (status)',
        'CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages (conversation_id, timestamp DESC)',
        'CREATE INDEX IF NOT EXISTS idx_messages_sender_id ON messages (sender_id)',
        'CREATE INDEX IF NOT EXISTS idx_messages_sender_type ON messages (sender_type)',
        'CREATE INDEX IF NOT EXISTS idx_messages_message_type ON messages (message_type)'
      ];
      
      for (const indexSQL of indexes) {
        try {
          await this.pool.query(indexSQL);
        } catch (error) {
          // Ignore index creation errors (they might already exist)
          console.warn(`⚠️  Could not create index: ${error.message}`);
        }
      }
      
      console.log('✅ TimescaleDB schema initialized successfully');
    } catch (error) {
      console.error('❌ TimescaleDB schema initialization failed:', error);
      throw error;
    }
  }

  async truncateAllTables() {
    try {
      console.log('🧹 Cleaning TimescaleDB tables...');
      
      // Instead of TRUNCATE, drop and recreate tables to avoid memory issues
      const tables = ['messages', 'conversations', 'sellers', 'buyers'];
      
      for (const table of tables) {
        try {
          // Drop table if exists
          await this.pool.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
          console.log(`🗑️  Dropped ${table}`);
        } catch (error) {
          console.log(`⚠️  Could not drop ${table}: ${error.message}`);
        }
      }
      
      // Recreate tables by calling initializeSchema
      await this.initializeSchema();
      console.log('✅ TimescaleDB cleanup completed');
    } catch (error) {
      console.error('❌ TimescaleDB cleanup failed:', error);
      throw error;
    }
  }
}
