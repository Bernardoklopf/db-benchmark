import { faker } from '@faker-js/faker';
import { v4 as uuidv4 } from 'uuid';

export class DataGenerator {
  constructor() {
    this.platforms = ['whatsapp', 'instagram'];
    this.messageTypes = ['text', 'image', 'audio', 'video', 'document'];
    this.senderTypes = ['seller', 'buyer'];
    this.conversationStatuses = ['active', 'closed', 'archived'];
  }

  // Generate sellers
  generateSeller() {
    return {
      id: uuidv4(),
      name: faker.company.name(),
      email: faker.internet.email(),
      phone: faker.phone.number(),
      created_at: faker.date.past({ years: 2 }),
      active: faker.datatype.boolean(0.8) // 80% active
    };
  }

  generateSellers(count) {
    return Array.from({ length: count }, () => this.generateSeller());
  }

  // Generate buyers
  generateBuyer(platform = null) {
    const selectedPlatform = platform || faker.helpers.arrayElement(this.platforms);
    return {
      id: uuidv4(),
      name: faker.person.fullName(),
      email: faker.internet.email(),
      phone: faker.phone.number(),
      platform_id: this.generatePlatformId(selectedPlatform),
      platform: selectedPlatform,
      created_at: faker.date.past({ years: 1 })
    };
  }

  generateBuyers(count, platform = null) {
    return Array.from({ length: count }, () => this.generateBuyer(platform));
  }

  // Generate conversations
  generateConversation(sellerId, buyerId, platform = 'whatsapp') {
    const createdAt = faker.date.recent({ days: 3 }); // Reduced from past year to recent 3 days
    const lastMessageAt = faker.date.between({ 
      from: createdAt, 
      to: new Date() 
    });

    return {
      id: uuidv4(),
      seller_id: sellerId,
      buyer_id: buyerId,
      platform: platform,
      conversation_id: this.generatePlatformId(platform), // Generate unique conversation ID for the platform
      created_at: createdAt,
      updated_at: lastMessageAt,
      last_message_at: lastMessageAt,
      status: faker.helpers.arrayElement(this.conversationStatuses),
      message_count: faker.number.int({ min: 1, max: 500 })
    };
  }

  generateConversations(sellerIds, buyerIds, conversationsPerSeller = 10) {
    const conversations = [];
    const uniqueConversations = new Set(); // Track unique combinations
    
    for (const sellerId of sellerIds) {
      const shuffledBuyers = faker.helpers.shuffle([...buyerIds]);
      let conversationsCreated = 0;
      
      for (const buyerId of shuffledBuyers) {
        if (conversationsCreated >= conversationsPerSeller) break;
        
        // Generate conversation and check for uniqueness
        const conversation = this.generateConversation(sellerId, buyerId);
        const uniqueKey = `${sellerId}:${buyerId}:${conversation.platform}`;
        
        // Only add if this combination doesn't exist
        if (!uniqueConversations.has(uniqueKey)) {
          uniqueConversations.add(uniqueKey);
          conversations.push(conversation);
          conversationsCreated++;
        }
      }
    }
    
    return conversations;
  }

  // Generate messages
  generateMessage(conversationId, senderId, senderType, baseTimestamp = null) {
    const timestamp = baseTimestamp || faker.date.recent({ days: 3 }); // Reduced from 30 to 3 days to avoid ClickHouse partition limits
    const messageType = faker.helpers.arrayElement(this.messageTypes);
    
    return {
      id: uuidv4(),
      conversation_id: conversationId,
      sender_type: senderType,
      sender_id: senderId,
      message_text: this.generateMessageText(messageType),
      message_type: messageType,
      metadata: this.generateMessageMetadata(messageType),
      timestamp: timestamp
    };
  }

  generateConversationMessages(conversation, sellerIds, buyerIds, messagesPerConversation = 50) {
    const messages = [];
    const startTime = conversation.created_at;
    const endTime = conversation.last_message_at;
    
    // Generate timestamps distributed between start and end
    const timestamps = this.generateDistributedTimestamps(
      startTime, 
      endTime, 
      messagesPerConversation
    );

    for (let i = 0; i < messagesPerConversation; i++) {
      const senderType = faker.helpers.arrayElement(this.senderTypes);
      const senderId = senderType === 'seller' ? conversation.seller_id : conversation.buyer_id;
      
      messages.push(this.generateMessage(
        conversation.id,
        senderId,
        senderType,
        timestamps[i]
      ));
    }

    return messages.sort((a, b) => a.timestamp - b.timestamp);
  }

  generateBulkMessages(conversations, sellerIds, buyerIds, messagesPerConversation = 50) {
    const allMessages = [];
    
    for (const conversation of conversations) {
      const messages = this.generateConversationMessages(
        conversation,
        sellerIds,
        buyerIds,
        messagesPerConversation
      );
      allMessages.push(...messages);
    }
    
    return allMessages;
  }

  // Generate realistic message content based on type
  generateMessageText(messageType) {
    switch (messageType) {
      case 'text':
        return faker.lorem.sentences({ min: 1, max: 3 });
      case 'image':
        return '📷 Image';
      case 'audio':
        return '🎵 Audio message';
      case 'video':
        return '🎥 Video';
      case 'document':
        return `📄 ${faker.system.fileName()}`;
      default:
        return faker.lorem.sentence();
    }
  }

  // Generate metadata based on message type
  generateMessageMetadata(messageType) {
    const baseMetadata = {
      delivered: faker.datatype.boolean(0.95),
      read: faker.datatype.boolean(0.8),
      timestamp_delivered: faker.date.recent({ days: 1 }),
      timestamp_read: faker.date.recent({ days: 1 })
    };

    switch (messageType) {
      case 'image':
        return {
          ...baseMetadata,
          file_size: faker.number.int({ min: 100000, max: 5000000 }),
          width: faker.number.int({ min: 480, max: 1920 }),
          height: faker.number.int({ min: 480, max: 1080 }),
          format: faker.helpers.arrayElement(['jpg', 'png', 'gif'])
        };
      case 'audio':
        return {
          ...baseMetadata,
          duration: faker.number.int({ min: 1, max: 300 }),
          file_size: faker.number.int({ min: 50000, max: 2000000 }),
          format: 'ogg'
        };
      case 'video':
        return {
          ...baseMetadata,
          duration: faker.number.int({ min: 5, max: 600 }),
          file_size: faker.number.int({ min: 1000000, max: 50000000 }),
          width: faker.number.int({ min: 480, max: 1920 }),
          height: faker.number.int({ min: 480, max: 1080 }),
          format: 'mp4'
        };
      case 'document':
        return {
          ...baseMetadata,
          file_size: faker.number.int({ min: 10000, max: 10000000 }),
          file_name: faker.system.fileName(),
          mime_type: faker.system.mimeType()
        };
      default:
        return baseMetadata;
    }
  }

  // Generate platform-specific IDs
  generatePlatformId(platform) {
    switch (platform) {
      case 'whatsapp':
        return `${faker.phone.number()}@c.us`;
      case 'instagram':
        return faker.internet.username();
      default:
        return faker.string.alphanumeric(10);
    }
  }

  // Generate distributed timestamps between two dates
  generateDistributedTimestamps(startDate, endDate, count) {
    const start = startDate.getTime();
    const end = endDate.getTime();
    const interval = (end - start) / count;
    
    const timestamps = [];
    for (let i = 0; i < count; i++) {
      // Add some randomness to make it more realistic
      const baseTime = start + (interval * i);
      const randomOffset = faker.number.int({ 
        min: -interval * 0.3, 
        max: interval * 0.3 
      });
      timestamps.push(new Date(baseTime + randomOffset));
    }
    
    return timestamps.sort((a, b) => a - b);
  }

  // Generate realistic conversation patterns
  generateRealisticConversationBatch(sellerCount, buyerCount, options = {}) {
    const {
      conversationsPerSeller = 20,
      messagesPerConversation = 100,
      activePlatformDistribution = { whatsapp: 0.7, instagram: 0.3 },
      timeRange = { days: 30 }
    } = options;

    console.log('🏭 Generating realistic test data...');
    
    // Generate sellers
    console.log(`👥 Generating ${sellerCount} sellers...`);
    const sellers = this.generateSellers(sellerCount);
    const sellerIds = sellers.map(s => s.id);

    // Generate buyers with platform distribution
    console.log(`🛒 Generating ${buyerCount} buyers...`);
    const buyers = [];
    const whatsappCount = Math.floor(buyerCount * activePlatformDistribution.whatsapp);
    const instagramCount = buyerCount - whatsappCount;
    
    buyers.push(...this.generateBuyers(whatsappCount, 'whatsapp'));
    buyers.push(...this.generateBuyers(instagramCount, 'instagram'));
    const buyerIds = buyers.map(b => b.id);

    // Generate conversations
    console.log("💬 Generating conversations...");
    const conversations = this.generateConversations(
      sellerIds, 
      buyerIds, 
      conversationsPerSeller
    );

    // Generate messages
    console.log("📝 Generating messages...");
    const messages = this.generateBulkMessages(
      conversations,
      sellerIds,
      buyerIds,
      messagesPerConversation
    );

    console.log(`✅ Generated:
    - ${sellers.length} sellers
    - ${buyers.length} buyers  
    - ${conversations.length} conversations
    - ${messages.length} messages`);

    return {
      sellers,
      buyers,
      conversations,
      messages,
      summary: {
        sellers: sellers.length,
        buyers: buyers.length,
        conversations: conversations.length,
        messages: messages.length,
        platforms: {
          whatsapp: buyers.filter(b => b.platform === 'whatsapp').length,
          instagram: buyers.filter(b => b.platform === 'instagram').length
        }
      }
    };
  }

  // Generate simple messages for benchmarking
  generateMessages(count) {
    const messages = [];
    
    // Create test sellers and buyers first to satisfy foreign key constraints
    const seller = this.generateSeller();
    const buyer = this.generateBuyer();
    
    // Create a test conversation with valid seller and buyer IDs
    const conversation = this.generateConversation(seller.id, buyer.id);
    
    // Generate messages for this conversation
    for (let i = 0; i < count; i++) {
      const senderType = faker.helpers.arrayElement(this.senderTypes);
      const senderId = senderType === 'seller' ? seller.id : buyer.id;
      
      messages.push({
        id: uuidv4(),
        conversation_id: conversation.id,
        sender_type: senderType,
        sender_id: senderId,
        message_text: faker.lorem.sentence(),
        message_type: faker.helpers.arrayElement(this.messageTypes),
        metadata: { benchmark: true },
        timestamp: new Date(),
        created_at: new Date()
      });
    }
    
    // Return the complete dataset with all required entities
    return {
      sellers: [seller],
      buyers: [buyer], 
      conversations: [conversation],
      messages: messages
    };
  }

  // Generate high-volume streaming data for stress testing
  generateStreamingMessages(conversationIds, count, timeWindow = 60000) {
    const messages = [];
    const startTime = new Date();
    
    for (let i = 0; i < count; i++) {
      const conversationId = faker.helpers.arrayElement(conversationIds);
      const timestamp = new Date(startTime.getTime() + (timeWindow * Math.random()));
      const senderType = faker.helpers.arrayElement(this.senderTypes);
      
      messages.push({
        id: uuidv4(),
        conversation_id: conversationId,
        sender_type: senderType,
        sender_id: uuidv4(), // Random sender for stress test
        message_text: faker.lorem.sentence(),
        message_type: 'text',
        metadata: { stress_test: true },
        timestamp: timestamp
      });
    }
    
    return messages.sort((a, b) => a.timestamp - b.timestamp);
  }

  // Generate data for specific benchmark scenarios
  generateBenchmarkScenario(scenario, options = {}) {
    switch (scenario) {
      case 'high_write_volume':
        return this.generateRealisticConversationBatch(
          options.sellers || 100,
          options.buyers || 1000,
          {
            conversationsPerSeller: 50,
            messagesPerConversation: 200,
            ...options
          }
        );
        
      case 'read_heavy_analytics':
        return this.generateRealisticConversationBatch(
          options.sellers || 50,
          options.buyers || 500,
          {
            conversationsPerSeller: 30,
            messagesPerConversation: 500,
            ...options
          }
        );
        
      case 'mixed_workload':
        return this.generateRealisticConversationBatch(
          options.sellers || 200,
          options.buyers || 2000,
          {
            conversationsPerSeller: 25,
            messagesPerConversation: 150,
            ...options
          }
        );
        
      default:
        return this.generateRealisticConversationBatch(
          options.sellers || 10,
          options.buyers || 100,
          options
        );
    }
  }
}
