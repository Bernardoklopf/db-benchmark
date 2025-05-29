#!/usr/bin/env node

import chalk from 'chalk';
import dotenv from 'dotenv';
import { ScyllaDBClient } from './src/database/scylladb-client.js';
import { ClickHouseClient } from './src/database/clickhouse-client.js';
import { TimescaleDBClient } from './src/database/timescaledb-client.js';
import { DataGenerator } from './src/utils/data-generator.js';

// Load environment variables
dotenv.config();

async function testSetup() {
  console.log(chalk.blue('🧪 Testing WhatsApp Database Benchmark Setup\n'));

  const clients = {
    ScyllaDB: new ScyllaDBClient(),
    ClickHouse: new ClickHouseClient(),
    TimescaleDB: new TimescaleDBClient()
  };

  let allHealthy = true;

  // Initialize schemas first
  console.log(chalk.blue('🏗️  Initializing database schemas...\n'));
  try {
    await clients.ScyllaDB.initializeSchema();
    console.log(chalk.green('✅ ScyllaDB schema initialized'));
  } catch (error) {
    console.error(chalk.red('❌ ScyllaDB schema initialization failed:'), error.message);
    allHealthy = false;
  }

  // Test database connections
  for (const [name, client] of Object.entries(clients)) {
    try {
      console.log(chalk.blue(`🔍 Testing ${name} connection...`));
      await client.connect();
      
      const health = await client.healthCheck();
      console.log(chalk.green(`✅ ${name}: ${health.status}`));
      
      await client.disconnect();
    } catch (error) {
      console.log(chalk.red(`❌ ${name}: ${error.message}`));
      allHealthy = false;
    }
  }

  if (!allHealthy) {
    console.log(chalk.red('\n❌ Some databases are not healthy. Please check Docker containers.'));
    console.log(chalk.yellow('💡 Try: npm run docker:up && sleep 30'));
    process.exit(1);
  }

  // Test data generation
  console.log(chalk.blue('\n🏭 Testing data generation...'));
  try {
    const generator = new DataGenerator();
    const testData = generator.generateRealisticConversationBatch(2, 10, {
      conversationsPerSeller: 2,
      messagesPerConversation: 5
    });
    
    console.log(chalk.green('✅ Generated test data:'));
    console.log(`   - ${testData.sellers.length} sellers`);
    console.log(`   - ${testData.buyers.length} buyers`);
    console.log(`   - ${testData.conversations.length} conversations`);
    console.log(`   - ${testData.messages.length} messages`);
  } catch (error) {
    console.log(chalk.red(`❌ Data generation failed: ${error.message}`));
    process.exit(1);
  }

  // Test small data insertion
  console.log(chalk.blue('\n📊 Testing small data insertion...'));
  
  const generator = new DataGenerator();
  const smallData = generator.generateRealisticConversationBatch(1, 5, {
    conversationsPerSeller: 1,
    messagesPerConversation: 3
  });

  for (const [name, client] of Object.entries(clients)) {
    try {
      console.log(chalk.blue(`   Testing ${name} insertion...`));
      await client.connect();
      
      await client.createSellersBatch(smallData.sellers);
      await client.createBuyersBatch(smallData.buyers);
      await client.createConversationsBatch(smallData.conversations);
      await client.createMessagesBatch(smallData.messages);
      
      const metrics = await client.getMetrics();
      console.log(chalk.green(`   ✅ ${name}: Inserted ${metrics.messages || 'N/A'} messages`));
      
      await client.disconnect();
    } catch (error) {
      console.log(chalk.red(`   ❌ ${name}: ${error.message}`));
    }
  }

  console.log(chalk.green('\n🎉 Setup test completed successfully!'));
  console.log(chalk.blue('\n🚀 Ready to run benchmarks:'));
  console.log('   npm start                    # Full benchmark suite');
  console.log('   npm run benchmark:writes     # Write performance only');
  console.log('   npm run health               # Check database status');
  console.log('   node src/cli.js --help       # View all options');
}

// Error handling
process.on('unhandledRejection', (error) => {
  console.error(chalk.red('❌ Unhandled promise rejection:'), error.message);
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  console.error(chalk.red('❌ Uncaught exception:'), error.message);
  process.exit(1);
});

// Run the test
testSetup().catch(error => {
  console.error(chalk.red('❌ Setup test failed:'), error.message);
  process.exit(1);
});
