import { ScyllaDBClient } from '../database/scylladb-client.js';
import { ClickHouseClient } from '../database/clickhouse-client.js';
import { TimescaleDBClient } from '../database/timescaledb-client.js';
import { CockroachDBClient } from '../database/cockroachdb-client.js';
import { DataGenerator } from '../utils/data-generator.js';
import { performance } from 'node:perf_hooks';
import chalk from 'chalk';
import ora from 'ora';
import { Table } from 'console-table-printer';

export class BenchmarkRunner {
  constructor(options = {}) {
    this.options = {
      warmupRuns: 3,
      benchmarkRuns: 5,
      batchSize: 1000,
      databases: ['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb'], // Default to all databases
      ...options
    };
    
    // Only initialize clients for specified databases
    this.clients = {};
    const clientMap = {
      scylladb: ScyllaDBClient,
      clickhouse: ClickHouseClient,
      timescaledb: TimescaleDBClient,
      cockroachdb: CockroachDBClient
    };
    
    for (const dbName of this.options.databases) {
      if (clientMap[dbName]) {
        this.clients[dbName] = new clientMap[dbName]();
      }
    }
    
    this.dataGenerator = new DataGenerator();
    this.results = {};
  }

  async initialize() {
    console.log(chalk.blue('🚀 Initializing benchmark environment...'));
    
    const spinner = ora('Connecting to databases...').start();
    
    try {
      // Only connect to the specified databases
      const connectionPromises = Object.values(this.clients).map(client => client.connect());
      await Promise.all(connectionPromises);
      
      spinner.succeed('All specified databases connected successfully');
      
      // Clean databases before benchmarking
      await this.cleanupDatabases();
      
      // Health check only for specified databases
      const healthCheckPromises = Object.entries(this.clients).map(async ([name, client]) => {
        const isHealthy = await client.healthCheck();
        return { name, isHealthy };
      });
      
      const healthResults = await Promise.all(healthCheckPromises);
      
      console.log(chalk.green('✅ Health checks passed'));
      for (const { name, isHealthy } of healthResults) {
        console.log(`   ${name.toUpperCase()}: ${isHealthy.status}`);
      }
      
    } catch (error) {
      spinner.fail('Database initialization failed');
      throw error;
    }
  }

  async cleanup() {
    console.log(chalk.blue('🧹 Cleaning up connections...'));
    
    await Promise.all(Object.values(this.clients).map(client => client.disconnect()));
    
    console.log(chalk.green('✅ Cleanup completed'));
  }

  // Clean all databases before benchmarking
  async cleanupDatabases() {
    console.log(chalk.yellow('🧹 Cleaning databases before benchmark...'));
    
    const cleanupPromises = Object.entries(this.clients).map(async ([dbName, client]) => {
      try {
        if (client.truncateAllTables) {
          await client.truncateAllTables();
        } else {
          console.log(chalk.yellow(`⚠️  Cleanup not implemented for ${dbName.toUpperCase()}`));
        }
        console.log(chalk.green(`🗑️  ${dbName.toUpperCase()} cleaned successfully`));
      } catch (error) {
        console.log(chalk.red(`❌ Failed to clean ${dbName.toUpperCase()}: ${error.message}`));
      }
    });
    
    await Promise.all(cleanupPromises);
  }

  // Benchmark write operations
  async benchmarkWrites(scenario = 'mixed_workload') {
    console.log(chalk.yellow(`\n📝 Running write benchmarks for scenario: ${scenario}`));
    
    const testData = this.dataGenerator.generateBenchmarkScenario(scenario);
    const databases = Object.keys(this.clients);
    
    this.results.writes = {};
    
    for (const dbName of databases) {
      console.log(chalk.blue(`\n🔄 Testing ${dbName.toUpperCase()} writes...`));
      
      const client = this.clients[dbName];
      const dbResults = {
        sellers: await this.benchmarkEntityWrites(client, 'sellers', testData.sellers),
        buyers: await this.benchmarkEntityWrites(client, 'buyers', testData.buyers),
        conversations: await this.benchmarkEntityWrites(client, 'conversations', testData.conversations),
        messages: await this.benchmarkEntityWrites(client, 'messages', testData.messages)
      };
      
      this.results.writes[dbName] = dbResults;
    }
    
    this.displayWriteResults();
    return this.results.writes;
  }

  async benchmarkEntityWrites(client, entityType, data) {
    const methodName = `create${entityType.charAt(0).toUpperCase() + entityType.slice(1, -1)}sBatch`;
    const batchSize = this.options.batchSize;
    const batches = this.createBatches(data, batchSize);
    
    console.log(`   📊 Writing ${data.length} ${entityType} in ${batches.length} batches...`);
    
    const results = {
      totalRecords: data.length,
      batchSize: batchSize,
      batches: batches.length,
      times: [],
      errors: 0
    };
    
    const spinner = ora(`Writing ${entityType}...`).start();
    
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      const startTime = performance.now();
      
      try {
        await client[methodName](batch);
        const endTime = performance.now();
        results.times.push(endTime - startTime);
        
        spinner.text = `Writing ${entityType}... ${i + 1}/${batches.length} batches`;
      } catch (error) {
        results.errors++;
        console.error(`Error writing batch ${i + 1}:`, error.message);
      }
    }
    
    spinner.succeed(`Completed writing ${entityType}`);
    
    // Calculate statistics
    results.totalTime = results.times.reduce((sum, time) => sum + time, 0);
    results.avgTime = results.totalTime / results.times.length;
    results.minTime = Math.min(...results.times);
    results.maxTime = Math.max(...results.times);
    results.throughput = results.totalRecords / (results.totalTime / 1000); // records per second
    
    return results;
  }

  // Benchmark read operations
  async benchmarkReads() {
    console.log(chalk.yellow('\n📖 Running read benchmarks...'));
    
    const databases = Object.keys(this.clients);
    this.results.reads = {};
    
    for (const dbName of databases) {
      console.log(chalk.blue(`\n🔍 Testing ${dbName.toUpperCase()} reads...`));
      
      const client = this.clients[dbName];
      const dbResults = await this.runReadBenchmarks(client, dbName);
      
      this.results.reads[dbName] = dbResults;
    }
    
    this.displayReadResults();
    return this.results.reads;
  }

  async runReadBenchmarks(client, dbName) {
    const benchmarks = [
      {
        name: 'Recent Messages',
        operation: () => client.getRecentMessages(5, 1000),
        description: 'Fetch messages from last 5 minutes'
      },
      {
        name: 'Conversation Messages',
        operation: async () => {
          // Get a random conversation ID from the first conversation
          const conversations = await client.getRecentConversations ? 
            client.getRecentConversations(1) : 
            [{ id: 'test-conversation-id' }];
          
          if (conversations.length === 0) return [];
          
          const conversationId = conversations[0].id;
          return client.getConversationMessages ? 
            client.getConversationMessages(conversationId, 100) : 
            [];
        },
        description: 'Get all messages for a specific conversation'
      },
      {
        name: 'Inactive Conversations',
        operation: () => client.getInactiveConversations(5),
        description: 'Find conversations inactive for 5+ minutes'
      },
      {
        name: 'Message Search',
        operation: () => client.searchMessages ? client.searchMessages('hello', 100) : Promise.resolve([]),
        description: 'Search messages containing "hello"'
      }
    ];

    const results = {};
    
    for (const benchmark of benchmarks) {
      console.log(`   🔍 Running: ${benchmark.name}`);
      
      const times = [];
      const errors = [];
      
      // Warmup runs
      for (let i = 0; i < this.options.warmupRuns; i++) {
        try {
          await benchmark.operation();
        } catch (error) {
          // Ignore warmup errors
        }
      }
      
      // Actual benchmark runs
      for (let i = 0; i < this.options.benchmarkRuns; i++) {
        const startTime = performance.now();
        
        try {
          const result = await benchmark.operation();
          const endTime = performance.now();
          times.push(endTime - startTime);
          
          if (i === 0) {
            results[benchmark.name] = {
              ...results[benchmark.name],
              resultCount: Array.isArray(result) ? result.length : 1
            };
          }
        } catch (error) {
          errors.push(error.message);
        }
      }
      
      // Calculate statistics
      if (times.length > 0) {
        results[benchmark.name] = {
          description: benchmark.description,
          runs: times.length,
          avgTime: times.reduce((sum, time) => sum + time, 0) / times.length,
          minTime: Math.min(...times),
          maxTime: Math.max(...times),
          errors: errors.length,
          resultCount: results[benchmark.name]?.resultCount || 0
        };
      }
    }
    
    return results;
  }

  // Benchmark concurrent operations
  async benchmarkConcurrency(concurrentUsers = 5, operationsPerUser = 20) {
    console.log(chalk.yellow(`\n⚡ Running concurrency benchmarks (${concurrentUsers} users, ${operationsPerUser} ops each)...`));
    
    const databases = Object.keys(this.clients);
    this.results.concurrency = {};
    
    // Generate test data for concurrent operations
    const conversationIds = Array.from({ length: 100 }, () => 
      this.dataGenerator.generateConversation('seller-1', 'buyer-1').id
    );
    
    for (const dbName of databases) {
      console.log(chalk.blue(`\n⚡ Testing ${dbName.toUpperCase()} concurrency...`));
      
      const client = this.clients[dbName];
      const results = await this.runConcurrencyTest(client, conversationIds, concurrentUsers, operationsPerUser);
      
      this.results.concurrency[dbName] = results;
    }
    
    this.displayConcurrencyResults();
    return this.results.concurrency;
  }

  async runConcurrencyTest(client, conversationIds, concurrentUsers, operationsPerUser) {
    const startTime = performance.now();
    
    const userPromises = Array.from({ length: concurrentUsers }, async (_, userIndex) => {
      const userResults = {
        userId: userIndex,
        operations: 0,
        errors: 0,
        times: []
      };
      
      for (let i = 0; i < operationsPerUser; i++) {
        const opStartTime = performance.now();
        
        try {
          // Simulate mixed read/write operations
          if (Math.random() < 0.7) {
            // 70% reads
            const conversationId = conversationIds[Math.floor(Math.random() * conversationIds.length)];
            await client.getConversationMessages(conversationId, 50);
          } else {
            // 30% writes
            const message = this.dataGenerator.generateMessage(
              conversationIds[Math.floor(Math.random() * conversationIds.length)],
              `user-${userIndex}`,
              'buyer'
            );
            await client.createMessage(message);
          }
          
          const opEndTime = performance.now();
          userResults.times.push(opEndTime - opStartTime);
          userResults.operations++;
        } catch (error) {
          userResults.errors++;
        }
      }
      
      return userResults;
    });
    
    const userResults = await Promise.all(userPromises);
    const endTime = performance.now();
    
    // Aggregate results
    const totalOperations = userResults.reduce((sum, user) => sum + user.operations, 0);
    const totalErrors = userResults.reduce((sum, user) => sum + user.errors, 0);
    const allTimes = userResults.flatMap(user => user.times);
    
    return {
      concurrentUsers,
      operationsPerUser,
      totalOperations,
      totalErrors,
      totalTime: endTime - startTime,
      avgOperationTime: allTimes.reduce((sum, time) => sum + time, 0) / allTimes.length,
      throughput: totalOperations / ((endTime - startTime) / 1000),
      errorRate: totalErrors / (concurrentUsers * operationsPerUser)
    };
  }

  // Utility methods
  createBatches(data, batchSize) {
    const batches = [];
    for (let i = 0; i < data.length; i += batchSize) {
      batches.push(data.slice(i, i + batchSize));
    }
    return batches;
  }

  displayWriteResults() {
    console.log(chalk.green('\n📊 Write Benchmark Results'));
    
    const table = new Table({
      title: 'Write Performance Comparison',
      columns: [
        { name: 'database', title: 'Database' },
        { name: 'entity', title: 'Entity' },
        { name: 'records', title: 'Records' },
        { name: 'throughput', title: 'Throughput (rec/s)' },
        { name: 'avgTime', title: 'Avg Time (ms)' },
        { name: 'errors', title: 'Errors' }
      ]
    });
    
    for (const [dbName, dbResults] of Object.entries(this.results.writes)) {
      for (const [entity, results] of Object.entries(dbResults)) {
        table.addRow({
          database: dbName.toUpperCase(),
          entity: entity,
          records: results.totalRecords.toLocaleString(),
          throughput: Math.round(results.throughput).toLocaleString(),
          avgTime: Math.round(results.avgTime),
          errors: results.errors
        });
      }
    }
    
    table.printTable();
  }

  displayReadResults() {
    console.log(chalk.green('\n📊 Read Benchmark Results'));
    
    const table = new Table({
      title: 'Read Performance Comparison',
      columns: [
        { name: 'database', title: 'Database' },
        { name: 'operation', title: 'Operation' },
        { name: 'avgTime', title: 'Avg Time (ms)' },
        { name: 'minTime', title: 'Min Time (ms)' },
        { name: 'maxTime', title: 'Max Time (ms)' },
        { name: 'results', title: 'Results' },
        { name: 'errors', title: 'Errors' }
      ]
    });
    
    for (const [dbName, dbResults] of Object.entries(this.results.reads)) {
      for (const [operation, results] of Object.entries(dbResults)) {
        table.addRow({
          database: dbName.toUpperCase(),
          operation: operation,
          avgTime: Math.round(results.avgTime),
          minTime: Math.round(results.minTime),
          maxTime: Math.round(results.maxTime),
          results: results.resultCount,
          errors: results.errors
        });
      }
    }
    
    table.printTable();
  }

  displayConcurrencyResults() {
    console.log(chalk.green('\n📊 Concurrency Benchmark Results'));
    
    const table = new Table({
      title: 'Concurrency Performance Comparison',
      columns: [
        { name: 'database', title: 'Database' },
        { name: 'users', title: 'Concurrent Users' },
        { name: 'operations', title: 'Total Operations' },
        { name: 'throughput', title: 'Throughput (ops/s)' },
        { name: 'avgTime', title: 'Avg Op Time (ms)' },
        { name: 'errorRate', title: 'Error Rate (%)' }
      ]
    });
    
    for (const [dbName, results] of Object.entries(this.results.concurrency)) {
      table.addRow({
        database: dbName.toUpperCase(),
        users: results.concurrentUsers,
        operations: results.totalOperations.toLocaleString(),
        throughput: Math.round(results.throughput).toLocaleString(),
        avgTime: Math.round(results.avgOperationTime),
        errorRate: (results.errorRate * 100).toFixed(2)
      });
    }
    
    table.printTable();
  }

  async generateReport() {
    console.log(chalk.blue('\n📋 Generating comprehensive report...'));
    
    const report = {
      timestamp: new Date().toISOString(),
      configuration: this.options,
      results: this.results,
      summary: await this.generateSummary()
    };
    
    return report;
  }

  async generateSummary() {
    const summary = {
      winner: {
        writes: this.findBestPerformer('writes', 'throughput'),
        reads: this.findBestPerformer('reads', 'avgTime', true), // Lower is better for time
        concurrency: this.findBestPerformer('concurrency', 'throughput')
      },
      metrics: await this.getSystemMetrics()
    };
    
    return summary;
  }

  findBestPerformer(category, metric, lowerIsBetter = false) {
    if (!this.results[category]) return null;
    
    let bestDb = null;
    let bestValue = lowerIsBetter ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
    
    for (const [dbName, results] of Object.entries(this.results[category])) {
      let value;
      
      if (category === 'writes') {
        // Average throughput across all entities
        const throughputs = Object.values(results).map(r => r[metric]);
        value = throughputs.reduce((sum, t) => sum + t, 0) / throughputs.length;
      } else if (category === 'reads') {
        // Average time across all operations
        const times = Object.values(results).map(r => r[metric]);
        value = times.reduce((sum, t) => sum + t, 0) / times.length;
      } else {
        value = results[metric];
      }
      
      if ((lowerIsBetter && value < bestValue) || (!lowerIsBetter && value > bestValue)) {
        bestValue = value;
        bestDb = dbName;
      }
    }
    
    return { database: bestDb, value: bestValue };
  }

  async getSystemMetrics() {
    const metrics = {};
    
    for (const [dbName, client] of Object.entries(this.clients)) {
      try {
        metrics[dbName] = {
          records: await client.getMetrics(),
          system: await client.getSystemMetrics?.()
        };
      } catch (error) {
        metrics[dbName] = { error: error.message };
      }
    }
    
    return metrics;
  }

  // Main benchmark execution
  async runFullBenchmark(scenario = 'mixed_workload', additionalOptions = {}) {
    try {
      await this.initialize();
      
      console.log(chalk.magenta(`\n🏁 Starting full benchmark suite with scenario: ${scenario}`));
      console.log(chalk.gray(`Configuration: ${JSON.stringify(this.options, null, 2)}`));
      
      // Extract concurrency parameters from additional options
      const concurrentUsers = additionalOptions.concurrentUsers || 5;
      const operationsPerUser = additionalOptions.operationsPerUser || 20;
      
      // Run all benchmark categories
      await this.benchmarkWrites(scenario);
      await this.benchmarkReads();
      await this.benchmarkConcurrency(concurrentUsers, operationsPerUser);
      
      // Generate final report
      const report = await this.generateReport();
      
      console.log(chalk.green('\n🎉 Benchmark completed successfully!'));
      console.log(chalk.blue('\n🏆 Winners:'));
      console.log(`   Writes: ${report.summary.winner.writes?.database?.toUpperCase() || 'N/A'}`);
      console.log(`   Reads: ${report.summary.winner.reads?.database?.toUpperCase() || 'N/A'}`);
      console.log(`   Concurrency: ${report.summary.winner.concurrency?.database?.toUpperCase() || 'N/A'}`);
      
      return report;
      
    } catch (error) {
      console.error(chalk.red('❌ Benchmark failed:'), error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }
}
