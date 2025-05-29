import { ScyllaDBClient } from '../database/scylladb-client.js';
import { ClickHouseClient } from '../database/clickhouse-client.js';
import { TimescaleDBClient } from '../database/timescaledb-client.js';
import { CockroachDBClient } from '../database/cockroachdb-client.js';
import { DataGenerator } from '../utils/data-generator.js';
import { DockerOrchestrator } from '../utils/docker-orchestrator.js';
import { performance } from 'node:perf_hooks';
import fs from 'node:fs/promises';
import path from 'node:path';
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
      mode: 'together', // 'isolated' or 'together'
      scenario: 'custom',
      outputFile: null,
      ...options
    };
    
    // Only initialize clients for specified databases
    this.clients = {};
    this.clientMap = {
      scylladb: ScyllaDBClient,
      clickhouse: ClickHouseClient,
      timescaledb: TimescaleDBClient,
      cockroachdb: CockroachDBClient
    };
    
    this.initializeClients();
    this.dataGenerator = new DataGenerator();
    this.dockerOrchestrator = new DockerOrchestrator();
    this.results = {
      metadata: {
        timestamp: new Date().toISOString(),
        mode: this.options.mode,
        scenario: this.options.scenario,
        databases: this.options.databases,
        configuration: {
          warmupRuns: this.options.warmupRuns,
          benchmarkRuns: this.options.benchmarkRuns,
          batchSize: this.options.batchSize
        }
      },
      databases: {}
    };
  }

  initializeClients() {
    for (const dbName of this.options.databases) {
      if (this.clientMap[dbName]) {
        this.clients[dbName] = new this.clientMap[dbName]();
      } else {
        console.warn(chalk.yellow(`⚠️ Unknown database: ${dbName}`));
      }
    }
    
    if (Object.keys(this.clients).length === 0) {
      throw new Error('No valid databases specified for benchmarking');
    }
  }

  async initialize() {
    console.log(chalk.blue(`🚀 Initializing benchmark environment (${this.options.mode} mode)...`));
    console.log(chalk.gray(`Databases: ${Object.keys(this.clients).join(', ')}`));
    console.log(chalk.gray(`Scenario: ${this.options.scenario}`));
    
    const spinner = ora('Connecting to databases...').start();
    
    try {
      // Connect to all specified databases
      const connectionPromises = Object.entries(this.clients).map(async ([dbName, client]) => {
        try {
          await client.connect();
          console.log(chalk.green(`✅ ${dbName} connected`));
          return { dbName, success: true };
        } catch (error) {
          console.error(chalk.red(`❌ ${dbName} connection failed: ${error.message}`));
          return { dbName, success: false, error: error.message };
        }
      });
      
      const connectionResults = await Promise.all(connectionPromises);
      
      // Remove failed connections from clients
      const failedConnections = connectionResults.filter(result => !result.success);
      for (const failed of failedConnections) {
        delete this.clients[failed.dbName];
        this.results.databases[failed.dbName] = {
          status: 'connection_failed',
          error: failed.error
        };
      }
      
      if (Object.keys(this.clients).length === 0) {
        spinner.fail('No databases connected successfully');
        throw new Error('All database connections failed');
      }
      
      spinner.succeed(`Connected to ${Object.keys(this.clients).length} database(s)`);
      
      // Initialize schemas for connected databases
      await this.initializeSchemas();
      
    } catch (error) {
      spinner.fail('Database initialization failed');
      throw error;
    }
  }

  async initializeSchemas() {
    console.log(chalk.blue('🏗️ Initializing database schemas...'));
    
    const initPromises = Object.entries(this.clients).map(async ([dbName, client]) => {
      try {
        if (typeof client.initializeSchema === 'function') {
          await client.initializeSchema();
          console.log(chalk.green(`✅ ${dbName} schema initialized`));
        }
      } catch (error) {
        console.error(chalk.red(`❌ ${dbName} schema initialization failed: ${error.message}`));
        // Don't remove the client, just log the error
      }
    });
    
    await Promise.all(initPromises);
  }

  async cleanup() {
    console.log(chalk.blue('🧹 Cleaning up database connections...'));
    
    const cleanupPromises = Object.entries(this.clients).map(async ([dbName, client]) => {
      try {
        if (typeof client.disconnect === 'function') {
          await client.disconnect();
          console.log(chalk.gray(`🔌 ${dbName} disconnected`));
        }
      } catch (error) {
        console.error(chalk.yellow(`⚠️ ${dbName} cleanup warning: ${error.message}`));
      }
    });
    
    await Promise.all(cleanupPromises);
  }

  async runBenchmarks() {
    console.log(chalk.blue('🏁 Starting benchmark suite...'));
    
    try {
      await this.initialize();
      
      // Run benchmarks for each connected database
      for (const [dbName, client] of Object.entries(this.clients)) {
        console.log(chalk.cyan(`\n📊 Benchmarking ${dbName.toUpperCase()}...`));
        
        try {
          this.results.databases[dbName] = await this.runDatabaseBenchmark(dbName, client);
          console.log(chalk.green(`✅ ${dbName} benchmarking completed`));
        } catch (error) {
          console.error(chalk.red(`❌ ${dbName} benchmarking failed: ${error.message}`));
          this.results.databases[dbName] = {
            status: 'benchmark_failed',
            error: error.message
          };
        }
      }
      
      // Generate and save results
      await this.generateReport();
      
    } finally {
      await this.cleanup();
    }
    
    return this.results;
  }

  async runDatabaseBenchmark(dbName, client) {
    const dbResults = {
      status: 'completed',
      benchmarks: {}
    };
    
    // Run different benchmark types based on scenario
    const benchmarkTypes = this.getBenchmarkTypes();
    
    for (const benchmarkType of benchmarkTypes) {
      console.log(chalk.yellow(`  🔄 Running ${benchmarkType} benchmark...`));
      
      try {
        dbResults.benchmarks[benchmarkType] = await this.runSpecificBenchmark(
          client,
          benchmarkType,
          dbName
        );
      } catch (error) {
        console.error(chalk.red(`    ❌ ${benchmarkType} failed: ${error.message}`));
        dbResults.benchmarks[benchmarkType] = {
          status: 'failed',
          error: error.message
        };
      }
    }
    
    return dbResults;
  }

  getBenchmarkTypes() {
    const scenarioMap = {
      custom: ['writes', 'reads'],
      high_write_volume: ['writes', 'batch_writes'],
      read_heavy_analytics: ['reads', 'complex_queries'],
      mixed_workload: ['writes', 'reads', 'batch_writes', 'complex_queries']
    };
    
    return scenarioMap[this.options.scenario] || scenarioMap.custom;
  }

  async runSpecificBenchmark(client, benchmarkType, dbName) {
    const results = {
      warmup: [],
      benchmark: [],
      average: 0,
      min: 0,
      max: 0,
      operations_per_second: 0
    };
    
    // Warmup runs
    for (let i = 0; i < this.options.warmupRuns; i++) {
      const warmupTime = await this.executeBenchmarkOperation(client, benchmarkType, true);
      results.warmup.push(warmupTime);
    }
    
    // Actual benchmark runs
    for (let i = 0; i < this.options.benchmarkRuns; i++) {
      const benchmarkTime = await this.executeBenchmarkOperation(client, benchmarkType, false);
      results.benchmark.push(benchmarkTime);
    }
    
    // Calculate statistics
    results.average = results.benchmark.reduce((a, b) => a + b, 0) / results.benchmark.length;
    results.min = Math.min(...results.benchmark);
    results.max = Math.max(...results.benchmark);
    results.operations_per_second = (this.options.batchSize / results.average) * 1000;
    
    return results;
  }

  async executeBenchmarkOperation(client, benchmarkType, isWarmup = false) {
    const startTime = performance.now();
    
    try {
      switch (benchmarkType) {
        case 'writes':
          await this.runWriteBenchmark(client);
          break;
        case 'reads':
          await this.runReadBenchmark(client);
          break;
        case 'batch_writes':
          await this.runBatchWriteBenchmark(client);
          break;
        case 'complex_queries':
          await this.runComplexQueryBenchmark(client);
          break;
        default:
          throw new Error(`Unknown benchmark type: ${benchmarkType}`);
      }
    } catch (error) {
      if (!isWarmup) {
        console.error(chalk.red(`    ⚠️ Operation failed: ${error.message}`));
      }
      throw error;
    }
    
    const endTime = performance.now();
    return endTime - startTime;
  }

  async runWriteBenchmark(client) {
    const testData = this.dataGenerator.generateMessages(this.options.batchSize);
    
    // Create required entities first to satisfy foreign key constraints
    if (testData.sellers && typeof client.createSeller === 'function') {
      for (const seller of testData.sellers) {
        await client.createSeller(seller);
      }
    }
    
    if (testData.buyers && typeof client.createBuyer === 'function') {
      for (const buyer of testData.buyers) {
        await client.createBuyer(buyer);
      }
    }
    
    if (testData.conversations && typeof client.createConversation === 'function') {
      for (const conversation of testData.conversations) {
        await client.createConversation(conversation);
      }
    }
    
    // Now create messages
    for (const message of testData.messages) {
      await client.createMessage(message);
    }
  }

  async runReadBenchmark(client) {
    // Implement read operations based on client capabilities
    if (typeof client.getRecentMessages === 'function') {
      await client.getRecentMessages(100);
    } else if (typeof client.healthCheck === 'function') {
      await client.healthCheck();
    }
  }

  async runBatchWriteBenchmark(client) {
    const testData = this.dataGenerator.generateMessages(this.options.batchSize);
    
    // Create required entities first to satisfy foreign key constraints
    if (testData.sellers && typeof client.createSeller === 'function') {
      for (const seller of testData.sellers) {
        await client.createSeller(seller);
      }
    }
    
    if (testData.buyers && typeof client.createBuyer === 'function') {
      for (const buyer of testData.buyers) {
        await client.createBuyer(buyer);
      }
    }
    
    if (testData.conversations && typeof client.createConversation === 'function') {
      for (const conversation of testData.conversations) {
        await client.createConversation(conversation);
      }
    }
    
    // Now create messages in batch
    if (typeof client.createMessagesBatch === 'function') {
      await client.createMessagesBatch(testData.messages);
    } else {
      // Fallback to individual writes
      for (const message of testData.messages) {
        await client.createMessage(message);
      }
    }
  }

  async runComplexQueryBenchmark(client) {
    // Implement complex queries based on client capabilities
    if (typeof client.getMessagesByTimeRange === 'function') {
      const endTime = new Date();
      const startTime = new Date(endTime.getTime() - 24 * 60 * 60 * 1000); // Last 24 hours
      await client.getMessagesByTimeRange(startTime, endTime);
    } else {
      // Fallback to simple read
      await this.runReadBenchmark(client);
    }
  }

  async generateReport() {
    console.log(chalk.blue('\n📊 Generating benchmark report...'));
    
    // Display results in console
    this.displayResults();
    
    // Save to file if specified
    if (this.options.outputFile) {
      await this.saveResultsToFile();
    }
  }

  displayResults() {
    const table = new Table({
      title: `Benchmark Results - ${this.options.mode.toUpperCase()} Mode`,
      columns: [
        { name: 'database', title: 'Database', alignment: 'left' },
        { name: 'status', title: 'Status', alignment: 'center' },
        { name: 'avg_ops_sec', title: 'Avg Ops/Sec', alignment: 'right' },
        { name: 'best_time', title: 'Best Time (ms)', alignment: 'right' }
      ]
    });
    
    for (const [dbName, dbResult] of Object.entries(this.results.databases)) {
      if (dbResult.status === 'completed' && dbResult.benchmarks) {
        const avgOpsPerSec = Object.values(dbResult.benchmarks)
          .filter(b => b.operations_per_second)
          .reduce((sum, b) => sum + b.operations_per_second, 0) / 
          Object.keys(dbResult.benchmarks).length;
        
        const bestTime = Math.min(
          ...Object.values(dbResult.benchmarks)
            .filter(b => b.min)
            .map(b => b.min)
        );
        
        table.addRow({
          database: dbName.toUpperCase(),
          status: '✅ Completed',
          avg_ops_sec: Math.round(avgOpsPerSec).toLocaleString(),
          best_time: Math.round(bestTime)
        });
      } else {
        table.addRow({
          database: dbName.toUpperCase(),
          status: '❌ Failed',
          avg_ops_sec: 'N/A',
          best_time: 'N/A'
        });
      }
    }
    
    table.printTable();
  }

  async saveResultsToFile() {
    try {
      // Ensure reports directory exists
      const reportsDir = path.join(process.cwd(), 'reports');
      await fs.mkdir(reportsDir, { recursive: true });
      
      // Generate filename if not provided
      let filename = this.options.outputFile;
      if (!filename) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        filename = `benchmark-${this.options.mode}-${this.options.scenario}-${timestamp}.json`;
      }
      
      // Ensure .json extension
      if (!filename.endsWith('.json')) {
        filename += '.json';
      }
      
      const filepath = path.join(reportsDir, filename);
      
      await fs.writeFile(filepath, JSON.stringify(this.results, null, 2));
      
      console.log(chalk.green(`📄 Results saved to: ${filepath}`));
      
    } catch (error) {
      console.error(chalk.red(`❌ Failed to save results: ${error.message}`));
    }
  }

  async runIsolatedBenchmarks() {
    console.log(chalk.blue('🚀 Starting Isolated Database Benchmark Suite'));
    console.log(chalk.gray(`Scenario: ${this.options.scenario}`));
    console.log(chalk.gray(`Databases: ${this.options.databases.join(', ')}`));
    console.log(chalk.gray('Mode: ISOLATED (one database at a time)'));
    
    const allResults = {
      metadata: {
        ...this.results.metadata,
        mode: 'isolated'
      },
      databases: {}
    };

    for (const database of this.options.databases) {
      console.log(chalk.cyan(`\n🔄 Testing ${database.toUpperCase()} in isolation...`));
      
      try {
        // Stop other databases and clean up
        await this.prepareIsolatedEnvironment(database);
        
        // Run benchmark for this database only
        const dbResult = await this.runSingleDatabaseBenchmark(database);
        allResults.databases[database] = dbResult;
        
        console.log(chalk.green(`✅ ${database.toUpperCase()} completed successfully`));
        
      } catch (error) {
        console.error(chalk.red(`❌ ${database.toUpperCase()} failed: ${error.message}`));
        allResults.databases[database] = {
          status: 'failed',
          error: error.message
        };
      }
    }
    
    // Save consolidated results
    if (this.options.outputFile) {
      await this.saveConsolidatedResults(allResults);
    }
    
    // Display final summary
    this.displayIsolatedSummary(allResults);
    
    return allResults;
  }

  async prepareIsolatedEnvironment(targetDatabase) {
    console.log(chalk.yellow(`🛠️ Preparing isolated environment for ${targetDatabase.toUpperCase()}...`));
    
    // Clean up Docker volumes to ensure fresh state
    await this.dockerOrchestrator.cleanupDockerVolumes();
    
    // Ensure the target database is ready (it should already be running from startIsolatedDatabases)
    await this.dockerOrchestrator.waitForDatabaseReady(targetDatabase);
  }

  async runSingleDatabaseBenchmark(database) {
    // Create a temporary runner for just this database
    const singleDbRunner = new BenchmarkRunner({
      ...this.options,
      databases: [database],
      mode: 'isolated'
    });
    
    try {
      // Run the benchmark
      const results = await singleDbRunner.runBenchmarks();
      return results.databases[database];
      
    } finally {
      // Ensure cleanup
      await singleDbRunner.cleanup();
    }
  }

  async cleanupDatabaseData(database) {
    console.log(chalk.yellow(`🧹 Cleaning up ${database.toUpperCase()} data...`));
    
    try {
      const ClientClass = this.clientMap[database];
      if (!ClientClass) return;
      
      const client = new ClientClass();
      await client.connect();
      
      // Clean up previous test data
      if (typeof client.truncateAllTables === 'function') {
        await client.truncateAllTables();
      }
      
      await client.disconnect();
      
    } catch (error) {
      console.log(chalk.gray(`⚠️ Cleanup warning for ${database}: ${error.message}`));
    }
  }

  async saveConsolidatedResults(results) {
    try {
      const reportsDir = path.join(process.cwd(), 'reports');
      await fs.mkdir(reportsDir, { recursive: true });
      
      let filename = this.options.outputFile;
      if (!filename.endsWith('.json')) {
        filename += '.json';
      }
      
      const filepath = path.join(reportsDir, filename);
      await fs.writeFile(filepath, JSON.stringify(results, null, 2));
      
      console.log(chalk.green(`\n📄 Results saved to: ${filepath}`));
      
    } catch (error) {
      console.error(chalk.red(`❌ Failed to save results: ${error.message}`));
    }
  }

  displayIsolatedSummary(results) {
    console.log(chalk.blue('\n📊 Final Summary:'));
    
    const successful = [];
    const failed = [];
    
    for (const [dbName, dbResult] of Object.entries(results.databases)) {
      if (dbResult.status === 'completed') {
        successful.push(dbName);
      } else {
        failed.push(dbName);
      }
    }
    
    if (successful.length > 0) {
      console.log(chalk.green(`✅ Successful: ${successful.join(', ')}`));
    }
    
    if (failed.length > 0) {
      console.log(chalk.red(`❌ Failed: ${failed.join(', ')}`));
    }
    
    console.log(chalk.gray(`\n📈 Total databases tested: ${Object.keys(results.databases).length}`));
    console.log(chalk.gray(`🕒 Completed at: ${new Date().toISOString()}`));
  }
}
