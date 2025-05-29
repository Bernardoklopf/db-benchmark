#!/usr/bin/env node

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import chalk from 'chalk';
import { Command } from 'commander';
import { BenchmarkRunner } from './benchmark/benchmark-runner.js';
import { DataGenerator } from './utils/data-generator.js';
import { DockerOrchestrator } from './utils/docker-orchestrator.js';
import { ScyllaDBClient } from './database/scylladb-client.js';
import { ClickHouseClient } from './database/clickhouse-client.js';
import { TimescaleDBClient } from './database/timescaledb-client.js';
import { CockroachDBClient } from './database/cockroachdb-client.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const program = new Command();
const dockerOrchestrator = new DockerOrchestrator();

program
  .name('whatsapp-benchmark')
  .description('WhatsApp Database Benchmarking Tool for ScyllaDB, ClickHouse, TimescaleDB, and CockroachDB')
  .version('1.0.0');

// Altogether benchmark command (all databases together)
program
  .command('benchmark-together')
  .description('Run benchmarks with all databases together')
  .option('-s, --scenario <type>', 'Benchmark scenario', 'mixed_workload')
  .option('-b, --batch-size <size>', 'Batch size for operations', '1000')
  .option('-w, --warmup-runs <runs>', 'Number of warmup runs', '3')
  .option('-r, --benchmark-runs <runs>', 'Number of benchmark runs', '5')
  .option('-c, --concurrent-users <users>', 'Number of concurrent users for concurrency test', '5')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .option('--skip-docker', 'Skip Docker setup (assume containers are running)')
  .action(async (options) => {
    try {
      console.log(chalk.blue('🚀 Starting Altogether Database Benchmark Suite'));
      console.log(chalk.gray(`Scenario: ${options.scenario}`));
      
      if (!options.skipDocker) {
        await dockerOrchestrator.startAllDatabases();
      }
      
      const benchmarkOptions = {
        batchSize: Number.parseInt(options.batchSize),
        warmupRuns: Number.parseInt(options.warmupRuns),
        benchmarkRuns: Number.parseInt(options.benchmarkRuns),
        databases: ['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb']
      };
      
      const runner = new BenchmarkRunner(benchmarkOptions);
      const report = await runner.runFullBenchmark(options.scenario, {
        concurrentUsers: Number.parseInt(options.concurrentUsers)
      });
      
      report.benchmarkType = 'altogether';
      report.timestamp = new Date().toISOString();
      
      if (options.output) {
        await saveReport(report, options.output);
        console.log(chalk.green(`📄 Report saved to: ${options.output}`));
      }
      
      if (!options.skipDocker) {
        await dockerOrchestrator.stopAllDatabases();
      }
      
    } catch (error) {
      console.error(chalk.red('❌ Altogether benchmark failed:'), error.message);
      if (!options.skipDocker) {
        await dockerOrchestrator.stopAllDatabases();
      }
      process.exit(1);
    }
  });

// Isolated benchmark command (one database at a time)
program
  .command('benchmark-isolated')
  .description('Run benchmarks with each database in isolation')
  .option('-d, --databases <databases>', 'Comma-separated list of databases to test', 'scylladb,clickhouse,timescaledb,cockroachdb')
  .option('-s, --scenario <type>', 'Benchmark scenario', 'mixed_workload')
  .option('-b, --batch-size <size>', 'Batch size for operations', '1000')
  .option('-w, --warmup-runs <runs>', 'Number of warmup runs', '3')
  .option('-r, --benchmark-runs <runs>', 'Number of benchmark runs', '5')
  .option('-c, --concurrent-users <users>', 'Number of concurrent users for concurrency test', '5')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .option('--skip-docker', 'Skip Docker setup (assume containers are running)')
  .action(async (options) => {
    try {
      const databases = options.databases.split(',').map(db => db.trim());
      console.log(chalk.blue('🚀 Starting Isolated Database Benchmark Suite'));
      console.log(chalk.gray(`Databases: ${databases.join(', ')}`));
      console.log(chalk.gray(`Scenario: ${options.scenario}`));
      
      const isolatedResults = {
        benchmarkType: 'isolated',
        timestamp: new Date().toISOString(),
        scenario: options.scenario,
        databases: {}
      };
      
      for (const database of databases) {
        console.log(chalk.blue(`\n🔍 Testing ${database.toUpperCase()} in isolation...`));
        
        if (!options.skipDocker) {
          await dockerOrchestrator.startIsolatedDatabase(database);
        }
        
        try {
          const benchmarkOptions = {
            batchSize: Number.parseInt(options.batchSize),
            warmupRuns: Number.parseInt(options.warmupRuns),
            benchmarkRuns: Number.parseInt(options.benchmarkRuns),
            databases: [database]
          };
          
          const runner = new BenchmarkRunner(benchmarkOptions);
          const report = await runner.runFullBenchmark(options.scenario, {
            concurrentUsers: Number.parseInt(options.concurrentUsers)
          });
          
          isolatedResults.databases[database] = {
            ...report,
            isolatedTest: true,
            timestamp: new Date().toISOString()
          };
          
          console.log(chalk.green(`✅ ${database.toUpperCase()} benchmark completed`));
          
        } catch (error) {
          console.error(chalk.red(`❌ ${database.toUpperCase()} benchmark failed:`), error.message);
          isolatedResults.databases[database] = {
            error: error.message,
            timestamp: new Date().toISOString()
          };
        }
        
        if (!options.skipDocker) {
          await dockerOrchestrator.stopIsolatedDatabase(database);
        }
      }
      
      if (options.output) {
        await saveReport(isolatedResults, options.output);
        console.log(chalk.green(`📄 Isolated benchmark report saved to: ${options.output}`));
      }
      
      // Print summary
      console.log(chalk.blue('\n📊 Isolated Benchmark Summary:'));
      for (const [db, result] of Object.entries(isolatedResults.databases)) {
        if (result.error) {
          console.log(chalk.red(`   ${db.toUpperCase()}: FAILED - ${result.error}`));
        } else {
          console.log(chalk.green(`   ${db.toUpperCase()}: SUCCESS`));
        }
      }
      
    } catch (error) {
      console.error(chalk.red('❌ Isolated benchmark suite failed:'), error.message);
      await dockerOrchestrator.cleanupAllContainers();
      process.exit(1);
    }
  });

// Legacy full benchmark command (for backward compatibility)
program
  .command('benchmark')
  .description('Run full benchmark suite (legacy - use benchmark-together or benchmark-isolated)')
  .option('-s, --scenario <type>', 'Benchmark scenario', 'mixed_workload')
  .option('-b, --batch-size <size>', 'Batch size for operations', '1000')
  .option('-w, --warmup-runs <runs>', 'Number of warmup runs', '3')
  .option('-r, --benchmark-runs <runs>', 'Number of benchmark runs', '5')
  .option('-c, --concurrent-users <users>', 'Number of concurrent users for concurrency test', '5')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .action(async (options) => {
    console.log(chalk.yellow('⚠️  Using legacy benchmark command. Consider using:'));
    console.log(chalk.yellow('   benchmark-together: Test all databases together'));
    console.log(chalk.yellow('   benchmark-isolated: Test each database in isolation'));
    
    // Default to altogether benchmark for backward compatibility
    const togetherOptions = { ...options, skipDocker: false };
    await program.commands.find(cmd => cmd.name() === 'benchmark-together').action(togetherOptions);
  });

// Write-only benchmark
program
  .command('benchmark-writes')
  .description('Run write performance benchmarks only')
  .option('-s, --scenario <type>', 'Benchmark scenario', 'mixed_workload')
  .option('-b, --batch-size <size>', 'Batch size for operations', '1000')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .action(async (options) => {
    try {
      console.log(chalk.blue('📝 Running Write Benchmarks'));
      
      const runner = new BenchmarkRunner({ batchSize: Number.parseInt(options.batchSize) });
      await runner.initialize();
      
      const results = await runner.benchmarkWrites(options.scenario);
      
      if (options.output) {
        await saveReport({ writes: results }, options.output);
        console.log(chalk.green(`📄 Results saved to: ${options.output}`));
      }
      
      await runner.cleanup();
      
    } catch (error) {
      console.error(chalk.red('❌ Write benchmark failed:'), error.message);
      process.exit(1);
    }
  });

// Read-only benchmark
program
  .command('benchmark-reads')
  .description('Run read performance benchmarks only')
  .option('-w, --warmup-runs <runs>', 'Number of warmup runs', '3')
  .option('-r, --benchmark-runs <runs>', 'Number of benchmark runs', '5')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .action(async (options) => {
    try {
      console.log(chalk.blue('📖 Running Read Benchmarks'));
      
      const runner = new BenchmarkRunner({
        warmupRuns: Number.parseInt(options.warmupRuns),
        benchmarkRuns: Number.parseInt(options.benchmarkRuns)
      });
      
      await runner.initialize();
      const results = await runner.benchmarkReads();
      
      if (options.output) {
        await saveReport({ reads: results }, options.output);
        console.log(chalk.green(`📄 Results saved to: ${options.output}`));
      }
      
      await runner.cleanup();
      
    } catch (error) {
      console.error(chalk.red('❌ Read benchmark failed:'), error.message);
      process.exit(1);
    }
  });

// Concurrency benchmark
program
  .command('benchmark-concurrency')
  .description('Run concurrency benchmarks only')
  .option('-u, --users <users>', 'Number of concurrent users', '10')
  .option('-ops, --operations <ops>', 'Operations per user', '100')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .action(async (options) => {
    try {
      console.log(chalk.blue('⚡ Running Concurrency Benchmarks'));
      
      const runner = new BenchmarkRunner();
      await runner.initialize();
      
      const results = await runner.benchmarkConcurrency(
        Number.parseInt(options.users),
        Number.parseInt(options.operations)
      );
      
      if (options.output) {
        await saveReport({ concurrency: results }, options.output);
        console.log(chalk.green(`📄 Results saved to: ${options.output}`));
      }
      
      await runner.cleanup();
      
    } catch (error) {
      console.error(chalk.red('❌ Concurrency benchmark failed:'), error.message);
      process.exit(1);
    }
  });

// Generate test data
program
  .command('generate-data')
  .description('Generate test data for manual testing')
  .option('-s, --sellers <count>', 'Number of sellers', '10')
  .option('-b, --buyers <count>', 'Number of buyers', '100')
  .option('-c, --conversations <count>', 'Conversations per seller', '10')
  .option('-m, --messages <count>', 'Messages per conversation', '50')
  .option('-d, --database <db>', 'Target database (scylladb|clickhouse|timescaledb|cockroachdb|all)', 'all')
  .action(async (options) => {
    try {
      console.log(chalk.blue('🏭 Generating test data...'));
      
      const generator = new DataGenerator();
      const data = generator.generateRealisticConversationBatch(
        Number.parseInt(options.sellers),
        Number.parseInt(options.buyers),
        {
          conversationsPerSeller: Number.parseInt(options.conversations),
          messagesPerConversation: Number.parseInt(options.messages)
        }
      );
      
      const databases = options.database === 'all' 
        ? ['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb']
        : [options.database];
      
      for (const dbName of databases) {
        console.log(chalk.blue(`\n📊 Inserting data into ${dbName.toUpperCase()}...`));
        
        const client = getClient(dbName);
        await client.connect();
        
        try {
          await client.createSellersBatch(data.sellers);
          await client.createBuyersBatch(data.buyers);
          await client.createConversationsBatch(data.conversations);
          await client.createMessagesBatch(data.messages);
          
          console.log(chalk.green(`✅ Data inserted into ${dbName.toUpperCase()}`));
        } finally {
          await client.disconnect();
        }
      }
      
      console.log(chalk.green('\n🎉 Test data generation completed!'));
      console.log(`Generated: ${data.summary.sellers} sellers, ${data.summary.buyers} buyers, ${data.summary.conversations} conversations, ${data.summary.messages} messages`);
      
    } catch (error) {
      console.error(chalk.red('❌ Data generation failed:'), error.message);
      process.exit(1);
    }
  });

// Health check command
program
  .command('health')
  .description('Check database connectivity and health')
  .action(async () => {
    try {
      console.log(chalk.blue('🏥 Checking database health...'));
      
      const clients = {
        scylladb: new ScyllaDBClient(),
        clickhouse: new ClickHouseClient(),
        timescaledb: new TimescaleDBClient(),
        cockroachdb: new CockroachDBClient()
      };
      
      for (const [dbName, client] of Object.entries(clients)) {
        console.log(chalk.blue(`\n🔍 Checking ${dbName.toUpperCase()}...`));
        
        try {
          await client.connect();
          const health = await client.healthCheck();
          const metrics = await client.getMetrics();
          
          console.log(chalk.green(`✅ ${dbName.toUpperCase()}: ${health.status}`));
          if (metrics) {
            console.log(`   Records: ${JSON.stringify(metrics, null, 2)}`);
          }
          
          await client.disconnect();
        } catch (error) {
          console.log(chalk.red(`❌ ${dbName.toUpperCase()}: ${error.message}`));
        }
      }
      
    } catch (error) {
      console.error(chalk.red('❌ Health check failed:'), error.message);
      process.exit(1);
    }
  });

// Setup databases - initialize schemas
program
  .command('setup')
  .description('Initialize database schemas (keyspaces, tables, etc.)')
  .option('-d, --database <db>', 'Target database (scylladb|clickhouse|timescaledb|cockroachdb|all)', 'all')
  .action(async (options) => {
    try {
      console.log(chalk.blue('🏗️  Setting up database schemas...'));
      
      const databases = options.database === 'all' 
        ? ['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb']
        : [options.database];
      
      for (const dbName of databases) {
        console.log(chalk.blue(`\n🔧 Setting up ${dbName.toUpperCase()}...`));
        
        try {
          let client;
          switch (dbName) {
            case 'scylladb':
              client = new ScyllaDBClient();
              await client.initializeSchema();
              await client.disconnect();
              break;
              
            case 'clickhouse':
              client = new ClickHouseClient();
              await client.connect();
              console.log('✅ ClickHouse schema already initialized via Docker');
              await client.disconnect();
              break;
              
            case 'timescaledb':
              client = new TimescaleDBClient();
              await client.connect();
              console.log('✅ TimescaleDB schema already initialized via Docker');
              await client.disconnect();
              break;
              
            case 'cockroachdb':
              client = new CockroachDBClient();
              await client.connect();
              console.log('✅ CockroachDB schema already initialized via Docker');
              await client.disconnect();
              break;
              
            default:
              throw new Error(`Unknown database: ${dbName}`);
          }
          
          console.log(chalk.green(`✅ ${dbName.toUpperCase()} setup completed`));
          
        } catch (error) {
          console.error(chalk.red(`❌ ${dbName.toUpperCase()} setup failed:`), error.message);
        }
      }
      
      console.log(chalk.green('\n🎉 Database setup completed!'));
      
    } catch (error) {
      console.error(chalk.red('❌ Setup failed:'), error.message);
      process.exit(1);
    }
  });

// Clean databases
program
  .command('clean')
  .description('Clean all data from databases (DESTRUCTIVE)')
  .option('-d, --database <db>', 'Target database (scylladb|clickhouse|timescaledb|cockroachdb|all)', 'all')
  .option('--confirm', 'Confirm destructive operation')
  .action(async (options) => {
    if (!options.confirm) {
      console.log(chalk.red('⚠️  This is a destructive operation. Use --confirm flag to proceed.'));
      process.exit(1);
    }
    
    try {
      console.log(chalk.yellow('🧹 Cleaning databases...'));
      
      const databases = options.database === 'all' 
        ? ['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb']
        : [options.database];
      
      for (const dbName of databases) {
        console.log(chalk.blue(`\n🗑️  Cleaning ${dbName.toUpperCase()}...`));
        
        const client = getClient(dbName);
        await client.connect();
        
        try {
          // Note: This would need to be implemented in each client
          if (client.truncateAllTables) {
            await client.truncateAllTables();
            console.log(chalk.green(`✅ ${dbName.toUpperCase()} cleaned`));
          } else {
            console.log(chalk.yellow(`⚠️  Clean operation not implemented for ${dbName.toUpperCase()}`));
          }
        } finally {
          await client.disconnect();
        }
      }
      
    } catch (error) {
      console.error(chalk.red('❌ Clean operation failed:'), error.message);
      process.exit(1);
    }
  });

// Compare results
program
  .command('compare')
  .description('Compare results from multiple benchmark runs')
  .argument('<files...>', 'JSON result files to compare')
  .action(async (files) => {
    try {
      console.log(chalk.blue('📊 Comparing benchmark results...'));
      
      const results = [];
      for (const file of files) {
        const content = await fs.readFile(file, 'utf8');
        const data = JSON.parse(content);
        results.push({ file: path.basename(file), data });
      }
      
      // Simple comparison display
      console.log(chalk.green('\n📈 Comparison Results:'));
      results.forEach(result => {
        console.log(chalk.blue(`\n📄 ${result.file}:`));
        if (result.data.summary?.winner) {
          console.log(`   Writes: ${result.data.summary.winner.writes?.database || 'N/A'}`);
          console.log(`   Reads: ${result.data.summary.winner.reads?.database || 'N/A'}`);
          console.log(`   Concurrency: ${result.data.summary.winner.concurrency?.database || 'N/A'}`);
        }
      });
      
    } catch (error) {
      console.error(chalk.red('❌ Comparison failed:'), error.message);
      process.exit(1);
    }
  });

// Docker orchestration commands
program
  .command('docker-up')
  .description('Start all database containers')
  .action(async () => {
    try {
      await dockerOrchestrator.startAllDatabases();
      console.log(chalk.green('✅ All database containers started successfully'));
    } catch (error) {
      console.error(chalk.red('❌ Failed to start containers:'), error.message);
      process.exit(1);
    }
  });

program
  .command('docker-down')
  .description('Stop all database containers')
  .action(async () => {
    try {
      await dockerOrchestrator.stopAllDatabases();
      console.log(chalk.green('✅ All database containers stopped successfully'));
    } catch (error) {
      console.error(chalk.red('❌ Failed to stop containers:'), error.message);
      process.exit(1);
    }
  });

program
  .command('docker-status')
  .description('Show status of all containers')
  .action(async () => {
    try {
      const status = await dockerOrchestrator.getContainerStatus();
      console.log(chalk.blue('🐳 Container Status:'));
      console.log(status);
    } catch (error) {
      console.error(chalk.red('❌ Failed to get container status:'), error.message);
      process.exit(1);
    }
  });

program
  .command('docker-isolated')
  .description('Start a single database in isolation')
  .option('-d, --database <db>', 'Database to start (scylladb, clickhouse, timescaledb, cockroachdb)', 'scylladb')
  .action(async (options) => {
    try {
      await dockerOrchestrator.startIsolatedDatabase(options.database);
      console.log(chalk.green(`✅ ${options.database.toUpperCase()} container started in isolation`));
    } catch (error) {
      console.error(chalk.red(`❌ Failed to start ${options.database}:`), error.message);
      process.exit(1);
    }
  });

program
  .command('docker-cleanup')
  .description('Stop and cleanup all containers')
  .action(async () => {
    try {
      await dockerOrchestrator.cleanupAllContainers();
      console.log(chalk.green('✅ All containers cleaned up successfully'));
    } catch (error) {
      console.error(chalk.red('❌ Failed to cleanup containers:'), error.message);
      process.exit(1);
    }
  });

// List scenarios
program
  .command('scenarios')
  .description('List available benchmark scenarios')
  .action(() => {
    console.log(chalk.blue('📋 Available Benchmark Scenarios:'));
    console.log(chalk.green('\n🔥 high_write_volume'));
    console.log('   - 100 sellers, 1000 buyers');
    console.log('   - 50 conversations per seller');
    console.log('   - 200 messages per conversation');
    console.log('   - Focus: High-throughput message ingestion');
    
    console.log(chalk.green('\n📊 read_heavy_analytics'));
    console.log('   - 50 sellers, 500 buyers');
    console.log('   - 30 conversations per seller');
    console.log('   - 500 messages per conversation');
    console.log('   - Focus: Complex analytical queries');
    
    console.log(chalk.green('\n⚖️  mixed_workload (default)'));
    console.log('   - 200 sellers, 2000 buyers');
    console.log('   - 25 conversations per seller');
    console.log('   - 150 messages per conversation');
    console.log('   - Focus: Balanced read/write operations');
    
    console.log(chalk.green('\n🧪 custom'));
    console.log('   - 10 sellers, 100 buyers');
    console.log('   - 10 conversations per seller');
    console.log('   - 50 messages per conversation');
    console.log('   - Focus: Small-scale testing');
  });

// Helper functions
function getClient(dbName) {
  switch (dbName) {
    case 'scylladb':
      return new ScyllaDBClient();
    case 'clickhouse':
      return new ClickHouseClient();
    case 'timescaledb':
      return new TimescaleDBClient();
    case 'cockroachdb':
      return new CockroachDBClient();
    default:
      throw new Error(`Unknown database: ${dbName}`);
  }
}

async function saveReport(report, filename) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const reportsDir = path.join(projectRoot, 'reports');
  
  // Ensure reports directory exists
  try {
    await fs.mkdir(reportsDir, { recursive: true });
  } catch (error) {
    // Directory might already exist, that's fine
  }
  
  // Determine the full file path
  let fullPath;
  if (filename) {
    // If filename is provided, use it (with timestamp if no extension)
    const hasExtension = filename.includes('.');
    const finalFilename = hasExtension ? filename : `${filename}-${timestamp}.json`;
    fullPath = path.join(reportsDir, finalFilename);
  } else {
    // Default filename with timestamp
    fullPath = path.join(reportsDir, `benchmark-report-${timestamp}.json`);
  }
  
  await fs.writeFile(fullPath, JSON.stringify(report, null, 2));
  return fullPath;
}

// Error handling
process.on('unhandledRejection', (error) => {
  console.error(chalk.red('❌ Unhandled promise rejection:'), error);
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  console.error(chalk.red('❌ Uncaught exception:'), error);
  process.exit(1);
});

// Parse command line arguments
program.parse();
