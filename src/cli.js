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

// Available databases
const AVAILABLE_DATABASES = ['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb'];
const AVAILABLE_SCENARIOS = ['custom', 'high_write_volume', 'read_heavy_analytics', 'mixed_workload'];

program
  .name('whatsapp-benchmark')
  .description('WhatsApp Database Benchmarking Tool for ScyllaDB, ClickHouse, TimescaleDB, and CockroachDB')
  .version('1.0.0');

// Helper function to parse database list
function parseDatabaseList(value) {
  if (!value) return AVAILABLE_DATABASES;
  
  const databases = value.split(',').map(db => db.trim().toLowerCase());
  const invalid = databases.filter(db => !AVAILABLE_DATABASES.includes(db));
  
  if (invalid.length > 0) {
    console.error(chalk.red(`❌ Invalid databases: ${invalid.join(', ')}`));
    console.error(chalk.gray(`Available databases: ${AVAILABLE_DATABASES.join(', ')}`));
    process.exit(1);
  }
  
  return databases;
}

// Helper function to generate output filename
function generateOutputFilename(mode, scenario, databases) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const dbList = databases.length === AVAILABLE_DATABASES.length ? 'all' : databases.join('-');
  return `benchmark-${mode}-${scenario}-${dbList}-${timestamp}.json`;
}

// Isolated benchmark command (databases tested one by one)
program
  .command('benchmark-isolated')
  .description('Run benchmarks with databases in isolation (recommended for resource-constrained environments)')
  .option('-s, --scenario <type>', 'Benchmark scenario', 'custom')
  .option('-b, --batch-size <size>', 'Batch size for operations', '1000')
  .option('-w, --warmup-runs <runs>', 'Number of warmup runs', '3')
  .option('-r, --benchmark-runs <runs>', 'Number of benchmark runs', '5')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .option('--databases <list>', 'Comma-separated list of databases to test', AVAILABLE_DATABASES.join(','))
  .option('--skip-docker', 'Skip Docker setup')
  .action(async (options) => {
    try {
      console.log(chalk.blue('🚀 Starting Isolated Database Benchmark Suite'));
      
      const databases = parseDatabaseList(options.databases);
      const scenario = options.scenario;
      
      if (!AVAILABLE_SCENARIOS.includes(scenario)) {
        console.error(chalk.red(`❌ Invalid scenario: ${scenario}`));
        console.error(chalk.gray(`Available scenarios: ${AVAILABLE_SCENARIOS.join(', ')}`));
        process.exit(1);
      }
      
      console.log(chalk.gray(`Scenario: ${scenario}`));
      console.log(chalk.gray(`Databases: ${databases.join(', ')}`));
      console.log(chalk.gray('Mode: ISOLATED (one database at a time)'));
      
      const outputFile = options.output || generateOutputFilename('isolated', scenario, databases);
      
      // Start Docker containers if needed
      if (!options.skipDocker) {
        await dockerOrchestrator.startDatabases(databases, 'isolated');
      }
      
      // Run isolated benchmarks using the enhanced BenchmarkRunner method
      const benchmarkOptions = {
        databases: databases,
        mode: 'isolated',
        scenario: scenario,
        batchSize: Number.parseInt(options.batchSize),
        warmupRuns: Number.parseInt(options.warmupRuns),
        benchmarkRuns: Number.parseInt(options.benchmarkRuns),
        outputFile: outputFile
      };
      
      const runner = new BenchmarkRunner(benchmarkOptions);
      const allResults = await runner.runIsolatedBenchmarks();
      
      console.log(chalk.green('🎉 All isolated benchmarks completed!'));
      
    } catch (error) {
      console.error(chalk.red('❌ Isolated benchmark failed:'), error.message);
      process.exit(1);
    }
  });

// Together benchmark command (all databases simultaneously)
program
  .command('benchmark-together')
  .description('Run benchmarks with all databases together (requires adequate resources)')
  .option('-s, --scenario <type>', 'Benchmark scenario', 'mixed_workload')
  .option('-b, --batch-size <size>', 'Batch size for operations', '1000')
  .option('-w, --warmup-runs <runs>', 'Number of warmup runs', '3')
  .option('-r, --benchmark-runs <runs>', 'Number of benchmark runs', '5')
  .option('-o, --output <file>', 'Output file for results (JSON)')
  .option('--databases <list>', 'Comma-separated list of databases to test', AVAILABLE_DATABASES.join(','))
  .option('--skip-docker', 'Skip Docker setup')
  .action(async (options) => {
    try {
      console.log(chalk.blue('🚀 Starting Together Database Benchmark Suite'));
      
      const databases = parseDatabaseList(options.databases);
      const scenario = options.scenario;
      
      if (!AVAILABLE_SCENARIOS.includes(scenario)) {
        console.error(chalk.red(`❌ Invalid scenario: ${scenario}`));
        console.error(chalk.gray(`Available scenarios: ${AVAILABLE_SCENARIOS.join(', ')}`));
        process.exit(1);
      }
      
      console.log(chalk.gray(`Scenario: ${scenario}`));
      console.log(chalk.gray(`Databases: ${databases.join(', ')}`));
      console.log(chalk.gray('Mode: TOGETHER (all databases simultaneously)'));
      
      const outputFile = options.output || generateOutputFilename('together', scenario, databases);
      
      // Start all databases together
      if (!options.skipDocker) {
        await dockerOrchestrator.startDatabases(databases, 'together');
      }
      
      const benchmarkOptions = {
        databases: databases,
        mode: 'together',
        scenario: scenario,
        batchSize: Number.parseInt(options.batchSize),
        warmupRuns: Number.parseInt(options.warmupRuns),
        benchmarkRuns: Number.parseInt(options.benchmarkRuns),
        outputFile: outputFile
      };
      
      const runner = new BenchmarkRunner(benchmarkOptions);
      const results = await runner.runBenchmarks();
      
      console.log(chalk.green('✅ Together benchmark completed successfully!'));
      
    } catch (error) {
      console.error(chalk.red('❌ Together benchmark failed:'), error.message);
      process.exit(1);
    }
  });

// Quick test command for single database
program
  .command('test-database <database>')
  .description('Quick test of a single database')
  .option('-s, --scenario <type>', 'Benchmark scenario', 'custom')
  .option('--skip-docker', 'Skip Docker setup')
  .action(async (database, options) => {
    try {
      if (!AVAILABLE_DATABASES.includes(database.toLowerCase())) {
        console.error(chalk.red(`❌ Invalid database: ${database}`));
        console.error(chalk.gray(`Available databases: ${AVAILABLE_DATABASES.join(', ')}`));
        process.exit(1);
      }
      
      const dbName = database.toLowerCase();
      console.log(chalk.blue(`🚀 Quick testing ${dbName.toUpperCase()}...`));
      
      if (!options.skipDocker) {
        await dockerOrchestrator.startDatabases([dbName], 'isolated');
      }
      
      const benchmarkOptions = {
        databases: [dbName],
        mode: 'isolated',
        scenario: options.scenario,
        batchSize: 100, // Smaller batch for quick test
        warmupRuns: 1,
        benchmarkRuns: 2,
        outputFile: null
      };
      
      const runner = new BenchmarkRunner(benchmarkOptions);
      await runner.runBenchmarks();
      
      console.log(chalk.green(`✅ ${dbName.toUpperCase()} quick test completed!`));
      
    } catch (error) {
      console.error(chalk.red(`❌ Quick test failed: ${error.message}`));
      process.exit(1);
    }
  });

// Docker management commands
program
  .command('docker-up')
  .description('Start all database containers')
  .option('--databases <list>', 'Comma-separated list of databases to start', AVAILABLE_DATABASES.join(','))
  .action(async (options) => {
    try {
      const databases = parseDatabaseList(options.databases);
      await dockerOrchestrator.startDatabases(databases, 'together');
      console.log(chalk.green('✅ Database containers started successfully!'));
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
      console.log(chalk.green('✅ All containers stopped successfully!'));
    } catch (error) {
      console.error(chalk.red('❌ Failed to stop containers:'), error.message);
      process.exit(1);
    }
  });

program
  .command('docker-status')
  .description('Show status of database containers')
  .action(async () => {
    try {
      console.log(chalk.blue('🐳 Container Status:'));
      const status = await dockerOrchestrator.getContainerStatus();
      console.log(status);
    } catch (error) {
      console.error(chalk.red('❌ Failed to get container status:'), error.message);
      process.exit(1);
    }
  });

program
  .command('docker-cleanup')
  .description('Stop all containers and clean up Docker resources')
  .action(async () => {
    try {
      await dockerOrchestrator.cleanupAllContainers();
      console.log(chalk.green('✅ Docker cleanup completed!'));
    } catch (error) {
      console.error(chalk.red('❌ Docker cleanup failed:'), error.message);
      process.exit(1);
    }
  });

// Health check command
program
  .command('health')
  .description('Check health of all database connections')
  .option('--databases <list>', 'Comma-separated list of databases to check', AVAILABLE_DATABASES.join(','))
  .action(async (options) => {
    try {
      console.log(chalk.blue('🏥 Checking database health...'));
      
      const databases = parseDatabaseList(options.databases);
      const clients = {
        scylladb: ScyllaDBClient,
        clickhouse: ClickHouseClient,
        timescaledb: TimescaleDBClient,
        cockroachdb: CockroachDBClient
      };
      
      for (const dbName of databases) {
        console.log(chalk.yellow(`\n🔍 Checking ${dbName.toUpperCase()}...`));
        
        try {
          const ClientClass = clients[dbName];
          const client = new ClientClass();
          
          await client.connect();
          const health = await client.healthCheck();
          
          console.log(chalk.green(`✅ ${dbName.toUpperCase()}: healthy`));
          if (health.records) {
            console.log(chalk.gray(`   Records: ${JSON.stringify(health.records)}`));
          }
          
          await client.disconnect();
          
        } catch (error) {
          console.error(chalk.red(`❌ ${dbName.toUpperCase()}: ${error.message}`));
        }
      }
      
    } catch (error) {
      console.error(chalk.red('❌ Health check failed:'), error.message);
      process.exit(1);
    }
  });

// Setup command (now just informs users that setup is automatic)
program
  .command('setup')
  .description('Initialize database schemas (now happens automatically when starting databases)')
  .option('--databases <list>', 'Comma-separated list of databases to setup', AVAILABLE_DATABASES.join(','))
  .action(async (options) => {
    console.log(chalk.blue('ℹ️ Schema initialization is now automatic'));
    console.log(chalk.gray('Database schemas are now automatically initialized when starting databases.'));
    console.log(chalk.gray('You can start databases with:'));
    console.log(chalk.cyan('  npm run start-db'));
    console.log(chalk.gray('Or:'));
    console.log(chalk.cyan(`  node src/cli.js start-db --databases ${options.databases}`));
  });

// Scenarios command
program
  .command('scenarios')
  .description('List available benchmark scenarios')
  .action(() => {
    console.log(chalk.blue('📋 Available Benchmark Scenarios:\n'));
    
    const scenarios = {
      custom: {
        description: 'Basic read/write operations for quick testing',
        operations: ['writes', 'reads'],
        recommended: 'Development and quick validation'
      },
      high_write_volume: {
        description: 'Heavy write workload testing',
        operations: ['writes', 'batch_writes'],
        recommended: 'Write performance optimization'
      },
      read_heavy_analytics: {
        description: 'Complex analytical queries',
        operations: ['reads', 'complex_queries'],
        recommended: 'Query performance optimization'
      },
      mixed_workload: {
        description: 'Realistic mixed read/write workload',
        operations: ['writes', 'reads', 'batch_writes', 'complex_queries'],
        recommended: 'Production simulation'
      }
    };
    
    for (const [name, info] of Object.entries(scenarios)) {
      console.log(chalk.cyan(`${name}:`));
      console.log(chalk.gray(`  Description: ${info.description}`));
      console.log(chalk.gray(`  Operations: ${info.operations.join(', ')}`));
      console.log(chalk.gray(`  Recommended for: ${info.recommended}\n`));
    }
  });

// Utility functions
async function saveResults(results, filename) {
  try {
    const reportsDir = path.join(process.cwd(), 'reports');
    await fs.mkdir(reportsDir, { recursive: true });
    
    let outputFilename = filename;
    if (!outputFilename.endsWith('.json')) {
      outputFilename += '.json';
    }
    
    const filepath = path.join(reportsDir, outputFilename);
    await fs.writeFile(filepath, JSON.stringify(results, null, 2));
    
    console.log(chalk.green(`\n📄 Results saved to: ${filepath}`));
    
  } catch (error) {
    console.error(chalk.red(`❌ Failed to save results: ${error.message}`));
  }
}

function displayFinalSummary(results) {
  console.log(chalk.blue('\n📊 Final Summary:'));
  
  const successful = Object.entries(results.databases)
    .filter(([_, result]) => result.status === 'completed')
    .map(([db, _]) => db);
    
  const failed = Object.entries(results.databases)
    .filter(([_, result]) => result.status !== 'completed')
    .map(([db, _]) => db);
  
  if (successful.length > 0) {
    console.log(chalk.green(`✅ Successful: ${successful.join(', ')}`));
  }
  
  if (failed.length > 0) {
    console.log(chalk.red(`❌ Failed: ${failed.join(', ')}`));
  }
  
  console.log(chalk.gray(`\n📈 Total databases tested: ${Object.keys(results.databases).length}`));
  console.log(chalk.gray(`🕒 Completed at: ${results.metadata.timestamp}`));
}

// Parse command line arguments
program.parse();
