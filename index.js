#!/usr/bin/env node

import { BenchmarkRunner } from './src/benchmark/benchmark-runner.js';
import chalk from 'chalk';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

async function main() {
  console.log(chalk.blue('🚀 WhatsApp Database Benchmark Suite'));
  console.log(chalk.gray('Comparing ScyllaDB, ClickHouse, and TimescaleDB performance\n'));

  try {
    // Initialize benchmark runner with default options
    const runner = new BenchmarkRunner({
      batchSize: 1000,
      warmupRuns: 3,
      benchmarkRuns: 5
    });

    // Run the full benchmark suite
    console.log(chalk.blue('📊 Starting full benchmark suite...'));
    const report = await runner.runFullBenchmark('mixed_workload');

    // Display summary
    console.log(chalk.green('\n🎉 Benchmark completed successfully!'));
    console.log(chalk.blue('\n📋 Summary:'));
    if (report.summary?.winner) {
      console.log(`   Best for Writes: ${report.summary.winner.writes?.database || 'N/A'}`);
      console.log(`   Best for Reads: ${report.summary.winner.reads?.database || 'N/A'}`);
      console.log(`   Best for Concurrency: ${report.summary.winner.concurrency?.database || 'N/A'}`);
    }

    // Save report
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `benchmark-report-${timestamp}.json`;
    
    const fs = await import('node:fs/promises');
    await fs.writeFile(filename, JSON.stringify(report, null, 2));
    console.log(chalk.green(`\n📄 Full report saved to: ${filename}`));

    console.log(chalk.blue('\n💡 Tip: Use the CLI for more options:'));
    console.log('   node src/cli.js --help');

  } catch (error) {
    console.error(chalk.red('❌ Benchmark failed:'), error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

export { main };
