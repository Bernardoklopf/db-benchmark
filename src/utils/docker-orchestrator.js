import { promisify } from 'node:util';
import { exec } from 'node:child_process';
import chalk from 'chalk';
import ora from 'ora';

const execAsync = promisify(exec);

export class DockerOrchestrator {
  constructor() {
    this.dockerPath = this.findDockerPath();
    this.dockerComposePath = this.findDockerComposePath();
    this.projectRoot = process.cwd();
    
    // Database service mappings
    this.databaseServices = {
      scylladb: 'scylladb',
      clickhouse: 'clickhouse',
      timescaledb: 'timescaledb',
      cockroachdb: 'cockroachdb'
    };
    
    // Supporting services that are always needed
    this.supportingServices = ['redis', 'prometheus', 'grafana'];
  }

  findDockerPath() {
    const possiblePaths = [
      '/usr/local/bin/docker',
    ];
    
    // For now, return the most common path - could be enhanced with actual detection
    return '/usr/local/bin/docker';
  }

  findDockerComposePath() {
    // Try docker compose (newer) first, then docker-compose (legacy)
    return '/usr/local/bin/docker-compose';
  }

  async executeCommand(command, description = '', showSpinner = true) {
    let spinner;
    
    if (showSpinner && description) {
      spinner = ora(description).start();
    }
    
    try {
      const { stdout, stderr } = await execAsync(command, { cwd: this.projectRoot });
      
      if (spinner) {
        spinner.succeed(description);
      }
      
      return stdout;
    } catch (error) {
      if (spinner) {
        spinner.fail(`${description} failed`);
      }
      
      console.error(chalk.red(`Command failed: ${command}`));
      console.error(chalk.red(`Error: ${error.message}`));
      throw error;
    }
  }

  async startDatabases(databases = [], mode = 'together') {
    console.log(chalk.blue(`🐳 Starting databases in ${mode} mode...`));
    
    // Validate databases
    const validDatabases = Object.keys(this.databaseServices);
    const invalidDatabases = databases.filter(db => !validDatabases.includes(db));
    
    if (invalidDatabases.length > 0) {
      throw new Error(`Invalid databases: ${invalidDatabases.join(', ')}. Valid options: ${validDatabases.join(', ')}`);
    }
    
    // If no databases specified, use all
    const targetDatabases = databases.length === 0 ? validDatabases : databases;
    
    if (mode === 'isolated') {
      await this.startIsolatedDatabases(targetDatabases);
    } else {
      await this.startTogetherDatabases(targetDatabases);
    }
    
    // Wait for all specified databases to be ready
    await this.waitForDatabases(targetDatabases);
    
    console.log(chalk.green(`✅ All specified databases (${targetDatabases.join(', ')}) are ready!`));
  }

  async startIsolatedDatabases(databases) {
    // In isolated mode, we start one database at a time
    // First, ensure all containers are stopped
    await this.stopAllDatabases();
    
    // Start supporting services first
    const supportingServicesStr = this.supportingServices.join(' ');
    await this.executeCommand(
      `${this.dockerComposePath} -f docker-compose.yml up -d ${supportingServicesStr}`,
      'Starting supporting services (Redis, Prometheus, Grafana)'
    );
    
    // Start the specified databases
    const databaseServicesStr = databases.map(db => this.databaseServices[db]).join(' ');
    await this.executeCommand(
      `${this.dockerComposePath} -f docker-compose.yml up -d ${databaseServicesStr}`,
      `Starting databases: ${databases.join(', ')}`
    );
  }

  async startTogetherDatabases(databases) {
    // In together mode, start all specified databases simultaneously
    const allServices = [
      ...this.supportingServices,
      ...databases.map(db => this.databaseServices[db])
    ];
    
    const servicesStr = allServices.join(' ');
    await this.executeCommand(
      `${this.dockerComposePath} -f docker-compose.yml up -d ${servicesStr}`,
      `Starting all services: ${allServices.join(', ')}`
    );
  }

  async stopAllDatabases() {
    console.log(chalk.yellow('🛑 Stopping all database containers...'));
    await this.executeCommand(
      `${this.dockerComposePath} -f docker-compose.yml down`,
      'Stopping all containers'
    );
  }

  async stopDatabases(databases) {
    if (!databases || databases.length === 0) {
      return await this.stopAllDatabases();
    }
    
    const services = databases.map(db => this.databaseServices[db]);
    const servicesStr = services.join(' ');
    
    await this.executeCommand(
      `${this.dockerComposePath} -f docker-compose.yml stop ${servicesStr}`,
      `Stopping databases: ${databases.join(', ')}`
    );
  }

  async stopOtherDatabases(targetDatabase) {
    const allDatabases = Object.keys(this.databaseServices);
    const otherDatabases = allDatabases.filter(db => db !== targetDatabase);
    
    if (otherDatabases.length > 0) {
      console.log(chalk.yellow(`🛑 Stopping other databases: ${otherDatabases.join(', ')}`));
      await this.stopDatabases(otherDatabases);
    }
  }

  async cleanupDockerVolumes() {
    console.log(chalk.yellow('🧹 Cleaning up Docker volumes...'));
    try {
      await this.executeCommand(
        `${this.dockerPath} volume prune -f`,
        'Cleaning up unused Docker volumes'
      );
    } catch (error) {
      console.log(chalk.gray('⚠️ Volume cleanup failed (non-critical):', error.message));
    }
  }

  async restartDatabase(database) {
    console.log(chalk.yellow(`🔄 Restarting ${database.toUpperCase()}...`));
    
    // Stop only the target database
    await this.stopDatabases([database]);
    
    // Wait a moment for clean shutdown
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Start only the target database (supporting services should already be running)
    const databaseService = this.databaseServices[database];
    if (databaseService) {
      await this.executeCommand(
        `${this.dockerComposePath} -f docker-compose.yml up -d ${databaseService}`,
        `Starting database: ${database}`
      );
    } else {
      throw new Error(`Unknown database: ${database}`);
    }
  }

  async getContainerStatus() {
    try {
      const result = await this.executeCommand(
        `${this.dockerComposePath} -f docker-compose.yml ps`,
        'Getting container status',
        false
      );
      return result;
    } catch (error) {
      console.error(chalk.red('Failed to get container status:'), error.message);
      return 'Unable to retrieve container status';
    }
  }

  async getRunningDatabases() {
    try {
      const status = await this.getContainerStatus();
      const runningDatabases = [];
      
      for (const [dbName, serviceName] of Object.entries(this.databaseServices)) {
        if (status.includes(serviceName) && (status.includes('Up') || status.includes('healthy'))) {
          runningDatabases.push(dbName);
        }
      }
      
      return runningDatabases;
    } catch (error) {
      console.error(chalk.red('Failed to get running databases:'), error.message);
      return [];
    }
  }

  async cleanupAllContainers() {
    console.log(chalk.yellow('🧹 Cleaning up all containers and resources...'));
    
    // Stop all containers
    await this.executeCommand(
      `${this.dockerComposePath} -f docker-compose.yml down`,
      'Stopping all containers'
    );
    
    // Optional: Remove volumes (uncomment if needed)
    // await this.executeCommand(
    //   `${this.dockerComposePath} -f docker-compose.yml down -v`,
    //   'Removing volumes'
    // );
    
    // Prune unused containers and networks
    await this.executeCommand(
      `${this.dockerPath} system prune -f`,
      'Cleaning up Docker system'
    );
  }

  async waitForDatabases(databases) {
    const maxAttempts = 60; // Increased timeout
    const delay = 2000; // 2 seconds
    
    console.log(chalk.yellow(`⏳ Waiting for databases to be ready: ${databases.join(', ')}...`));
    
    for (const database of databases) {
      await this.waitForDatabase(database, maxAttempts, delay);
    }
  }

  async waitForDatabase(database, maxAttempts = 60, delay = 2000) {
    const serviceName = this.databaseServices[database];
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // Check if container is running and healthy
        const healthCheck = await this.executeCommand(
          `${this.dockerComposePath} -f docker-compose.yml ps ${serviceName}`,
          '',
          false
        );
        
        if (healthCheck.includes('healthy') || healthCheck.includes('Up')) {
          console.log(chalk.green(`✅ ${database} is ready (attempt ${attempt})`));
          return true;
        }
        
        if (attempt < maxAttempts) {
          console.log(chalk.gray(`⏳ ${database} not ready yet (attempt ${attempt}/${maxAttempts}), waiting...`));
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      } catch (error) {
        if (attempt === maxAttempts) {
          console.error(chalk.red(`❌ ${database} failed to start after ${maxAttempts} attempts`));
          throw new Error(`Database ${database} failed to start: ${error.message}`);
        }
        
        console.log(chalk.gray(`⏳ ${database} not ready yet (attempt ${attempt}/${maxAttempts}), waiting...`));
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    throw new Error(`Database ${database} failed to start within ${maxAttempts * delay / 1000} seconds`);
  }

  async waitForDatabaseReady(database, maxAttempts = 60, delay = 2000) {
    // Alias for waitForDatabase to maintain consistency with BenchmarkRunner expectations
    return await this.waitForDatabase(database, maxAttempts, delay);
  }
}
