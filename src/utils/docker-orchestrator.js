import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import chalk from 'chalk';
import ora from 'ora';

const execAsync = promisify(exec);

export class DockerOrchestrator {
  constructor() {
    this.dockerPath = '/usr/local/bin/docker';
    this.dockerComposePath = '/usr/local/bin/docker-compose';
    this.projectRoot = process.cwd();
  }

  async executeCommand(command, description = '', showSpinner = true) {
    let spinner;
    
    if (showSpinner && description) {
      spinner = ora(description).start();
    }
    
    try {
      const { stdout, stderr } = await execAsync(command);
      
      if (spinner) {
        spinner.succeed(description);
      }
      
      return stdout;
    } catch (error) {
      if (spinner) {
        spinner.fail(`${description} failed`);
      }
      
      throw new Error(`Command failed: ${command}\nError: ${error.message}`);
    }
  }

  async startAllDatabases() {
    console.log(chalk.blue('🐳 Starting all databases together...'));
    
    await this.executeCommand(
      `${this.dockerComposePath} -f docker-compose.yml up -d`,
      'Starting all database containers'
    );

    // Wait for containers to be ready
    await this.waitForContainers(['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb']);
  }

  async startIsolatedDatabase(database) {
    const validDatabases = ['scylladb', 'clickhouse', 'timescaledb', 'cockroachdb'];
    
    if (!validDatabases.includes(database)) {
      throw new Error(`Invalid database: ${database}. Valid options: ${validDatabases.join(', ')}`);
    }

    // First stop all containers
    await this.executeCommand(`${this.dockerComposePath} -f docker-compose.yml down`, 'Stopping all containers');
    
    // Start only the specified database
    await this.executeCommand(`${this.dockerComposePath} -f docker-compose.yml up -d ${database}`, `Starting ${database} in isolation`);
    
    // Wait for the database to be ready
    await this.waitForDatabase(database);
  }

  async stopAllDatabases() {
    await this.executeCommand(`${this.dockerComposePath} -f docker-compose.yml down`, 'Stopping all database containers');
  }

  async stopIsolatedDatabase(database) {
    await this.executeCommand(`${this.dockerComposePath} -f docker-compose.yml stop ${database}`, `Stopping ${database}`);
  }

  async getContainerStatus() {
    try {
      const result = await this.executeCommand(`${this.dockerComposePath} -f docker-compose.yml ps`, 'Getting container status', false);
      return result;
    } catch (error) {
      console.error(chalk.red('Failed to get container status:'), error.message);
      return 'Unable to retrieve container status';
    }
  }

  async cleanupAllContainers() {
    // Stop all containers
    await this.executeCommand(`${this.dockerComposePath} -f docker-compose.yml down`, 'Stopping all containers');
    
    // Remove volumes (optional - uncomment if you want to clean volumes too)
    // await this.executeCommand(`${this.dockerComposePath} -f docker-compose.yml down -v`, 'Removing volumes');
    
    // Prune unused containers and networks
    await this.executeCommand(`${this.dockerPath} system prune -f`, 'Cleaning up Docker system');
  }

  async waitForDatabase(database) {
    const maxAttempts = 30;
    const delay = 2000; // 2 seconds
    
    console.log(chalk.yellow(`⏳ Waiting for ${database} to be ready...`));
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const result = await this.executeCommand(
          `${this.dockerComposePath} -f docker-compose.yml exec -T ${database} echo "ready"`,
          '',
          false
        );
        
        if (result.includes('ready')) {
          console.log(chalk.green(`✅ ${database} is ready!`));
          return;
        }
      } catch (error) {
        // Container might not be ready yet, continue waiting
      }
      
      if (attempt < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    console.log(chalk.yellow(`⚠️  ${database} may not be fully ready, but continuing...`));
  }

  async waitForContainers(containers, maxWaitTime = 120000) {
    console.log(chalk.blue('⏳ Waiting for containers to be ready...'));
    
    const startTime = Date.now();
    const checkInterval = 2000; // Check every 2 seconds
    
    while (Date.now() - startTime < maxWaitTime) {
      try {
        const healthChecks = await Promise.all(
          containers.map(async (container) => {
            try {
              const { stdout } = await execAsync(
                `${this.dockerPath} ps --filter "name=${container}" --filter "status=running" --format "{{.Names}}"`
              );
              return stdout.trim().includes(container);
            } catch {
              return false;
            }
          })
        );

        if (healthChecks.every(healthy => healthy)) {
          console.log(chalk.green('✅ All containers are running'));
          
          // Additional wait for database initialization
          console.log(chalk.blue('⏳ Waiting for database initialization...'));
          await new Promise(resolve => setTimeout(resolve, 10000));
          
          return;
        }
      } catch (error) {
        // Continue waiting
      }
      
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    }
    
    throw new Error(`Containers failed to start within ${maxWaitTime / 1000} seconds`);
  }
}
