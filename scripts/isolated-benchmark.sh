#!/bin/bash

# Isolated Database Benchmark Script
# This script runs each database with maximum resources in isolation

set -e

# Use specified paths
NODE_PATH="/Users/bernardo.klopffleisch/.nvm/versions/node/v22.14.0/bin/node"
DOCKER_PATH="/usr/local/bin/docker"
DOCKER_COMPOSE_PATH="/usr/local/bin/docker-compose"

DATABASE=${1:-"scylladb"}
SCENARIO=${2:-"mixed_workload"}
OUTPUT=${3:-""}

echo "🎯 Starting Isolated Benchmark for $DATABASE"
echo "📊 Scenario: $SCENARIO"
echo "🔧 Using Node.js: $NODE_PATH"
echo "🐳 Using Docker: $DOCKER_PATH"

# Stop all running containers first
echo "🛑 Stopping all existing containers..."
$DOCKER_COMPOSE_PATH down -v 2>/dev/null || true

# Wait for cleanup
sleep 5

# Start the isolated database
echo "🚀 Starting $DATABASE in isolation with maximum resources..."
$DOCKER_COMPOSE_PATH -f docker-compose.$DATABASE.yml up -d

# Wait for database to be ready
echo "⏳ Waiting for $DATABASE to be ready..."
sleep 30

# Check health status
echo "🏥 Checking $DATABASE health..."
$NODE_PATH src/cli.js health

# Initialize schema
echo "🏗️  Initializing $DATABASE schema..."
$NODE_PATH src/cli.js setup

# Run the isolated benchmark
echo "🏁 Running isolated benchmark..."
if [ -n "$OUTPUT" ]; then
    $NODE_PATH src/cli.js benchmark-isolated -d $DATABASE -s $SCENARIO -o $OUTPUT
else
    $NODE_PATH src/cli.js benchmark-isolated -d $DATABASE -s $SCENARIO
fi

echo "✅ Isolated benchmark completed for $DATABASE!"
echo "📁 Results saved in reports/ folder"

# Optionally stop the containers
read -p "Stop $DATABASE containers? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🛑 Stopping $DATABASE containers..."
    $DOCKER_COMPOSE_PATH -f docker-compose.$DATABASE.yml down
fi
