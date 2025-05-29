#!/bin/bash

echo "🚀 Starting WhatsApp Database Benchmark Suite..."
echo "📋 This will start all databases with M1 Mac optimized settings"
echo ""

# Find Docker executable
DOCKER_CMD=""
if command -v docker >/dev/null 2>&1; then
    DOCKER_CMD="docker"
elif [ -f "/Applications/Docker.app/Contents/Resources/bin/docker" ]; then
    DOCKER_CMD="/Applications/Docker.app/Contents/Resources/bin/docker"
elif [ -f "/usr/local/bin/docker" ]; then
    DOCKER_CMD="/usr/local/bin/docker"
else
    echo "❌ Docker not found. Please install Docker Desktop."
    exit 1
fi

# Find Docker Compose
COMPOSE_CMD=""
if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
elif $DOCKER_CMD compose version >/dev/null 2>&1; then
    COMPOSE_CMD="$DOCKER_CMD compose"
elif [ -f "/Applications/Docker.app/Contents/Resources/cli-plugins/docker-compose" ]; then
    COMPOSE_CMD="$DOCKER_CMD compose"
else
    echo "❌ Docker Compose not found."
    exit 1
fi

# Check if Docker is running
if ! $DOCKER_CMD info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Clean up any existing containers
echo "🧹 Cleaning up existing containers..."
$COMPOSE_CMD down --volumes 2>/dev/null || true

# Start all services
echo "🏗️  Starting database services..."
$COMPOSE_CMD up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
echo "   This may take 1-2 minutes on first startup..."

# Function to check service health
check_service() {
    local service=$1
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if $COMPOSE_CMD ps $service | grep -q "healthy"; then
            echo "✅ $service is ready"
            return 0
        fi
        echo "   ⏳ Waiting for $service... (attempt $attempt/$max_attempts)"
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "❌ $service failed to start properly"
    return 1
}

# Check each service
check_service "scylladb"
check_service "clickhouse" 
check_service "timescaledb"

echo ""
echo "🎉 All databases are ready!"
echo ""
echo "📊 You can now run benchmarks:"
echo "   node src/cli.js health-check"
echo "   node src/cli.js benchmark --scenario custom"
echo ""
echo "🔍 Service URLs:"
echo "   ScyllaDB:   localhost:9042 (CQL)"
echo "   ClickHouse: localhost:8123 (HTTP) / localhost:9000 (Native)"
echo "   TimescaleDB: localhost:5432 (PostgreSQL)"
echo ""
echo "🛑 To stop all services: $COMPOSE_CMD down"
