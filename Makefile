.PHONY: help setup start stop restart clean test health benchmark logs

# Default target
help:
	@echo "WhatsApp Database Benchmark Suite"
	@echo ""
	@echo "Available commands:"
	@echo "  make setup      - Install dependencies and setup environment"
	@echo "  make start      - Start all database containers"
	@echo "  make stop       - Stop all containers"
	@echo "  make restart    - Restart all containers"
	@echo "  make test       - Test database connections and setup"
	@echo "  make health     - Check database health"
	@echo "  make benchmark  - Run full benchmark suite"
	@echo "  make clean      - Clean all data (DESTRUCTIVE)"
	@echo "  make logs       - View container logs"
	@echo ""

# Setup project
setup:
	@echo "🔧 Setting up WhatsApp Database Benchmarks..."
	npm install
	cp .env.example .env
	@echo "✅ Setup complete! Edit .env file if needed."

# Start databases
start:
	@echo "🚀 Starting database containers..."
	docker-compose up -d
	@echo "⏳ Waiting for databases to be ready..."
	sleep 30
	@echo "✅ Databases started!"

# Stop containers
stop:
	@echo "🛑 Stopping containers..."
	docker-compose down

# Restart containers
restart:
	@echo "🔄 Restarting containers..."
	docker-compose restart
	sleep 20

# Test setup
test:
	@echo "🧪 Testing setup..."
	node test-setup.js

# Health check
health:
	@echo "🏥 Checking database health..."
	npm run health

# Run benchmark
benchmark:
	@echo "📊 Running benchmark suite..."
	npm start

# Clean data
clean:
	@echo "🧹 Cleaning all data..."
	npm run clean

# View logs
logs:
	@echo "📋 Viewing container logs..."
	docker-compose logs -f

# Quick start (setup + start + test)
quickstart: setup start test
	@echo "🎉 Quick start completed!"
	@echo "💡 Run 'make benchmark' to start benchmarking"
