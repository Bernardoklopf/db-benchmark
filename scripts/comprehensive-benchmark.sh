#!/bin/bash

# Comprehensive Isolated Database Benchmark Script
# Runs all databases in isolation and generates a consolidated report

set -e

# Use specified paths
NODE_PATH="/Users/bernardo.klopffleisch/.nvm/versions/node/v22.14.0/bin/node"
DOCKER_PATH="/usr/local/bin/docker"
DOCKER_COMPOSE_PATH="/usr/local/bin/docker-compose"

# Configuration
SCENARIO=${1:-"mixed_workload"}
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_DIR="reports"
CONSOLIDATED_REPORT="$REPORT_DIR/comprehensive-benchmark-$TIMESTAMP.json"

# Database list
DATABASES=("scylladb" "clickhouse" "timescaledb" "cockroachdb")

echo "🎯 Starting Comprehensive Isolated Database Benchmark"
echo "📊 Scenario: $SCENARIO"
echo "🔧 Using Node.js: $NODE_PATH"
echo "🐳 Using Docker: $DOCKER_PATH"
echo "📅 Timestamp: $TIMESTAMP"

# Ensure reports directory exists
mkdir -p "$REPORT_DIR"

# Initialize consolidated report
echo "{" > "$CONSOLIDATED_REPORT"
echo "  \"timestamp\": \"$(date -Iseconds)\"," >> "$CONSOLIDATED_REPORT"
echo "  \"scenario\": \"$SCENARIO\"," >> "$CONSOLIDATED_REPORT"
echo "  \"node_version\": \"$($NODE_PATH --version)\"," >> "$CONSOLIDATED_REPORT"
echo "  \"docker_version\": \"$($DOCKER_PATH --version | head -n1)\"," >> "$CONSOLIDATED_REPORT"
echo "  \"results\": {" >> "$CONSOLIDATED_REPORT"

# Stop all containers first
echo "🛑 Stopping all existing containers..."
$DOCKER_COMPOSE_PATH down -v 2>/dev/null || true
sleep 5

# Benchmark each database
for i in "${!DATABASES[@]}"; do
    DB="${DATABASES[$i]}"
    echo ""
    echo "🔄 [$((i+1))/${#DATABASES[@]}] Benchmarking $DB..."
    
    # Start the isolated database
    echo "🚀 Starting $DB in isolation..."
    $DOCKER_COMPOSE_PATH -f docker-compose.$DB.yml up -d
    
    # Wait for database to be ready
    echo "⏳ Waiting for $DB to be ready..."
    sleep 45
    
    # Check health status with retries
    echo "🏥 Checking $DB health..."
    for retry in {1..5}; do
        if $NODE_PATH src/cli.js health 2>/dev/null; then
            echo "✅ $DB is healthy"
            break
        else
            echo "⚠️  Health check failed, retrying ($retry/5)..."
            sleep 10
        fi
    done
    
    # Initialize schema
    echo "🏗️  Initializing $DB schema..."
    $NODE_PATH src/cli.js setup
    
    # Run the benchmark
    echo "🏁 Running $DB benchmark..."
    OUTPUT_FILE="$DB-isolated-$TIMESTAMP"
    
    if $NODE_PATH src/cli.js benchmark-isolated -d $DB -s $SCENARIO -o $OUTPUT_FILE; then
        echo "✅ $DB benchmark completed successfully"
        
        # Extract results and add to consolidated report
        RESULT_FILE="$REPORT_DIR/$OUTPUT_FILE.json"
        if [ -f "$RESULT_FILE" ]; then
            # Add comma if not the first database
            if [ $i -gt 0 ]; then
                echo "," >> "$CONSOLIDATED_REPORT"
            fi
            
            # Add database results
            echo "    \"$DB\": " >> "$CONSOLIDATED_REPORT"
            cat "$RESULT_FILE" | jq '.' >> "$CONSOLIDATED_REPORT"
        fi
    else
        echo "❌ $DB benchmark failed"
        
        # Add error result
        if [ $i -gt 0 ]; then
            echo "," >> "$CONSOLIDATED_REPORT"
        fi
        echo "    \"$DB\": {" >> "$CONSOLIDATED_REPORT"
        echo "      \"error\": \"Benchmark failed\"," >> "$CONSOLIDATED_REPORT"
        echo "      \"timestamp\": \"$(date -Iseconds)\"" >> "$CONSOLIDATED_REPORT"
        echo "    }" >> "$CONSOLIDATED_REPORT"
    fi
    
    # Stop the database containers
    echo "🛑 Stopping $DB containers..."
    $DOCKER_COMPOSE_PATH -f docker-compose.$DB.yml down -v
    
    # Brief pause between databases
    sleep 10
done

# Close consolidated report
echo "" >> "$CONSOLIDATED_REPORT"
echo "  }" >> "$CONSOLIDATED_REPORT"
echo "}" >> "$CONSOLIDATED_REPORT"

echo ""
echo "🎉 Comprehensive benchmark completed!"
echo "📁 Consolidated report: $CONSOLIDATED_REPORT"
echo "📊 Individual reports available in $REPORT_DIR/"

# Display summary
echo ""
echo "📋 BENCHMARK SUMMARY"
echo "===================="
if command -v jq >/dev/null 2>&1; then
    echo "Results overview:"
    jq -r '.results | to_entries[] | "\(.key): " + (if .value.error then "❌ " + .value.error else "✅ Completed" end)' "$CONSOLIDATED_REPORT"
else
    echo "Install 'jq' to see detailed summary"
    echo "Report saved to: $CONSOLIDATED_REPORT"
fi
