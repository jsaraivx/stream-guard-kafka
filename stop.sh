#!/bin/bash

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

echo "========================================"
echo "🛑 STOPPING STREAM-GUARD-KAFKA PIPELINE"
echo "========================================"
echo ""

# 1. Kill background Python processes
echo "[1/2] 🐍 Stopping Python processes (Consumer & Producer)..."

pkill -f "python -m src.consumer.kafka_consumer"
pkill -f "python generate_transactions.py"

echo "✅ Python processes terminated."
echo ""

# 2. Tear down Docker containers
echo "[2/2] 🐳 Tearing down Docker containers..."
cd "$PROJECT_DIR" || exit

echo ""
read -p "🗑️  Type 'all' to delete ALL data volumes (PostgreSQL/Kafka), or just press Enter to keep data: " DELETE_CHOICE

if [[ "$DELETE_CHOICE" == "all" ]]; then
    echo "Tearing down containers AND wiping volumes (-v)..."
    docker-compose down -v
    echo ""
    echo "========================================"
    echo "✨ PIPELINE AND DATA SUCCESSFULLY CLEARED!"
    echo "========================================"
else
    echo "Tearing down containers (Data will be preserved)..."
    docker-compose down
    echo ""
    echo "========================================"
    echo "💤 PIPELINE STOPPED (Data preserved)."
    echo "========================================"
fi
