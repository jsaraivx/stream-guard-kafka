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

# 2. Tear down Docker containers and wipe volumes
echo "[2/2] 🐳 Tearing down Docker containers and wiping volumes (-v)..."
cd "$PROJECT_DIR" || exit
docker-compose down -v

echo ""
echo "========================================"
echo "✨ PIPELINE AND DATA SUCCESSFULLY CLEARED!"
echo "========================================"
