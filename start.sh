#!/bin/bash

# 1. Initial Setup
TODAY_DATE=$(date +"%Y-%m-%d_%H-%M-%S")
PROJECT_DIR="$HOME/projects/stream-guard-kafka"
LOG_DIR="$PROJECT_DIR/logs"

# Create logs directory
mkdir -p "$LOG_DIR"
LOG_CONSUMER="$LOG_DIR/consumer_$TODAY_DATE.log"
LOG_PRODUCER="$LOG_DIR/producer_$TODAY_DATE.log"

echo "========================================"
echo "🚀 STARTING STREAM-GUARD-KAFKA PIPELINE"
echo "========================================"

# 2. Build Infrastructure (Docker)
echo "----------------------------------------"
echo "[1/4] 🐳 Starting containers..."
cd $PROJECT_DIR || exit
docker-compose up -d

# 3. Wait for and Configure ksqlDB
echo "----------------------------------------"
echo "[2/4] ⏳ Waiting for ksqlDB Server to stabilize..."

# Checking loop until ksqlDB responds with 200 OK on port 8088
until curl --output /dev/null --silent --fail http://localhost:8088/info; do
    printf '.'
    sleep 2
done
echo -e "\n✅ ksqlDB Online! (Rules are automatically applied by the ksqldb-cli container)"

# 4. Start Python Processing (Virtual Environment)
echo "----------------------------------------"
echo "🐍 Activating virtual environment..."
source $PROJECT_DIR/.venv/bin/activate

echo "[3/4] 🛡️ Starting Kafka Consumer (in background)..."
# Runs in the background (&) and logs to file so it doesn't block the terminal
python -m src.consumer.kafka_consumer > "$LOG_CONSUMER" 2>&1 &
CONSUMER_PID=$!
echo "✅ Consumer running (PID: $CONSUMER_PID) | Logs saved to: $LOG_CONSUMER"

# Short sleep for consumer to create GroupID in the broker
sleep 3 

echo "[4/4] 💸 Starting Data Generator (in background)..."
python generate_transactions.py > "$LOG_PRODUCER" 2>&1 &
PRODUCER_PID=$!
echo "✅ Producer running (PID: $PRODUCER_PID) | Logs saved to: $LOG_PRODUCER"

# 5. Finalization and terminal instructions
echo "========================================"
echo "🎉 PIPELINE AND VIRTUALIZERS STARTED!"
echo "========================================"
echo ""
echo "📝 To see logs in real-time, use these commands in other terminal tabs:"
echo "   tail -f $LOG_CONSUMER"
echo "   tail -f $LOG_PRODUCER"
echo ""
echo "🛑 To stop the Python scripts run:"
echo "   kill $CONSUMER_PID $PRODUCER_PID"
echo "========================================"