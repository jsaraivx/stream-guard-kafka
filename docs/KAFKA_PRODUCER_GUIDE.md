# Kafka Producer Integration Guide

## 📊 Two Approaches Explained

### 1. **Batch Mode** (`send_batch`)
**When to use:** Testing, data backfill, controlled scenarios

```python
from src.producer.kafka_producer import send_batch_to_kafka

# Generate 100 transactions and send all at once
count = send_batch_to_kafka(
    count=100,
    fraud_probability=0.15,
    bootstrap_servers="localhost:9092",
    topic="transactions"
)
```

**How it works:**
1. Generates ALL transactions in memory first
2. Sends them to Kafka sequentially
3. Flushes at the end to ensure delivery

**Pros:**
- Simple and straightforward
- Good for testing
- Predictable behavior

**Cons:**
- Not realistic for production
- All transactions have similar timestamps
- High memory usage for large batches

---

### 2. **Streaming Mode** (`stream_transactions`)
**When to use:** Production, realistic simulation, continuous load

```python
from src.producer.kafka_producer import stream_to_kafka

# Stream at 10 transactions/second for 60 seconds
count = stream_to_kafka(
    transactions_per_second=10,
    duration_seconds=60,
    fraud_probability=0.15
)
```

**How it works:**
1. Generates ONE transaction at a time
2. Sends immediately to Kafka
3. Sleeps to maintain desired rate (TPS)
4. Repeats until duration/count limit

**Pros:**
- Realistic production simulation
- Controlled throughput (TPS)
- Low memory footprint
- Natural timestamp distribution

**Cons:**
- Slightly more complex
- Requires rate control logic

---

## 🎯 Recommended Approach

**For your fraud detection project, I recommend STREAMING MODE because:**

1. **Realistic Simulation**: Mimics real-world transaction flow
2. **Velocity Testing**: Proper timestamps for velocity-based fraud detection
3. **Scalability**: Can run indefinitely without memory issues
4. **Production-Ready**: Same pattern you'd use in real systems

---

## 💡 Implementation Details

### Class Structure

```python
class TransactionKafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        # Initialize Kafka producer
        # Initialize transaction generator
    
    def send_transaction(self, transaction):
        # Send single transaction
        # Returns True/False
    
    def send_batch(self, count, fraud_probability):
        # Generate batch → Send all → Flush
        # Returns count sent
    
    def stream_transactions(self, tps, duration):
        # Loop: Generate → Send → Sleep
        # Maintains rate (TPS)
        # Returns total sent
    
    def stream_account_activity(self, account_id, count):
        # Special mode for velocity testing
        # Generates clustered transactions
```

### Integration with FakeTransactionGenerator

```python
# Inside TransactionKafkaProducer.__init__
self.transaction_generator = FakeTransactionGenerator(locale="pt_BR")

# Batch mode uses:
transactions = self.transaction_generator.generate_batch(count, fraud_prob)

# Streaming mode uses:
transaction = self.transaction_generator.generate_transaction(force_fraud=...)
```

---

## 🚀 Usage Examples

### Example 1: Quick Batch Test
```python
from src.producer.kafka_producer import send_batch_to_kafka

# Send 50 transactions quickly
send_batch_to_kafka(count=50, fraud_probability=0.2)
```

### Example 2: Continuous Stream
```python
from src.producer.kafka_producer import stream_to_kafka

# Stream at 10 TPS for 5 minutes
stream_to_kafka(
    transactions_per_second=10,
    duration_seconds=300,
    fraud_probability=0.15
)
```

### Example 3: Custom Control
```python
from src.producer.kafka_producer import TransactionKafkaProducer

producer = TransactionKafkaProducer()

# Send individual transaction
txn = producer.transaction_generator.generate_transaction()
producer.send_transaction(txn)

# Send batch
producer.send_batch(count=100)

# Stream for 60 seconds
producer.stream_transactions(
    transactions_per_second=5,
    duration_seconds=60
)

producer.close()
```

### Example 4: Velocity Fraud Testing
```python
producer = TransactionKafkaProducer()

# Generate rapid transactions for one account
# Some will be clustered (< 1 minute apart)
producer.stream_account_activity(
    account_id="ACC-12345678",
    transaction_count=10,
    time_window_minutes=5,
    include_velocity_fraud=True
)
```

---

## 🔧 Configuration

### Kafka Producer Settings

```python
producer = TransactionKafkaProducer(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    # Optional Kafka configs:
    acks='all',  # Wait for all replicas
    retries=3,   # Retry failed sends
    max_in_flight_requests_per_connection=1  # Ensure ordering
)
```

### Transaction Generation Settings

```python
# Controlled via FakeTransactionGenerator
generator = FakeTransactionGenerator(
    locale="pt_BR",  # Brazilian Portuguese fake data
    seed=42          # For reproducibility
)
```

---

## 📈 Performance Considerations

### Batch Mode
- **Throughput**: Limited by network + Kafka write speed
- **Memory**: O(n) where n = batch size
- **Latency**: All messages sent quickly, then flush

### Streaming Mode
- **Throughput**: Controlled by `transactions_per_second`
- **Memory**: O(1) - only one transaction in memory
- **Latency**: Consistent, predictable

---

## 🧪 Testing the Producer

### Prerequisites
```bash
# Start Kafka
docker-compose up -d

# Verify Kafka is running
docker ps | grep kafka
```

### Run Interactive Generator
```bash
# Interactive Generator Menu
python generate_transactions.py

# Quick batch test
python -c "from src.producer.kafka_producer import send_batch_to_kafka; \
           send_batch_to_kafka(count=10)"
```

### Verify Messages
1. **Kafka-UI**: http://localhost:8080
   - Navigate to Topics → transactions
   - View messages

2. **Command Line**:
```bash
docker exec -it stream-guard-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning \
  --max-messages 10
```

---

## 🎯 Recommendation Summary

| Scenario | Use | Why |
|----------|-----|-----|
| **Quick testing** | `send_batch` | Fast, simple |
| **Production simulation** | `stream_transactions` | Realistic, scalable |
| **Velocity testing** | `stream_account_activity` | Clustered transactions |
| **Custom scenarios** | `TransactionKafkaProducer` class | Full control |

**For your fraud detection MVP, start with `stream_transactions` at 10 TPS.**

---

## 🔄 Next Steps

1. **Test Producer**: Run `python generate_transactions.py` and select an option
2. **Implement Consumer**: Create fraud detector
3. **Add Detection Rules**: Amount, time, velocity
4. **End-to-End Test**: Producer → Kafka → Consumer → PostgreSQL
