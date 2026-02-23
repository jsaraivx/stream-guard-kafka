# Stream-Guard-Kafka 🛡️

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Kafka](https://img.shields.io/badge/Kafka-3.6-black.svg)
![ksqlDB](https://img.shields.io/badge/ksqlDB-0.29-orange.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)

> **Real-time fraud detection system for financial transactions using Hybrid Architecture (Kappa Architecture).**

## 🎯 Objective

Simulate a high-performance Data Engineering pipeline where banking transactions are ingested, analyzed, and stored in real-time. The project demonstrates the use of **Declarative Stream Processing (ksqlDB)** for temporal window rules and **Imperative Processing (Python)** for complex business logic and persistence.

## 🏗️ Architecture

The system uses a hybrid pattern to maximize performance:

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Producer  │─────▶│    Kafka    │◀────▶│   ksqlDB    │
│   (Faker)   │      │   Broker    │      │   Server    │
└─────────────┘      └──────┬──────┘      └──────┬──────┘
                            │                     │
                            │ Topic: fraud_alerts │
                            ▼                     ▼
                     ┌─────────────┐      ┌─────────────┐
                     │  Consumer   │      │  Kafka-UI   │
                     │  (Python)   │      │ (Monitor)   │
                     └──────┬──────┘      └─────────────┘
                            │
                            ▼
                     ┌─────────────┐
                     │ PostgreSQL  │
                     │  Database   │
                     └─────────────┘
```

### Data Flow

1. **Ingestion:** The `Producer Service` generates synthetic transactions (JSON) and publishes to the `transactions` topic.

2. **Stream Processing (ksqlDB):** ksqlDB consumes the raw stream and applies:
   - Immediate filters (e.g., High Amount)
   - Window Aggregations (Windowing) to detect bot behavior (Velocity)
   - Publishes only anomalies to the `fraud_alerts` topic

3. **Business Logic (Python):** The `Consumer Service` listens to the alerts topic, enriches data if necessary, and decides the final action.

4. **Storage (PostgreSQL):** Persistent storage for auditing and analytical dashboards.

## 🚀 Tech Stack

- **Language:** Python 3.10+ (Static Typing via `pydantic`)
- **Messaging:** Apache Kafka 3.6 (**KRaft** Mode - No ZooKeeper)
- **Stream Processing:** ksqlDB (Confluent)
- **Database:** PostgreSQL 15
- **Infrastructure:** Docker & Docker Compose
- **Main Libraries:**
  - `kafka-python`: Low-latency Kafka client
  - `faker`: Realistic data generation
  - `sqlalchemy`: ORM for persistence

## 🔍 Detection Strategy (Hybrid)

The logic was divided to maximize performance and demonstrate proper use of each tool:

| **Fraud Type** | **Business Rule** | **Responsible Technology** | **Why?** |
|----------------|-------------------|---------------------------|----------|
| **High Amount** | Transactions > R$ 3,000.00 | **ksqlDB** | Simple filtering (`WHERE`) is instantaneous in SQL |
| **High Velocity** | > 3 transactions from same account in 1 minute | **ksqlDB** | Window Aggregation (`WINDOW TUMBLING`) is native and performant in ksqlDB, avoiding complex state management in Python |
| **Blacklist / Context** | Specific business logic | **Python** | Allows external queries and complex conditional logic before saving |

## ⚙️ Configuration and Execution

### Prerequisites

- Docker 20.10+ & Docker Compose
- Python 3.10+
- Git

### Step by Step

1. **Clone the repository:**
```bash
git clone https://github.com/your-username/stream-guard-kafka.git
cd stream-guard-kafka
```

2. **Virtual Environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or .\venv\Scripts\activate  # Windows
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Start Infrastructure & Background Pipeline:**
```bash
# Starts Docker, configures ksqlDB, and runs Python consumers/producers in background.
# You will be prompted at the end to view live logs.
./start.sh
```

5. **Stop Infrastructure & Clean Data:**
```bash
# Stops Python background processes and Docker containers.
# You will be prompted if you want to wipe PostgreSQL/Kafka data volumes.
./stop.sh
```

5. **Access Services:**
   - **Kafka-UI:** http://localhost:8080 (Topic Monitoring)
   - **PostgreSQL:** `localhost:5432` (User: `streamguard` / Pass: `streamguard_2024`)

## 📁 Project Structure

```
stream-guard-kafka/
├── src/
│   ├── consumer/        # Python Consumer logic
│   ├── producer/        # Data generator script (Faker)
│   ├── models/          # Pydantic Schemas (Data Contract)
│   ├── database/        # Postgres connection
│   └── config/          # Centralized settings
├── ksqldb/
│   └── queries.sql      # Stream and Table creation scripts
├── docker/
│   └── init-db.sql      # Initial Postgres schema
├── tests/               # Unit tests
├── docs/                # Documentation
├── docker-compose.yaml  # Infrastructure (Kafka, ksqlDB, Postgres)
├── requirements.txt
└── README.md
```

## 🧪 Quick Start

### 1. Generate Fake Transactions
```bash
# Run the interactive generator (Menu for Batch, Stream, Velocity)
python generate_transactions.py
```

### 2. Monitor with Kafka-UI
Access http://localhost:8080 to view:
- Topics and messages
- Consumer groups
- Throughput metrics

### 3. Query with ksqlDB
```bash
# Access ksqlDB CLI
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

# Inside CLI
SHOW TOPICS;
SELECT * FROM transactions EMIT CHANGES;
```

## 📊 Monitoring and Useful Commands

### Kafka Commands
```bash
# List topics
docker exec -it stream-guard-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Consume messages
docker exec -it stream-guard-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning
```

### PostgreSQL Queries
```sql
-- Total transactions
SELECT COUNT(*) FROM transactions;

-- Fraud rate
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as frauds,
  ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as fraud_rate
FROM transactions;

-- Suspicious accounts
SELECT * FROM account_risk_profile
WHERE fraud_rate > 50
ORDER BY fraud_rate DESC;
```

## 🔮 Roadmap

- [x] Docker Infrastructure (Kafka KRaft + Postgres)
- [x] Python Producer (Faker)
- [x] Fake Transaction Generator with realistic distributions
- [x] Kafka Producer with batch and streaming modes
- [x] ksqlDB Queries implementation (Streams & Tables)
- [x] Python Consumer for persistence
- [ ] Dashboard in Streamlit/PowerBI
- [ ] CI/CD with GitHub Actions
- [ ] Migration to Serverless (Upstash + Cloud Run)

## 📚 Documentation

- [Fake Generator Guide](docs/FAKE_GENERATOR_GUIDE.md) - How to generate synthetic transactions
- [Kafka Producer Guide](docs/KAFKA_PRODUCER_GUIDE.md) - Batch vs Streaming modes explained
- [Quick Start Guide](QUICKSTART.md) - Infrastructure setup and testing

## 🎓 Learning Objectives

This project demonstrates:
- ✅ **Kappa Architecture** for real-time processing
- ✅ **Hybrid Processing**: Declarative (ksqlDB) + Imperative (Python)
- ✅ **Stream Processing** with windowing and aggregations
- ✅ **Event-Driven Architecture** with Kafka
- ✅ **Data Modeling** with Pydantic schemas
- ✅ **Clean Code** principles (SOLID, type hints)
- ✅ **Infrastructure as Code** with Docker Compose

## 📝 License

MIT License
