# Fake Transaction Generator - Usage Guide

## 📝 Overview

The `FakeTransactionGenerator` module allows you to generate realistic synthetic banking transactions for testing the fraud detection system.

## 🚀 Quick Start

### Import
```python
from src.producer import FakeTransactionGenerator, generate_fake_transactions
```

### Simple Generation
```python
# Convenience function
transactions = generate_fake_transactions(count=100, fraud_probability=0.15)
```

## 🎯 Usage Examples

### 1. Individual Transaction
```python
generator = FakeTransactionGenerator(locale="pt_BR")

# Normal transaction
transaction = generator.generate_transaction()

# Fraudulent transaction (forced)
fraud_txn = generator.generate_transaction(force_fraud=True)

# Transaction for specific account
account_txn = generator.generate_transaction(account_id="ACC-12345678")
```

### 2. Batch Generation
```python
# Generate 100 transactions with 15% fraud probability
transactions = generator.generate_batch(
    count=100,
    fraud_probability=0.15
)
```

### 3. Time Series
```python
from datetime import datetime

# Generate transactions with sequential timestamps
transactions = generator.generate_time_series(
    count=50,
    start_time=datetime.utcnow(),
    time_interval_seconds=1,  # 1 transaction per second
    fraud_probability=0.2
)
```

### 4. Account Activity (Velocity Testing)
```python
# Generate multiple transactions for one account
# Useful for testing velocity-based fraud detection
transactions = generator.generate_account_activity(
    account_id="ACC-12345678",
    transaction_count=10,
    time_window_minutes=60,
    include_velocity_fraud=True  # Some transactions clustered together
)
```

### 5. Save to File
```python
transactions = generator.generate_batch(count=100)

# Save as JSON array
generator.save_to_file(transactions, "transactions.json", format="json")

# Save as JSON Lines (one line per transaction)
generator.save_to_file(transactions, "transactions.jsonl", format="jsonl")
```

## 📊 Transaction Format

Each generated transaction has the following format:

```json
{
  "transaction_id": "4a830f1a-2078-48eb-8c71-a62924a1a20a",
  "account_id": "ACC-62109056",
  "amount": 61.18,
  "timestamp": "2026-02-08T19:23:17.130908",
  "merchant": "Leão Teixeira Ltda.",
  "category": "electronics"
}
```

## 🎲 Transaction Categories

### Low Risk (50% of normal transactions)
- groceries, pharmacy, gas_station, restaurant, utilities

### Medium Risk (35% of normal transactions)
- clothing, entertainment, travel, home_improvement

### High Risk (15% of normal transactions, 60% of fraudulent)
- electronics, jewelry, luxury_goods, cryptocurrency, casino

## 💰 Amount Distribution

### Normal Transactions
- 60%: R$ 10 - R$ 200 (small)
- 30%: R$ 200 - R$ 1,000 (medium)
- 10%: R$ 1,000 - R$ 3,000 (large)

### Fraudulent Transactions
- 70%: R$ 3,000 - R$ 15,000 (above threshold)
- 30%: R$ 500 - R$ 3,000 (attempting to avoid detection)

## 🧪 Interactive CLI Menu

Run the generator manually to see all features:

```bash
python generate_transactions.py
```

The script will present a menu allowing you to:
1. Run Continuous Daemon Mode (Default Pipeline)
2. Send a Burst of X transactions instantly
3. Send X transactions uniformly (Specific TPS)
4. Stream random realistic bank volume for Y seconds

## 🔧 Main Parameters

### `FakeTransactionGenerator(locale, seed)`
- `locale`: Locale for fake data (default: "pt_BR")
- `seed`: Seed for reproducibility (optional)

### `generate_batch(count, fraud_probability)`
- `count`: Number of transactions to generate
- `fraud_probability`: Fraud probability (0.0 to 1.0)

### `generate_time_series(count, start_time, time_interval_seconds, fraud_probability)`
- `count`: Number of transactions
- `start_time`: Initial timestamp
- `time_interval_seconds`: Interval between transactions
- `fraud_probability`: Fraud probability

### `generate_account_activity(account_id, transaction_count, time_window_minutes, include_velocity_fraud)`
- `account_id`: Account ID
- `transaction_count`: Number of transactions
- `time_window_minutes`: Time window
- `include_velocity_fraud`: If True, clusters some transactions (fraud pattern)

## 📈 Next Steps

1. **Kafka Integration**: Create Producer that sends transactions to Kafka
2. **Consumer with Detection**: Implement fraud detection rules
3. **Complete Pipeline**: Test end-to-end flow

## 💡 Tips

- Use `seed` to generate reproducible data in tests
- Adjust `fraud_probability` to simulate different scenarios
- Use `generate_account_activity` to test velocity rule
- JSON Lines format is ideal for streaming
