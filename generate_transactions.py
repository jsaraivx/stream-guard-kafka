"""
Stream-Guard-Kafka — Transaction Generator
Generates 3000 fake transactions per batch and publishes to Kafka.
Runs continuously until interrupted (Ctrl+C).
Some transactions will naturally trigger ksqlDB fraud rules.
"""

import json
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from uuid import uuid4

from kafka import KafkaProducer
from faker import Faker

from src.config import settings

fake = Faker("pt_BR")

# ============================================================================
# Configuration (loaded from .env via settings)
# ============================================================================
KAFKA_BOOTSTRAP = settings.kafka.bootstrap_servers
TRANSACTIONS_TOPIC = settings.kafka.transactions_topic
BATCH_SIZE = settings.producer.batch_size
DELAY_BETWEEN_BATCHES = settings.producer.delay_between_batches

# Fraud-triggering probability weights
FRAUD_CHANCE = settings.producer.fraud_probability

CATEGORIES = [
    "food", "transport", "health", "education",
    "entertainment", "electronics", "jewelry",
    "clothing", "travel", "services", "cryptocurrency"
]

HIGH_RISK_CATEGORIES = ["electronics", "jewelry", "cryptocurrency", "travel"]

MERCHANTS = {
    "food": ["iFood", "Rappi", "Uber Eats", "McDonald's", "Burger King"],
    "transport": ["Uber", "99", "Shell", "Ipiranga", "Localiza"],
    "health": ["Drogasil", "Raia", "Unimed", "Hapvida", "Fleury"],
    "education": ["Udemy", "Coursera", "Hotmart", "Alura", "Descomplica"],
    "entertainment": ["Netflix", "Spotify", "Steam", "PlayStation Store", "Cinema"],
    "electronics": ["Kabum", "Pichau", "Amazon", "Mercado Livre", "AliExpress"],
    "jewelry": ["Vivara", "Pandora", "Tiffany", "H.Stern", "Monte Carlo"],
    "clothing": ["Renner", "C&A", "Zara", "Nike", "Shein"],
    "travel": ["Latam", "Gol", "Booking", "Airbnb", "Decolar"],
    "services": ["AWS", "Google Cloud", "DigitalOcean", "Vercel", "Heroku"],
    "cryptocurrency": ["Binance", "Mercado Bitcoin", "Foxbit", "Coinbase", "Bybit"],
}

ACCOUNTS = [f"ACC-{fake.random_int(min=1000, max=9999)}" for _ in range(50)]


# ============================================================================
# Transaction generator
# ============================================================================
def generate_transaction() -> dict:
    """Generate a single fake transaction. Some will trigger fraud rules."""

    is_suspicious = random.random() < FRAUD_CHANCE

    if is_suspicious:
        return _generate_suspicious_transaction()
    return _generate_normal_transaction()


def _generate_normal_transaction() -> dict:
    """Normal transaction: low amount, business hours, common categories."""
    category = random.choice(CATEGORIES)
    hour = random.randint(7, 22)

    return {
        "transaction_id": str(uuid4()),
        "account_id": random.choice(ACCOUNTS),
        "amount": round(random.uniform(10.0, 2500.0), 2),
        "timestamp": _random_timestamp(hour),
        "merchant": random.choice(MERCHANTS[category]),
        "category": category,
    }


def _generate_suspicious_transaction() -> dict:
    """Suspicious transaction: designed to trigger ksqlDB fraud rules."""
    fraud_type = random.choice(["high_amount", "night", "velocity", "combined"])

    account_id = random.choice(ACCOUNTS)
    category = random.choice(CATEGORIES)

    if fraud_type == "high_amount":
        # Rule 1: amount > 5000
        return {
            "transaction_id": str(uuid4()),
            "account_id": account_id,
            "amount": round(random.uniform(5001.0, 50000.0), 2),
            "timestamp": _random_timestamp(random.randint(0, 23)),
            "merchant": random.choice(MERCHANTS[category]),
            "category": category,
        }

    elif fraud_type == "night":
        # Rule 2: transaction between 22:00 and 06:00
        hour = random.choice([0, 1, 2, 3, 4, 5, 22, 23])
        category = random.choice(HIGH_RISK_CATEGORIES)
        return {
            "transaction_id": str(uuid4()),
            "account_id": account_id,
            "amount": round(random.uniform(500.0, 4000.0), 2),
            "timestamp": _random_timestamp(hour),
            "merchant": random.choice(MERCHANTS[category]),
            "category": category,
        }

    elif fraud_type == "velocity":
        # Rule 3: same account, multiple transactions (will count in window)
        return {
            "transaction_id": str(uuid4()),
            "account_id": ACCOUNTS[0],  # always same account to trigger velocity
            "amount": round(random.uniform(100.0, 1000.0), 2),
            "timestamp": _now_timestamp(),
            "merchant": random.choice(MERCHANTS[category]),
            "category": category,
        }

    else:
        # Combined: high amount + night + high risk category
        hour = random.choice([0, 1, 2, 3, 4, 5])
        category = random.choice(HIGH_RISK_CATEGORIES)
        return {
            "transaction_id": str(uuid4()),
            "account_id": account_id,
            "amount": round(random.uniform(8000.0, 30000.0), 2),
            "timestamp": _random_timestamp(hour),
            "merchant": random.choice(MERCHANTS[category]),
            "category": category,
        }


def _random_timestamp(hour: int) -> str:
    """Generate timestamp for today at given hour."""
    now = datetime.now()
    ts = now.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")


def _now_timestamp() -> str:
    """Generate timestamp for right now (used for velocity triggers)."""
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")


# ============================================================================
# Kafka producer
# ============================================================================


def produce_batch(producer: KafkaProducer, batch_num: int) -> dict:
    """Generate and send a batch of 3000 transactions."""
    stats = {"total": 0, "suspicious": 0, "normal": 0}

    print(f"\n{'='*60}")
    print(f"📦 Batch #{batch_num} — Generating {BATCH_SIZE} transactions...")
    print(f"{'='*60}")

    for i in range(BATCH_SIZE):
        tx = generate_transaction()

        # Count suspicious (high amount or night)
        is_sus = tx["amount"] > 5000 or _is_night_hour(tx["timestamp"])
        if is_sus:
            stats["suspicious"] += 1
        else:
            stats["normal"] += 1
        stats["total"] += 1

        producer.send(
            TRANSACTIONS_TOPIC,
            key=tx["transaction_id"].encode("utf-8"),
            value=json.dumps(tx).encode("utf-8"),
        )

        # Flush every 500 messages
        if (i + 1) % 500 == 0:
            producer.flush()
            print(f"  ✅ Sent {i + 1}/{BATCH_SIZE}")

    producer.flush()
    return stats


def _is_night_hour(timestamp: str) -> bool:
    hour = int(timestamp[11:13])
    return hour >= 22 or hour < 6


def run_daemon_mode(producer: KafkaProducer):
    """Original infinite loop for background execution."""
    print("\n🔄 Running in Continuous Daemon Mode...")
    batch_num = 0
    total_sent = 0
    total_suspicious = 0
    
    for i in range(10):
        batch_num += 1
        stats = produce_batch(producer, batch_num)
        total_sent += stats["total"]
        total_suspicious += stats["suspicious"]

        print(f"\n📊 Batch #{batch_num} Summary:")
        print(f"   Normal:     {stats['normal']}")
        print(f"   Suspicious: {stats['suspicious']}")
        print(f"   Total sent: {total_sent}")
        print(f"\n⏳ Waiting {DELAY_BETWEEN_BATCHES}s before next batch...")
        time.sleep(DELAY_BETWEEN_BATCHES)

def send_burst(producer: KafkaProducer, count: int):
    """Send X transactions as fast as possible."""
    print(f"\n🚀 Sending a burst of {count} transactions...")
    for i in range(count):
        tx = generate_transaction()
        producer.send(
            TRANSACTIONS_TOPIC,
            key=tx["transaction_id"].encode("utf-8"),
            value=json.dumps(tx).encode("utf-8"),
        )
        if (i + 1) % 500 == 0:
            producer.flush()
            print(f"  ✅ Sent {i + 1}/{count}")
    producer.flush()
    print(f"✅ Burst complete! {count} transactions sent.")

def send_uniform(producer: KafkaProducer, count: int, tps: int):
    """Send X transactions at a steady TPS rate."""
    print(f"\n🚀 Sending {count} transactions uniformly at {tps} TPS...")
    sleep_time = 1.0 / tps
    for i in range(count):
        tx = generate_transaction()
        producer.send(
            TRANSACTIONS_TOPIC,
            key=tx["transaction_id"].encode("utf-8"),
            value=json.dumps(tx).encode("utf-8"),
        )
        if (i + 1) % max(1, tps) == 0:
            producer.flush()
            print(f"  ✅ Sent {i + 1}/{count}")
        time.sleep(sleep_time)
    producer.flush()
    print(f"✅ Complete! {count} transactions sent.")

def stream_random_volume(producer: KafkaProducer, duration_sec: int):
    """Stream transactions with randomized realistic banking delays."""
    print(f"\n🚀 Streaming random bank volume for {duration_sec} seconds...")
    start_time = time.time()
    count = 0
    while time.time() - start_time < duration_sec:
        tx = generate_transaction()
        producer.send(
            TRANSACTIONS_TOPIC,
            key=tx["transaction_id"].encode("utf-8"),
            value=json.dumps(tx).encode("utf-8"),
        )
        count += 1
        
        # Simulate realistic load: mostly fast spikes, some slow moments
        if random.random() < 0.8:
            time.sleep(random.uniform(0.005, 0.05)) # Fast burst
        else:
            time.sleep(random.uniform(0.1, 0.4))    # Slow moment
            
        if count % 100 == 0:
            producer.flush()
            print(f"  ✅ Sent {count} transactions so far...")

    producer.flush()
    print(f"✅ Time's up! Total sent: {count} transactions.")

def interactive_menu(producer: KafkaProducer):
    """Interactive CLI menu for the user."""
    while True:
        print("\n" + "="*60)
        print("🎮 INTERACTIVE TRANSACTION GENERATOR")
        print("="*60)
        print("  1 - 🏃 Run Continuous Daemon Mode (Default Pipeline)")
        print("  2 - 💥 Send a Burst of X transactions instantly")
        print("  3 - ⏱️  Send X transactions uniformly (Specific TPS)")
        print("  4 - 🌊 Stream random realistic bank volume for Y seconds")
        print("  0 - ❌ Exit")
        print("="*60)
        
        choice = input("\nEnter your choice (0-4): ").strip()
        
        if choice == "0":
            break
        elif choice == "1":
            run_daemon_mode(producer)
            break
        elif choice == "2":
            try:
                count = int(input("How many transactions to send? (e.g. 1000): "))
                send_burst(producer, count)
            except ValueError:
                print("⚠️ Invalid number!")
        elif choice == "3":
            try:
                count = int(input("How many transactions to send? (e.g. 500): "))
                tps = int(input("At how many Transactions Per Second (TPS)? (e.g. 10): "))
                send_uniform(producer, count, tps)
            except ValueError:
                print("⚠️ Invalid number!")
        elif choice == "4":
            try:
                duration = int(input("For how many seconds? (e.g. 30 or 60): "))
                stream_random_volume(producer, duration)
            except ValueError:
                print("⚠️ Invalid number!")
        else:
            print("⚠️ Invalid choice!")

# ============================================================================
# Main
# ============================================================================
def main():
    print("🚀 Stream-Guard-Kafka — Transaction Generator")
    print(f"   Kafka: {KAFKA_BOOTSTRAP}")
    print(f"   Topic: {TRANSACTIONS_TOPIC}")
    print(f"   Fraud chance: {FRAUD_CHANCE * 100}%")
    print(f"   Press Ctrl+C to stop\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        retries=3,
    )

    def shutdown(sig, frame):
        print(f"\n\n🛑 Shutting down generator...")
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    # If args are passed or output is piped (like in start.sh), run as daemon
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        run_daemon_mode(producer)
    elif not sys.stdout.isatty():
        run_daemon_mode(producer)
    else:
        try:
            interactive_menu(producer)
        except KeyboardInterrupt:
            shutdown(None, None)

if __name__ == "__main__":
    main()
