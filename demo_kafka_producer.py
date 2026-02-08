#!/usr/bin/env python3
"""
Demo script for Kafka Producer.
Shows different ways to send transactions to Kafka.
"""

import logging
import sys

from src.producer.kafka_producer import (
    TransactionKafkaProducer,
    send_batch_to_kafka,
    stream_to_kafka
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def demo_batch_mode():
    """Demo: Send a batch of transactions."""
    print("\n" + "=" * 60)
    print("DEMO 1: Batch Mode - Send 50 transactions at once")
    print("=" * 60)
    
    try:
        count = send_batch_to_kafka(
            count=50,
            fraud_probability=0.15,
            bootstrap_servers="localhost:29092",
            topic="transactions"
        )
        print(f"\n✅ Successfully sent {count} transactions in batch mode")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("💡 Make sure Kafka is running: docker-compose up -d")


def demo_streaming_mode():
    """Demo: Stream transactions continuously."""
    print("\n" + "=" * 60)
    print("DEMO 2: Streaming Mode - 10 TPS for 30 seconds")
    print("=" * 60)
    print("Press Ctrl+C to stop early\n")
    
    try:
        count = stream_to_kafka(
            transactions_per_second=10,
            duration_seconds=30,
            fraud_probability=0.15,
            bootstrap_servers="localhost:29092",
            topic="transactions"
        )
        print(f"\n✅ Successfully streamed {count} transactions")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("💡 Make sure Kafka is running: docker-compose up -d")


def demo_account_velocity():
    """Demo: Stream account activity for velocity testing."""
    print("\n" + "=" * 60)
    print("DEMO 3: Account Velocity - Rapid transactions for one account")
    print("=" * 60)
    
    try:
        producer = TransactionKafkaProducer(
            bootstrap_servers="localhost:29092",
            topic="transactions"
        )
        
        count = producer.stream_account_activity(
            account_id="ACC-VELOCITY-TEST",
            transaction_count=15,
            time_window_minutes=5,
            include_velocity_fraud=True
        )
        
        producer.close()
        print(f"\n✅ Successfully sent {count} account transactions")
        print("💡 Some transactions were clustered to trigger velocity fraud detection")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("💡 Make sure Kafka is running: docker-compose up -d")


def demo_custom_producer():
    """Demo: Custom producer with fine control."""
    print("\n" + "=" * 60)
    print("DEMO 4: Custom Producer - Manual control")
    print("=" * 60)
    
    try:
        # Create producer
        producer = TransactionKafkaProducer(
            bootstrap_servers="localhost:29092",
            topic="transactions"
        )
        
        # Send individual transactions
        print("\nSending 5 individual transactions...")
        for i in range(5):
            txn = producer.transaction_generator.generate_transaction()
            success = producer.send_transaction(txn)
            if success:
                print(f"  ✓ Sent: {txn['transaction_id'][:8]}... | "
                      f"R$ {txn['amount']:.2f} | {txn['category']}")
        
        # Send a batch
        print("\nSending batch of 10 transactions...")
        batch_count = producer.send_batch(count=10, fraud_probability=0.3)
        print(f"  ✓ Batch sent: {batch_count} transactions")
        
        # Stream for 10 seconds
        print("\nStreaming at 5 TPS for 10 seconds...")
        stream_count = producer.stream_transactions(
            transactions_per_second=5,
            duration_seconds=10,
            fraud_probability=0.2
        )
        print(f"  ✓ Stream sent: {stream_count} transactions")
        
        producer.close()
        print("\n✅ Custom producer demo completed")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("💡 Make sure Kafka is running: docker-compose up -d")


def main():
    """Run all demos."""
    print("\n🚀 KAFKA PRODUCER DEMONSTRATION")
    print("=" * 60)
    print("\n⚠️  Prerequisites:")
    print("   1. Kafka must be running: docker-compose up -d")
    print("   2. Topic 'transactions' will be auto-created")
    print("   3. Press Ctrl+C to interrupt streaming demos")
    
    # Ask user which demo to run
    print("\n" + "=" * 60)
    print("Select demo to run:")
    print("  1 - Batch Mode (50 transactions)")
    print("  2 - Streaming Mode (10 TPS for 30s)")
    print("  3 - Account Velocity Test")
    print("  4 - Custom Producer (all features)")
    print("  5 - Run all demos")
    print("=" * 60)
    
    choice = input("\nEnter choice (1-5): ").strip()
    
    if choice == "1":
        demo_batch_mode()
    elif choice == "2":
        demo_streaming_mode()
    elif choice == "3":
        demo_account_velocity()
    elif choice == "4":
        demo_custom_producer()
    elif choice == "5":
        demo_batch_mode()
        demo_streaming_mode()
        demo_account_velocity()
        demo_custom_producer()
    else:
        print("Invalid choice!")
        return
    
    print("\n" + "=" * 60)
    print("✅ Demo completed!")
    print("=" * 60)
    print("\n💡 Next steps:")
    print("   1. Check Kafka-UI: http://localhost:8080")
    print("   2. Implement Consumer with fraud detection")
    print("   3. Test end-to-end pipeline")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
        sys.exit(0)
