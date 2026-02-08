#!/usr/bin/env python3
"""
Demo script for FakeTransactionGenerator.
Demonstrates different ways to generate fake transaction data.
"""

import json
from datetime import datetime, timedelta

from src.producer import FakeTransactionGenerator


def demo_basic_generation():
    """Demo: Generate basic transactions."""
    print("=" * 60)
    print("DEMO 1: Basic Transaction Generation")
    print("=" * 60)
    
    generator = FakeTransactionGenerator(locale="pt_BR")
    
    # Generate a single transaction
    transaction = generator.generate_transaction()
    print("\n📝 Individual Transaction:")
    print(generator.to_json(transaction))
    
    # Generate a fraudulent transaction
    fraud_transaction = generator.generate_transaction(force_fraud=True)
    print("\n⚠️  Fraudulent Transaction (forced):")
    print(generator.to_json(fraud_transaction))


def demo_batch_generation():
    """Demo: Generate batch of transactions."""
    print("\n" + "=" * 60)
    print("DEMO 2: Batch Generation")
    print("=" * 60)
    
    generator = FakeTransactionGenerator(locale="pt_BR")
    
    # Generate 10 transactions with 30% fraud probability
    transactions = generator.generate_batch(count=10, fraud_probability=0.3)
    
    print(f"\n📦 Generated {len(transactions)} transactions:")
    for i, txn in enumerate(transactions, 1):
        print(f"\n{i}. {txn['transaction_id'][:8]}... | "
              f"R$ {txn['amount']:>8.2f} | "
              f"{txn['category']:<20} | "
              f"{txn['merchant'][:30]}")


def demo_time_series():
    """Demo: Generate time series transactions."""
    print("\n" + "=" * 60)
    print("DEMO 3: Time Series Transactions")
    print("=" * 60)
    
    generator = FakeTransactionGenerator(locale="pt_BR")
    
    # Generate transactions with 5-second intervals
    start_time = datetime.utcnow()
    transactions = generator.generate_time_series(
        count=5,
        start_time=start_time,
        time_interval_seconds=5,
        fraud_probability=0.2
    )
    
    print("\n⏰ Transactions with sequential timestamps:")
    for txn in transactions:
        print(f"  {txn['timestamp']} | R$ {txn['amount']:>8.2f} | {txn['category']}")


def demo_account_activity():
    """Demo: Generate activity for a specific account."""
    print("\n" + "=" * 60)
    print("DEMO 4: Specific Account Activity")
    print("=" * 60)
    
    generator = FakeTransactionGenerator(locale="pt_BR")
    
    account_id = "ACC-12345678"
    
    # Generate 8 transactions for the account within 60 minutes
    # Some may be clustered (velocity fraud pattern)
    transactions = generator.generate_account_activity(
        account_id=account_id,
        transaction_count=8,
        time_window_minutes=60,
        include_velocity_fraud=True
    )
    
    print(f"\n👤 Account activity for {account_id}:")
    print(f"   Total transactions: {len(transactions)}")
    
    # Check for velocity patterns
    for i in range(1, len(transactions)):
        prev_time = datetime.fromisoformat(transactions[i-1]['timestamp'])
        curr_time = datetime.fromisoformat(transactions[i]['timestamp'])
        time_diff = (curr_time - prev_time).total_seconds()
        
        marker = "⚡" if time_diff < 60 else "  "
        print(f"   {marker} {transactions[i]['timestamp']} | "
              f"R$ {transactions[i]['amount']:>8.2f} | "
              f"Δt: {time_diff:.0f}s")


def demo_save_to_file():
    """Demo: Save transactions to files."""
    print("\n" + "=" * 60)
    print("DEMO 5: Save to Files")
    print("=" * 60)
    
    generator = FakeTransactionGenerator(locale="pt_BR")
    
    # Generate 20 transactions
    transactions = generator.generate_batch(count=20, fraud_probability=0.15)
    
    # Save as JSON array
    json_file = "sample_transactions.json"
    generator.save_to_file(transactions, json_file, format="json")
    print(f"\n💾 Saved {len(transactions)} transactions to: {json_file}")
    
    # Save as JSON Lines (one JSON per line)
    jsonl_file = "sample_transactions.jsonl"
    generator.save_to_file(transactions, jsonl_file, format="jsonl")
    print(f"💾 Saved {len(transactions)} transactions to: {jsonl_file}")


def demo_statistics():
    """Demo: Generate and analyze statistics."""
    print("\n" + "=" * 60)
    print("DEMO 6: Generated Transaction Statistics")
    print("=" * 60)
    
    generator = FakeTransactionGenerator(locale="pt_BR")
    
    # Generate 100 transactions
    transactions = generator.generate_batch(count=100, fraud_probability=0.15)
    
    # Calculate statistics
    total = len(transactions)
    amounts = [t['amount'] for t in transactions]
    categories = [t['category'] for t in transactions]
    
    # Amount statistics
    avg_amount = sum(amounts) / len(amounts)
    min_amount = min(amounts)
    max_amount = max(amounts)
    
    # High-value transactions (potential fraud indicator)
    high_value_threshold = 3000.0
    high_value_count = sum(1 for a in amounts if a > high_value_threshold)
    
    # Category distribution
    category_counts = {}
    for cat in categories:
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    print(f"\n📊 Statistics for {total} transactions:")
    print(f"\n   Amounts:")
    print(f"     Average: R$ {avg_amount:>10.2f}")
    print(f"     Minimum: R$ {min_amount:>10.2f}")
    print(f"     Maximum: R$ {max_amount:>10.2f}")
    print(f"     > R$ {high_value_threshold:.0f}: {high_value_count} ({high_value_count/total*100:.1f}%)")
    
    print(f"\n   Top 5 Categories:")
    for cat, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"     {cat:<20}: {count:>3} ({count/total*100:.1f}%)")


def main():
    """Run all demos."""
    print("\n🎲 FAKE TRANSACTION GENERATOR DEMONSTRATION")
    print("=" * 60)
    
    demo_basic_generation()
    demo_batch_generation()
    demo_time_series()
    demo_account_activity()
    demo_save_to_file()
    demo_statistics()
    
    print("\n" + "=" * 60)
    print("✅ Demonstration completed!")
    print("=" * 60)
    print("\n💡 Next steps:")
    print("   1. Integrate with Kafka Producer")
    print("   2. Create Consumer with fraud detection")
    print("   3. Test end-to-end pipeline")


if __name__ == "__main__":
    main()
