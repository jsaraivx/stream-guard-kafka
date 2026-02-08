"""
Kafka Producer for Stream-Guard-Kafka.
Sends fake transactions to Kafka topic in batch or streaming mode.
"""

import json
import logging
import time
from typing import Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.producer.fake_transaction_generator import FakeTransactionGenerator

logger = logging.getLogger(__name__)


class TransactionKafkaProducer:
    """Kafka producer for sending transactions to Kafka topic."""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions",
        **kwargs
    ):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker address
            topic: Kafka topic name
            **kwargs: Additional KafkaProducer configuration
        """
        self.topic = topic
        self.transaction_generator = FakeTransactionGenerator(locale="pt_BR")
        
        # Default producer configuration
        producer_config = {
            'bootstrap_servers': bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'max_in_flight_requests_per_connection': 1,  # Ensure ordering
        }
        
        # Override with custom config
        producer_config.update(kwargs)
        
        try:
            self.producer = KafkaProducer(**producer_config)
            logger.info(f"Kafka producer initialized for topic '{self.topic}'")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def send_transaction(self, transaction: Dict) -> bool:
        """
        Send a single transaction to Kafka.
        
        Args:
            transaction: Transaction dictionary
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use transaction_id as key for partitioning
            key = transaction.get('transaction_id', '')
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                key=key,
                value=transaction
            )
            
            # Wait for confirmation (blocking)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Sent transaction {transaction['transaction_id'][:8]}... "
                f"to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
            return False
    
    def send_batch(
        self,
        count: int = 100,
        fraud_probability: float = 0.15,
        flush: bool = True
    ) -> int:
        """
        Generate and send a batch of transactions.
        
        Args:
            count: Number of transactions to generate
            fraud_probability: Probability of fraudulent transactions
            flush: If True, flush producer after sending
            
        Returns:
            int: Number of successfully sent transactions
        """
        logger.info(f"Generating and sending {count} transactions...")
        
        # Generate batch
        transactions = self.transaction_generator.generate_batch(
            count=count,
            fraud_probability=fraud_probability
        )
        
        sent_count = 0
        
        # Send each transaction
        for txn in transactions:
            if self.send_transaction(txn):
                sent_count += 1
        
        # Flush to ensure all messages are sent
        if flush:
            self.producer.flush()
        
        logger.info(f"Successfully sent {sent_count}/{count} transactions")
        return sent_count
    
    def stream_transactions(
        self,
        transactions_per_second: int = 10,
        fraud_probability: float = 0.15,
        duration_seconds: Optional[int] = None,
        total_count: Optional[int] = None
    ) -> int:
        """
        Stream transactions continuously at a specified rate.
        
        Args:
            transactions_per_second: Rate of transaction generation
            fraud_probability: Probability of fraudulent transactions
            duration_seconds: Optional duration to run (None = infinite)
            total_count: Optional total number of transactions (None = infinite)
            
        Returns:
            int: Total number of transactions sent
        """
        logger.info(
            f"Starting transaction stream at {transactions_per_second} TPS "
            f"(duration: {duration_seconds}s, count: {total_count})"
        )
        
        interval = 1.0 / transactions_per_second
        sent_count = 0
        start_time = time.time()
        
        try:
            while True:
                # Check stopping conditions
                if total_count and sent_count >= total_count:
                    break
                
                if duration_seconds:
                    elapsed = time.time() - start_time
                    if elapsed >= duration_seconds:
                        break
                
                # Generate and send transaction
                transaction = self.transaction_generator.generate_transaction(
                    force_fraud=(
                        self.transaction_generator.faker.random.random() < fraud_probability
                    )
                )
                
                if self.send_transaction(transaction):
                    sent_count += 1
                    
                    # Log progress every 100 transactions
                    if sent_count % 100 == 0:
                        logger.info(f"Sent {sent_count} transactions...")
                
                # Sleep to maintain rate
                time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
        
        finally:
            self.producer.flush()
            logger.info(f"Stream completed. Total sent: {sent_count} transactions")
        
        return sent_count
    
    def stream_account_activity(
        self,
        account_id: str,
        transaction_count: int = 10,
        time_window_minutes: int = 60,
        include_velocity_fraud: bool = True
    ) -> int:
        """
        Stream transactions for a specific account (useful for velocity testing).
        
        Args:
            account_id: Account identifier
            transaction_count: Number of transactions to generate
            time_window_minutes: Time window for transactions
            include_velocity_fraud: If True, clusters some transactions
            
        Returns:
            int: Number of transactions sent
        """
        logger.info(
            f"Streaming {transaction_count} transactions for account {account_id}"
        )
        
        # Generate account activity
        transactions = self.transaction_generator.generate_account_activity(
            account_id=account_id,
            transaction_count=transaction_count,
            time_window_minutes=time_window_minutes,
            include_velocity_fraud=include_velocity_fraud
        )
        
        sent_count = 0
        
        # Send transactions in order
        for txn in transactions:
            if self.send_transaction(txn):
                sent_count += 1
            
            # Small delay between transactions
            time.sleep(0.1)
        
        self.producer.flush()
        logger.info(f"Sent {sent_count}/{len(transactions)} account transactions")
        
        return sent_count
    
    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


# Convenience functions
def send_batch_to_kafka(
    count: int = 100,
    fraud_probability: float = 0.15,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "transactions"
) -> int:
    """
    Quick function to send a batch of transactions to Kafka.
    
    Args:
        count: Number of transactions
        fraud_probability: Fraud probability
        bootstrap_servers: Kafka broker address
        topic: Kafka topic name
        
    Returns:
        int: Number of transactions sent
    """
    producer = TransactionKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )
    
    try:
        return producer.send_batch(count, fraud_probability)
    finally:
        producer.close()


def stream_to_kafka(
    transactions_per_second: int = 10,
    duration_seconds: Optional[int] = None,
    fraud_probability: float = 0.15,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "transactions"
) -> int:
    """
    Quick function to stream transactions to Kafka.
    
    Args:
        transactions_per_second: Rate of generation
        duration_seconds: Duration to run (None = infinite)
        fraud_probability: Fraud probability
        bootstrap_servers: Kafka broker address
        topic: Kafka topic name
        
    Returns:
        int: Number of transactions sent
    """
    producer = TransactionKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )
    
    try:
        return producer.stream_transactions(
            transactions_per_second=transactions_per_second,
            duration_seconds=duration_seconds,
            fraud_probability=fraud_probability
        )
    finally:
        producer.close()
