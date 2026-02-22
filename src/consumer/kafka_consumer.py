"""
Kafka Consumer for Stream-Guard-Kafka.
Reads from fraud_alerts topic, applies extra Python rules, and saves to PostgreSQL.
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaConsumer

from src.config import settings
from src.database.manager import db_manager
from src.services.fraud_engine import FraudEngine

logger = logging.getLogger(__name__)


class TransactionKafkaConsumer:
    """Consumes fraud alerts from ksqlDB, applies Python rules, and persists."""

    def __init__(self):
        """Initialize Kafka consumer with centralized settings."""
        self.consumer = KafkaConsumer(
            settings.kafka.fraud_alerts_topic,
            settings.kafka.transactions_topic,
            bootstrap_servers=settings.kafka.bootstrap_servers,
            auto_offset_reset=settings.kafka.auto_offset_reset,
            enable_auto_commit=settings.kafka.enable_auto_commit,
            group_id=settings.kafka.consumer_group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        
        # Initialize Python Fraud Engine
        self.fraud_engine = FraudEngine(
            MAX_AMOUNT=settings.fraud_detection.high_amount_threshold,
            BLACKLISTED_MERCHANTS=["AliExpress", "Shein"],
            BLACKLISTED_ACCOUNTS=["ACC-9999"]
        )

    def consume_stream(self) -> None:
        """Continuously consume messages from both topics."""
        logger.info(f"Started consuming from topics: {settings.kafka.fraud_alerts_topic}, {settings.kafka.transactions_topic}")
        
        try:
            for message in self.consumer:
                if message.topic == settings.kafka.fraud_alerts_topic:
                    self.process_fraud_alert(message.value)
                elif message.topic == settings.kafka.transactions_topic:
                    self.process_raw_transaction(message.value)
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user.")
        finally:
            self.consumer.close()
            db_manager.close()

    def process_raw_transaction(self, tx_data: Dict[str, Any]) -> None:
        """Process a single raw transaction and save to DB."""
        # The producer sends all fields we need (transaction_id, account_id, amount, timestamp, merchant, category)
        db_manager.insert_transaction(tx_data)
        
    def process_fraud_alert(self, raw_alert_data: Dict[str, Any]) -> None:
        """Process a single ksqlDB fraud alert."""
        
        # Normalize all keys to uppercase to safely support any incoming case
        alert_data = {k.upper(): v for k, v in raw_alert_data.items()}
        
        logger.info(f"Received ksqlDB alert: {alert_data.get('FRAUD_TYPE')} for TX {alert_data.get('TRANSACTION_ID')}")
        
        # 1. Provide safe gets for all fields. 
        # For window/group-by queries (velocity), ksqlDB won't emit transaction_id or timestamp.
        # We generate a fallback UUID for the alert and use the current time if missing.
        tx_id = alert_data.get("TRANSACTION_ID") or f"agg-{uuid.uuid4()}"
        account_id = alert_data.get("ACCOUNT_ID") or "UNKNOWN_ACCOUNT"
        amount = alert_data.get("AMOUNT") or alert_data.get("TOTAL_AMOUNT") or 0.0
        tx_timestamp = alert_data.get("TRANSACTION_TIMESTAMP") or alert_data.get("TIMESTAMP") or datetime.utcnow().isoformat()
        
        # Save the ksqlDB alert
        db_manager.insert_fraud_alert({
            "transaction_id": tx_id,
            "account_id": account_id,
            "amount": amount,
            "transaction_timestamp": tx_timestamp,
            "fraud_type": alert_data.get("FRAUD_TYPE"),
            "fraud_reason": alert_data.get("FRAUD_REASON"),
            "risk_score": alert_data.get("RISK_SCORE"),
            "source": "ksqldb"
        })
        
        # 2. Reconstruct original transaction for Python engine
        # (Assuming the stream contains merchant, we use it. If not, we skip the python rule for it)
        transaction_mock = {
            "transaction_id": tx_id,
            "account_id": account_id,
            "amount": amount,
            "timestamp": tx_timestamp,
            "merchant": alert_data.get("MERCHANT", "Unknown") 
        }

        # 3. Apply Python Fraud Engine extra rules (e.g. blacklists)
        python_result = self.fraud_engine.analyze(transaction_mock)
        
        if python_result["is_fraud"]:
            for reason in python_result["reasons"]:
                # Save each python reason as a separate alert row
                db_manager.insert_fraud_alert({
                    "transaction_id": tx_id,
                    "account_id": account_id,
                    "amount": amount,
                    "transaction_timestamp": tx_timestamp,
                    "fraud_type": "PYTHON_RULE",
                    "fraud_reason": reason,
                    "risk_score": 90,
                    "source": "python"
                })
                logger.warning(f"Python engine flagged TX {tx_id}: {reason}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = TransactionKafkaConsumer()
    consumer.consume_stream()
