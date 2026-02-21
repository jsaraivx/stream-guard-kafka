"""
Database connection and operations for Stream-Guard-Kafka.
Handles PostgreSQL connections using psycopg2 directly.
"""

import logging
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import execute_batch

from src.config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        """Initialize database connection using centralized settings."""
        self.connection_string = settings.database.connection_string
        self.conn = None
        self.connect()

    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.conn.autocommit = False
            logger.info("Database connected successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def get_cursor(self):
        """Return a new cursor."""
        if self.conn is None or self.conn.closed:
            self.connect()
        return self.conn.cursor()

    def commit(self):
        """Commit the current transaction."""
        if self.conn:
            self.conn.commit()

    def rollback(self):
        """Rollback the current transaction."""
        if self.conn:
            self.conn.rollback()

    def health_check(self):
        """Check if connection is alive."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def close(self):
        """Close the connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed.")

    def insert_transaction(self, tx: Dict[str, Any]) -> bool:
        """Insert a raw transaction into the transactions table."""
        query = """
            INSERT INTO transactions (
                transaction_id, account_id, amount, transaction_timestamp, 
                merchant, category
            ) VALUES (
                %(transaction_id)s, %(account_id)s, %(amount)s, %(timestamp)s,
                %(merchant)s, %(category)s
            ) ON CONFLICT (transaction_id) DO NOTHING;
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, tx)
            self.commit()
            return True
        except Exception as e:
            self.rollback()
            logger.error(f"Failed to insert transaction {tx.get('transaction_id')}: {e}")
            return False

    def insert_fraud_alert(self, alert: Dict[str, Any]) -> bool:
        """Insert a fraud alert into the fraud_alerts table."""
        query = """
            INSERT INTO fraud_alerts (
                transaction_id, account_id, amount, transaction_timestamp,
                fraud_type, fraud_reason, risk_score, source
            ) VALUES (
                %(transaction_id)s, %(account_id)s, %(amount)s, %(transaction_timestamp)s,
                %(fraud_type)s, %(fraud_reason)s, %(risk_score)s, %(source)s
            );
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, alert)
            self.commit()
            return True
        except Exception as e:
            self.rollback()
            logger.error(f"Failed to insert fraud alert for {alert.get('transaction_id')}: {e}")
            return False

    def bulk_insert_fraud_alerts(self, alerts: List[Dict[str, Any]]) -> int:
        """Efficiently insert multiple fraud alerts."""
        if not alerts:
            return 0

        query = """
            INSERT INTO fraud_alerts (
                transaction_id, account_id, amount, transaction_timestamp,
                fraud_type, fraud_reason, risk_score, source
            ) VALUES (
                %(transaction_id)s, %(account_id)s, %(amount)s, %(transaction_timestamp)s,
                %(fraud_type)s, %(fraud_reason)s, %(risk_score)s, %(source)s
            );
        """
        try:
            with self.get_cursor() as cursor:
                execute_batch(cursor, query, alerts)
            self.commit()
            return len(alerts)
        except Exception as e:
            self.rollback()
            logger.error(f"Failed to bulk insert fraud alerts: {e}")
            return 0


# Global database manager instance
db_manager = DatabaseManager()
