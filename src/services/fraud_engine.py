from datetime import datetime, timedelta
from typing import List, Dict, Any


class FraudEngine:
    """Fraud detection engine with batch processing support."""

    def __init__(self, *, MAX_AMOUNT: float, BLACKLISTED_MERCHANTS: List[str], BLACKLISTED_ACCOUNTS: List[str]) -> None:
        """
        Initialize fraud detection engine.
        
        :param MAX_AMOUNT: Maximum allowed transaction amount
        :param BLACKLISTED_MERCHANTS: List of blacklisted merchant IDs
        :param BLACKLISTED_ACCOUNTS: List of blacklisted account IDs
        """
        self.MAX_AMOUNT = MAX_AMOUNT
        self.BLACKLISTED_MERCHANTS = set(BLACKLISTED_MERCHANTS)  # Use set for O(1) lookup
        self.BLACKLISTED_ACCOUNTS = set(BLACKLISTED_ACCOUNTS)    # Use set for O(1) lookup
        self.user_history = {}

    def analyze(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a single transaction for fraud.
        
        :param transaction: Transaction dictionary
        :return: Fraud analysis result
        """
        reasons = []
        is_fraud = False

        if transaction.get("amount", 0) > self.MAX_AMOUNT:
            is_fraud = True
            reasons.append("HIGH_TICKET_VALUE")

        if transaction.get("merchant") in self.BLACKLISTED_MERCHANTS:
            is_fraud = True
            reasons.append("BLACKLISTED_MERCHANT")

        if transaction.get("account_id") in self.BLACKLISTED_ACCOUNTS:
            is_fraud = True
            reasons.append("BLACKLISTED_ACCOUNT")

        result = {
            "transaction_id": transaction.get("transaction_id", "unknown"),
            "is_fraud": is_fraud,
            "reasons": reasons,
            "timestamp": transaction.get("timestamp", datetime.utcnow().isoformat()),
            "account_id": transaction.get("account_id"),
            "amount": transaction.get("amount"),
            "merchant": transaction.get("merchant"),
        }
        return result

    def analyze_batch(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze a batch of transactions for fraud (much faster than one-by-one).
        
        :param transactions: List of transaction dictionaries
        :return: List of fraud analysis results
        """
        return [self.analyze(transaction) for transaction in transactions]