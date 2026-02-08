"""
Fake Transaction Generator for Stream-Guard-Kafka.
Generates realistic financial transaction data for testing fraud detection.
"""

import json
import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List
from uuid import uuid4

from faker import Faker


class FakeTransactionGenerator:
    """Generates fake financial transactions with realistic data."""
    
    # Transaction categories with risk levels
    CATEGORIES = {
        "low_risk": ["groceries", "pharmacy", "gas_station", "restaurant", "utilities"],
        "medium_risk": ["clothing", "entertainment", "travel", "home_improvement"],
        "high_risk": ["electronics", "jewelry", "luxury_goods", "cryptocurrency", "casino"]
    }
    
    # Flatten all categories
    ALL_CATEGORIES = [cat for cats in CATEGORIES.values() for cat in cats]
    
    def __init__(self, locale: str = "pt_BR", seed: int = None):
        """
        Initialize the fake transaction generator.
        
        Args:
            locale: Faker locale for generating localized data
            seed: Random seed for reproducibility
        """
        self.faker = Faker(locale)
        if seed:
            Faker.seed(seed)
            random.seed(seed)
    
    def generate_transaction(
        self,
        account_id: str = None,
        force_fraud: bool = False,
        timestamp: datetime = None
    ) -> Dict:
        """
        Generate a single fake transaction.
        
        Args:
            account_id: Optional specific account ID, otherwise random
            force_fraud: If True, generates a fraudulent transaction
            timestamp: Optional specific timestamp, otherwise current time
            
        Returns:
            Dictionary with transaction data
        """
        # Generate basic transaction data
        transaction = {
            "transaction_id": str(uuid4()),
            "account_id": account_id or self._generate_account_id(),
            "amount": self._generate_amount(force_fraud),
            "timestamp": (timestamp or datetime.utcnow()).isoformat(),
            "merchant": self.faker.company(),
            "category": self._generate_category(force_fraud)
        }
        
        return transaction
    
    def generate_batch(
        self,
        count: int = 100,
        fraud_probability: float = 0.15
    ) -> List[Dict]:
        """
        Generate a batch of fake transactions.
        
        Args:
            count: Number of transactions to generate
            fraud_probability: Probability of generating fraudulent transactions (0.0 to 1.0)
            
        Returns:
            List of transaction dictionaries
        """
        transactions = []
        
        for _ in range(count):
            force_fraud = random.random() < fraud_probability
            transactions.append(self.generate_transaction(force_fraud=force_fraud))
        
        return transactions
    
    def generate_time_series(
        self,
        count: int = 100,
        start_time: datetime = None,
        time_interval_seconds: int = 1,
        fraud_probability: float = 0.15
    ) -> List[Dict]:
        """
        Generate transactions with sequential timestamps.
        
        Args:
            count: Number of transactions to generate
            start_time: Starting timestamp (default: now)
            time_interval_seconds: Seconds between transactions
            fraud_probability: Probability of fraudulent transactions
            
        Returns:
            List of transaction dictionaries with sequential timestamps
        """
        transactions = []
        current_time = start_time or datetime.utcnow()
        
        for _ in range(count):
            force_fraud = random.random() < fraud_probability
            transactions.append(
                self.generate_transaction(
                    force_fraud=force_fraud,
                    timestamp=current_time
                )
            )
            current_time += timedelta(seconds=time_interval_seconds)
        
        return transactions
    
    def generate_account_activity(
        self,
        account_id: str,
        transaction_count: int = 10,
        time_window_minutes: int = 60,
        include_velocity_fraud: bool = True
    ) -> List[Dict]:
        """
        Generate multiple transactions for a single account.
        Useful for testing velocity-based fraud detection.
        
        Args:
            account_id: Account identifier
            transaction_count: Number of transactions to generate
            time_window_minutes: Time window for all transactions
            include_velocity_fraud: If True, may generate rapid transactions
            
        Returns:
            List of transactions for the account
        """
        transactions = []
        start_time = datetime.utcnow()
        
        for i in range(transaction_count):
            # For velocity fraud, cluster some transactions close together
            if include_velocity_fraud and i > 0 and random.random() < 0.3:
                # Generate transaction within 30 seconds of previous
                time_offset = timedelta(seconds=random.randint(5, 30))
            else:
                # Spread transactions across the time window
                time_offset = timedelta(
                    minutes=random.uniform(0, time_window_minutes)
                )
            
            timestamp = start_time + time_offset
            transactions.append(
                self.generate_transaction(
                    account_id=account_id,
                    timestamp=timestamp
                )
            )
        
        # Sort by timestamp
        transactions.sort(key=lambda x: x["timestamp"])
        return transactions
    
    def _generate_account_id(self) -> str:
        """Generate a realistic account ID."""
        # Format: ACC-XXXXXXXX (8 random digits)
        return f"ACC-{random.randint(10000000, 99999999)}"
    
    def _generate_amount(self, force_fraud: bool = False) -> float:
        """
        Generate transaction amount.
        
        Args:
            force_fraud: If True, generates high-value amounts
            
        Returns:
            Transaction amount as float
        """
        if force_fraud:
            # Fraudulent transactions tend to be higher value
            # 70% chance of being above fraud threshold (R$ 3000)
            if random.random() < 0.7:
                amount = random.uniform(3000.0, 15000.0)
            else:
                amount = random.uniform(500.0, 3000.0)
        else:
            # Normal transactions follow a more realistic distribution
            # Most transactions are small, with occasional larger ones
            distribution = random.random()
            if distribution < 0.6:  # 60% small transactions
                amount = random.uniform(10.0, 200.0)
            elif distribution < 0.9:  # 30% medium transactions
                amount = random.uniform(200.0, 1000.0)
            else:  # 10% larger transactions
                amount = random.uniform(1000.0, 3000.0)
        
        # Round to 2 decimal places
        return round(amount, 2)
    
    def _generate_category(self, force_fraud: bool = False) -> str:
        """
        Generate transaction category.
        
        Args:
            force_fraud: If True, biases toward high-risk categories
            
        Returns:
            Category name
        """
        if force_fraud:
            # 60% chance of high-risk category for fraudulent transactions
            if random.random() < 0.6:
                return random.choice(self.CATEGORIES["high_risk"])
            else:
                return random.choice(self.ALL_CATEGORIES)
        else:
            # Normal distribution across all categories
            # Weighted toward low and medium risk
            category_type = random.choices(
                ["low_risk", "medium_risk", "high_risk"],
                weights=[0.5, 0.35, 0.15]
            )[0]
            return random.choice(self.CATEGORIES[category_type])
    
    def to_json(self, transaction: Dict) -> str:
        """
        Convert transaction to JSON string.
        
        Args:
            transaction: Transaction dictionary
            
        Returns:
            JSON string
        """
        return json.dumps(transaction, ensure_ascii=False, indent=2)
    
    def to_json_lines(self, transactions: List[Dict]) -> str:
        """
        Convert transactions to JSON Lines format (one JSON per line).
        
        Args:
            transactions: List of transaction dictionaries
            
        Returns:
            JSON Lines string
        """
        return "\n".join(
            json.dumps(t, ensure_ascii=False) for t in transactions
        )
    
    def save_to_file(
        self,
        transactions: List[Dict],
        filename: str,
        format: str = "json"
    ) -> None:
        """
        Save transactions to file.
        
        Args:
            transactions: List of transactions
            filename: Output filename
            format: 'json' for JSON array or 'jsonl' for JSON Lines
        """
        with open(filename, "w", encoding="utf-8") as f:
            if format == "jsonl":
                f.write(self.to_json_lines(transactions))
            else:
                json.dump(transactions, f, ensure_ascii=False, indent=2)


# Convenience function for quick usage
def generate_fake_transactions(
    count: int = 100,
    fraud_probability: float = 0.15,
    locale: str = "pt_BR"
) -> List[Dict]:
    """
    Quick function to generate fake transactions.
    
    Args:
        count: Number of transactions
        fraud_probability: Probability of fraud (0.0 to 1.0)
        locale: Faker locale
        
    Returns:
        List of transaction dictionaries
    """
    generator = FakeTransactionGenerator(locale=locale)
    return generator.generate_batch(count, fraud_probability)
