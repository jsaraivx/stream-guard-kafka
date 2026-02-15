"""
Configuration management for Stream-Guard-Kafka.
Centralized settings using Pydantic for type safety and validation.
All values are loaded from .env file.
"""

from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka broker configuration."""

    bootstrap_servers: str = "localhost:29092"
    transactions_topic: str = "transactions"
    fraud_alerts_topic: str = "fraud_alerts"
    consumer_group_id: str = "fraud-detector-group"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    session_timeout_ms: int = 30000

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        case_sensitive=False
    )


class DatabaseSettings(BaseSettings):
    """PostgreSQL database configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "fraud_detection"
    user: str = "streamguard"
    password: str = "streamguard_2024"
    pool_size: int = 10
    max_overflow: int = 20

    model_config = SettingsConfigDict(
        env_prefix="DB_",
        case_sensitive=False
    )

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class ProducerSettings(BaseSettings):
    """Transaction producer configuration."""

    batch_size: int = 3000
    delay_between_batches: int = 2
    fraud_probability: float = 0.12
    transactions_per_second: int = 10
    high_risk_categories: List[str] = [
        "electronics",
        "jewelry",
        "cryptocurrency",
        "travel"
    ]

    model_config = SettingsConfigDict(
        env_prefix="PRODUCER_",
        case_sensitive=False
    )


class FraudDetectionSettings(BaseSettings):
    """Fraud detection rules configuration."""

    high_amount_threshold: float = 5000.00
    risk_start_hour: int = 22
    risk_end_hour: int = 6
    max_transactions_per_minute: int = 3
    velocity_check_enabled: bool = True

    model_config = SettingsConfigDict(
        env_prefix="FRAUD_",
        case_sensitive=False
    )


class Settings(BaseSettings):
    """Main application settings."""

    app_name: str = "Stream-Guard-Kafka"
    app_version: str = "0.1.0"
    debug: bool = False
    log_level: str = "INFO"

    kafka: KafkaSettings = KafkaSettings()
    database: DatabaseSettings = DatabaseSettings()
    producer: ProducerSettings = ProducerSettings()
    fraud_detection: FraudDetectionSettings = FraudDetectionSettings()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


# Global settings instance
settings = Settings()
