-- ============================================================================
-- Stream-Guard-Kafka ksqlDB DDL
-- Base stream definition for transactions
-- ============================================================================

-- Create base stream for raw transactions
CREATE STREAM IF NOT EXISTS transactions_stream (
    transaction_id VARCHAR KEY,
    account_id VARCHAR,
    amount DOUBLE,
    timestamp VARCHAR,
    merchant VARCHAR,
    category VARCHAR
) WITH (
    KAFKA_TOPIC = 'transactions',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1,
    REPLICAS = 1
);

CREATE STREAM IF NOT EXISTS high_amount_alerts
WITH (
    KAFKA_TOPIC = 'fraud_alerts',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1,
    REPLICAS = 1
)
AS
SELECT
    transaction_id,
    account_id,
    amount,
    timestamp AS transaction_timestamp,
    'HIGH_AMOUNT' AS fraud_type,
    'Amount exceeds 5000' AS fraud_reason,
    100 AS risk_score
FROM transactions_stream
WHERE amount > 5000
EMIT CHANGES;

CREATE STREAM IF NOT EXISTS night_alerts
WITH (
    KAFKA_TOPIC = 'fraud_alerts',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1,
    REPLICAS = 1
)
AS
SELECT
    transaction_id,
    account_id,
    amount,
    timestamp AS transaction_timestamp,
    'NIGHT' AS fraud_type,
    'Transaction during night hours' AS fraud_reason,
    100 AS risk_score
FROM transactions_stream
WHERE SUBSTRING(timestamp, 12, 8) > '22:00:00'
   OR SUBSTRING(timestamp, 12, 8) < '06:00:00'
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS high_velocity_alerts
WITH (
    KAFKA_TOPIC = 'fraud_alerts',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1,
    REPLICAS = 1
)
AS
SELECT
    account_id,
    COUNT(*) AS tx_count,
    SUM(amount) AS total_amount,
    'HIGH_VELOCITY' AS fraud_type,
    'More than 3 transactions in 1 minute' AS fraud_reason,
    100 AS risk_score
FROM transactions_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY account_id
HAVING COUNT(*) > 3
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS high_velocity_amount_alerts
WITH (
    KAFKA_TOPIC = 'fraud_alerts',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1,
    REPLICAS = 1
)
AS
SELECT
    account_id,
    COUNT(*) AS tx_count,
    SUM(amount) AS total_amount,
    'HIGH_VELOCITY_AMOUNT' AS fraud_type,
    'Total amount exceeds 10000 in 5 minutes' AS fraud_reason,
    100 AS risk_score
FROM transactions_stream
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY account_id
HAVING SUM(amount) > 10000
EMIT CHANGES;