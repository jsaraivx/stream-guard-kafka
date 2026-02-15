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
    PARTITIONS = 3,
    REPLICAS = 1
);

CREATE TABLE user_velocity_stats AS
SELECT 
    account_id,
    COUNT(*) as tx_count_1min,
    SUM(amount) as total_amount_10min
FROM transactions_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY account_id;
