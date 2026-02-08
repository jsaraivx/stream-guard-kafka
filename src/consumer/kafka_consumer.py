from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraud-detection',
    value_deserializer=lambda x: x.decode('utf-8')
)

class TransactionKafkaConsumer:
    def __init__(self):
        self.consumer = consumer

    def consume(
            self,
            topic: str,
            bootstrap_servers: list[str],
            group_id: str,
            auto_offset_reset: str,
            enable_auto_commit: bool,
            value_deserializer: callable
    ):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            group_id=group_id,
            value_deserializer=value_deserializer
        )
        for message in self.consumer:
            print(f"Message Offset: {message.offset}, Value: {message.value}")
