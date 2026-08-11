import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = ["40.123.240.176:9094"]
KAFKA_TOPIC = "financial-transactions-cdc"

print("Checking Kafka topic contents...")
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,  # Using None bypasses consumer groups entirely to read raw partition data
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Manually assign partitions for this topic to force a read
from kafka import TopicPartition

partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
if partitions:
    print(f"Found partitions for topic {KAFKA_TOPIC}: {partitions}")
    assignment = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
    consumer.assign(assignment)

    print("Attempting to read messages directly...")
    messages = consumer.poll(timeout_ms=5000)
    if not messages:
        print(
            "RESULT: No messages found in the topic. The producer is not successfully writing to this topic name."
        )
    else:
        for tp, msgs in messages.items():
            for msg in msgs:
                print(f"FOUND MESSAGE: {msg.value}")
else:
    print(
        f"RESULT: Topic '{KAFKA_TOPIC}' does not exist or has no partitions!"
    )

consumer.close()