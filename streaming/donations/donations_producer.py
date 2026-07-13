import json
from kafka import KafkaProducer

from streaming.common.config import KAFKA_BROKER, DONATION_TOPIC

producer = None


def get_producer() -> KafkaProducer:
    global producer

    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            linger_ms=10,
            batch_size=32768,
        )

    return producer


def publish_donation(event: dict):
    kafka_producer = get_producer()
    kafka_producer.send(DONATION_TOPIC, event)

    print(f"Published donation event: {event}")


# Learning:
# KafkaProducer should not be created for every event because it is an expensive
# object that establishes network connections and manages background threads.
# A single shared producer instance is reused throughout the application's
# lifetime, reducing connection overhead, enabling efficient batching of
# messages, and improving overall performance. This follows Kafka's recommended
# best practices for production applications.



# -----------------------------------------------------------------------------
# Kafka Producer Optimizations
#
# 1. Reused a single KafkaProducer instance instead of creating a new producer
#    for every request.
#    - Eliminated repeated connection setup and teardown.
#    - Enabled reuse of Kafka's background sender thread.
#
# 2. Configured producer batching.
#    - Added `linger_ms=10` to briefly wait for more messages before sending.
#    - Increased `batch_size=32768` (32 KB) to allow larger batches.
#
# Overall Performance Improvement (T1 → T3) [500 VUs, 30s]
#
# Metric                 T1               T3              Improvement
# ---------------------------------------------------------------------------
# Total Requests         2,664            24,553          ~9.2× higher
# Throughput             72 req/s         807 req/s       ~11.2× higher
# Avg Response Time      6.21 s           513 ms          ~91.7% lower
# p95 Response Time      8.65 s           633 ms          ~92.7% lower
# Failure Rate           12.72%           1.16%           ~90.9% lower
#
# Learning:
# Reusing a single KafkaProducer together with Kafka's batching configuration
# significantly improves throughput, reduces latency, and minimizes failures
# under high concurrent load.
# -----------------------------------------------------------------------------