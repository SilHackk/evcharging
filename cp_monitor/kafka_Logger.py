import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'ev_charging_logs'

def get_kafka_producer():
    """Attempts to connect to Kafka and returns a producer instance."""
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            print("[KAFKA] Producer connected successfully.")
            return producer
        except NoBrokersAvailable:
            print(f"[KAFKA] Broker not available. Retrying in 5 seconds... ({retries} left)")
            retries -= 1
            time.sleep(5)
    print("[KAFKA] ERROR: Could not connect to Kafka broker.")
    return None

def log_message(producer, key, event_data):
    """Sends a log message to the Kafka topic."""
    if producer:
        try:
            producer.send(KAFKA_TOPIC, key=key, value=event_data)
            producer.flush() # Ensure message is sent
        except Exception as e:
            print(f"[KAFKA] Failed to send message: {e}")