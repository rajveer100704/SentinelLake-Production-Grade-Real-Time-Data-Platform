import os
import time
import random
import uuid
import logging
from dotenv import load_dotenv
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Load environment variables
load_dotenv()

# Structured Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("KafkaProducer")

# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC_NAME = 'user_events'

# Load schema
value_schema = avro.load('kafka/schemas/event.avsc')

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "event_type": random.choice(["click", "view", "purchase"]),
        "timestamp": int(time.time() * 1000),
        "metadata": {
            "source": "web_app",
            "browser": random.choice(["chrome", "firefox", "safari"]),
            "env": os.getenv("ENV", "dev")
        }
    }

def run_producer():
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'schema.registry.url': SCHEMA_REGISTRY_URL,
        'on_delivery': delivery_report
    }

    try:
        producer = AvroProducer(producer_config, default_value_schema=value_schema)
        logger.info(f"Starting producer on topic: {TOPIC_NAME} at {KAFKA_BOOTSTRAP_SERVERS}")
        
        while True:
            event = generate_event()
            producer.produce(topic=TOPIC_NAME, value=event)
            producer.poll(0)
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception as e:
        logger.critical(f"Producer failed: {e}")
    finally:
        if 'producer' in locals():
            producer.flush()

if __name__ == "__main__":
    run_producer()
