import time
import random
import uuid
import json
from locust import HttpUser, task, between

# Note: This Locust file simulates a REST API that might be the source of our Kafka events.
# If we want to load test Kafka directly, we'd use a custom Locust client for Kafka.
# Here we simulate the ingestion endpoint logic.

class DataPlatformLoadTester(HttpUser):
    wait_time = between(0.1, 0.5) # High frequency

    @task
    def produce_user_event(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, 10000)}",
            "event_type": random.choice(["click", "view", "purchase"]),
            "timestamp": int(time.time() * 1000),
            "metadata": {
                "source": "load_test",
                "region": random.choice(["US", "EU", "APAC"])
            }
        }
        
        # In a real setup, this would be a POST to an ingestion microservice
        # For this demo, we can log it or send to a placeholder endpoint
        # self.client.post("/ingest", json=event)
        pass

    def on_start(self):
        print("Starting Load Test Simulation...")
