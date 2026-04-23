import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_TOPIC = "user_events"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Delta Lake Paths
DELTA_SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "/tmp/delta/silver")
DELTA_GOLD_PATH = os.getenv("DELTA_GOLD_PATH", "/tmp/delta/gold")

# SLAs
EXPECTED_MIN_EVENTS = 100
MAX_LATENCY_SEC = 2.0
MAX_KAFKA_LAG = 1000

# Observability
PROMETHEUS_URL = "http://localhost:9090/api/v1/query"
GRAFANA_URL = "http://localhost:3000"
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY", "your_api_key_here")
GRAFANA_DASHBOARD_UID = os.getenv("GRAFANA_DASHBOARD_UID", "streaming_metrics")

# History
HISTORY_DB_PATH = "tests/history/results.db"

# Self-Healing
MAX_RETRIES = 3
STABILIZATION_DELAY = 30
