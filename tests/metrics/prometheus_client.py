import requests
from tests.config import PROMETHEUS_URL

def query_prometheus(query):
    try:
        response = requests.get(PROMETHEUS_URL, params={"query": query}, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("result", [])
    except Exception as e:
        print(f"[METRICS] Prometheus query error: {e}")
        return []

def safe_extract(result):
    if not result:
        return 0.0
    try:
        # Prometheus result format: [[timestamp, value], ...]
        return float(result[0]["value"][1])
    except (IndexError, KeyError, ValueError, TypeError) as e:
        print(f"[METRICS] Error extracting value: {e}")
        return 0.0

def get_kafka_lag():
    result = query_prometheus("kafka_consumergroup_lag")
    return safe_extract(result)

def get_throughput():
    # Example: Rate of incoming messages to Kafka
    result = query_prometheus("rate(kafka_server_brokertopicmetrics_messagesin_total[1m])")
    return safe_extract(result)

def get_spark_latency():
    # Example: Spark streaming processing time
    result = query_prometheus("spark_streaming_processingTime")
    return safe_extract(result)

def collect_all_metrics():
    print("[METRICS] Scraping real-time metrics from Prometheus...")
    return {
        "kafka_lag": get_kafka_lag(),
        "throughput": get_throughput(),
        "latency": get_spark_latency()
    }

if __name__ == "__main__":
    print(collect_all_metrics())
