import sqlite3
from datetime import datetime
from tests.config import HISTORY_DB_PATH

def init_db():
    """
    Initializes the local SQLite database for performance benchmarking.
    NOTE: For production scale, replace this with a managed PostgreSQL instance.
    """
    conn = sqlite3.connect(HISTORY_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS test_runs (
        timestamp TEXT,
        throughput REAL,
        latency REAL,
        kafka_lag REAL,
        status TEXT
    )
    """)
    conn.commit()
    conn.close()

def save_test_run(metrics, status):
    print(f"[HISTORY] Saving test run results to {HISTORY_DB_PATH}...")
    conn = sqlite3.connect(HISTORY_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO test_runs VALUES (?, ?, ?, ?, ?)
    """, (
        datetime.now().isoformat(),
        metrics.get("throughput", 0.0),
        metrics.get("latency", 0.0),
        metrics.get("kafka_lag", 0.0),
        status
    ))
    conn.commit()
    conn.close()

def get_historical_data():
    conn = sqlite3.connect(HISTORY_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test_runs ORDER BY timestamp ASC")
    rows = cursor.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    init_db()
