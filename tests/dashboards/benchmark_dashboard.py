import matplotlib.pyplot as plt
from tests.history.db import get_historical_data

def generate_benchmark_charts():
    print("[DASHBOARD] Generating performance trend charts...")
    data = get_historical_data()
    
    if not data:
        print("[DASHBOARD] No historical data found to plot.")
        return

    timestamps = [r[0].split('T')[1][:8] for r in data] # Just time for X axis
    throughput = [r[1] for r in data]
    latency = [r[2] for r in data]

    # Plot Throughput
    plt.figure(figsize=(10, 5))
    plt.plot(timestamps, throughput, marker='o', color='b', label='Throughput (msg/s)')
    plt.title("System Throughput Over Time")
    plt.xlabel("Time")
    plt.ylabel("Throughput")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("tests/dashboards/throughput_trend.png")
    plt.close()

    # Plot Latency
    plt.figure(figsize=(10, 5))
    plt.plot(timestamps, latency, marker='x', color='r', label='Latency (s)')
    plt.title("Processing Latency Over Time")
    plt.xlabel("Time")
    plt.ylabel("Latency (s)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("tests/dashboards/latency_trend.png")
    plt.close()

    print("[DASHBOARD] Charts saved to tests/dashboards/")

if __name__ == "__main__":
    generate_benchmark_charts()
