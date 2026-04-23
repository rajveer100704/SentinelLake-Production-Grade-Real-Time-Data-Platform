import subprocess
import time
from tests.config import STABILIZATION_DELAY

def run_load_test(duration="60s", users="100", spawn_rate="10"):
    print(f"[LOAD] Starting Locust performance test for {duration}...")
    
    # Run locust headlessly
    cmd = [
        "locust",
        "-f", "tests/performance/locustfile.py",
        "--headless",
        "-u", users,
        "-r", spawn_rate,
        "--run-time", duration,
        "--only-summary"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print("[LOAD] Performance test completed.")
        
        print(f"[LOAD] Waiting {STABILIZATION_DELAY}s for data propagation...")
        time.sleep(STABILIZATION_DELAY)
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"[LOAD] Test failed: {e}")
        return False

if __name__ == "__main__":
    run_load_test()
