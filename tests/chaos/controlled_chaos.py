import os
import time
import subprocess

def run_command(cmd):
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[CHAOS] Command failed: {e.stderr.decode()}")
        return False

def stop_spark():
    print("[CHAOS] Stopping Spark master (graceful)...")
    return run_command("docker stop spark-master")

def start_spark():
    print("[CHAOS] Restarting Spark master...")
    return run_command("docker start spark-master")

def check_spark_health():
    print("[CHAOS] Checking Spark health...")
    # Simple check if container is running
    result = subprocess.run("docker inspect -f '{{.State.Running}}' spark-master", shell=True, capture_output=True)
    return result.stdout.decode().strip() == "true"

def run_chaos_test():
    if not stop_spark():
        return False
    
    time.sleep(10)
    
    if not start_spark():
        return False
    
    # Wait for recovery
    print("[CHAOS] Waiting for Spark to stabilize...")
    time.sleep(20)
    
    if check_spark_health():
        print("[CHAOS] Recovery successful")
        return True
    else:
        print("[CHAOS] Recovery FAILED")
        return False

if __name__ == "__main__":
    run_chaos_test()
