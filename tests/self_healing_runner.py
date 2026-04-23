import os
import time
from tests.integration.e2e_test import run_e2e_test
from tests.integration.data_quality_test import validate_data
from tests.chaos.controlled_chaos import run_chaos_test
from tests.performance.load_test import run_load_test
from tests.metrics.prometheus_client import collect_all_metrics
from tests.config import MAX_RETRIES

def heal_system(service_name="spark-master"):
    """
    Performs targeted healing by restarting only the specified service.
    """
    print(f"[HEALING] Attempting targeted restart of {service_name}...")
    os.system(f"docker restart {service_name}")
    print("[HEALING] Waiting for service to stabilize...")
    time.sleep(20)

def run_pipeline_validation():
    attempts = 0
    test_results = {}
    
    while attempts < MAX_RETRIES:
        try:
            print(f"\n[RUN] Validation Attempt {attempts + 1}")
            
            # 1. Performance / Load Test
            test_results["load_test"] = "PASSED" if run_load_test() else "FAILED"
            
            # 2. Functional / E2E
            test_results["e2e_test"] = "PASSED" if run_e2e_test() else "FAILED"
            
            # 3. Data Quality
            test_results["dq_test"] = "PASSED" if validate_data() else "FAILED"
            
            # 4. Chaos Resilience
            test_results["chaos_test"] = "PASSED" if run_chaos_test() else "FAILED"
            
            # Check if everything passed
            if all(v == "PASSED" for v in test_results.values()):
                print("\n✅ ALL SYSTEM VALIDATIONS PASSED")
                return True, test_results
            else:
                raise Exception("One or more tests failed.")

        except Exception as e:
            print(f"\n❌ VALIDATION ERROR: {e}")
            attempts += 1
            if attempts < MAX_RETRIES:
                heal_system("spark-master")
            else:
                print("\n🚨 SYSTEM FAILED AFTER MAX RETRIES")
                return False, test_results

    return False, test_results

if __name__ == "__main__":
    run_pipeline_validation()
