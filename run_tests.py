import sys
from tests.self_healing_runner import run_pipeline_validation
from tests.metrics.prometheus_client import collect_all_metrics
from tests.metrics.grafana_client import capture_dashboard_snapshot
from tests.history.db import init_db, save_test_run
from tests.dashboards.benchmark_dashboard import generate_benchmark_charts
from tests.report_generator import generate_final_report

def validate_slas(metrics):
    """
    Validates collected metrics against configured SLAs in config.py.
    """
    from tests.config import MAX_LATENCY_SEC, MAX_KAFKA_LAG
    
    sla_results = {
        "latency_sla": "PASSED" if metrics.get("latency", 0.0) < MAX_LATENCY_SEC else "FAILED",
        "kafka_lag_sla": "PASSED" if metrics.get("kafka_lag", 0.0) < MAX_KAFKA_LAG else "FAILED",
        "throughput_sla": "PASSED" if metrics.get("throughput", 0.0) > 0 else "WARNING (No Traffic)"
    }
    
    overall_sla = all(v == "PASSED" for k, v in sla_results.items() if k != "throughput_sla")
    return overall_sla, sla_results

def main():
    print("🚀 STARTING PRODUCTION-GRADE PLATFORM VALIDATION\n")
    
    # 0. Initialize
    init_db()
    
    # 1. Run Core Validation (with Self-Healing)
    status, tests_summary = run_pipeline_validation()
    
    # 2. Collect Real-Time Observability Data
    metrics = collect_all_metrics()
    
    # 3. SLA Enforcement (NEW: Senior-Level Edge)
    sla_passed, sla_summary = validate_slas(metrics)
    
    # 4. Capture Visual Proof
    capture_dashboard_snapshot()
    
    # 5. Persistence & Benchmarking
    save_test_run(metrics, "SUCCESS" if status and sla_passed else "FAILED")
    generate_benchmark_charts()
    
    # 6. Final Reporting
    report_path = generate_final_report(status and sla_passed, metrics, {
        "functional_tests": tests_summary,
        "sla_validation": sla_summary
    })
    
    print(f"\n[DONE] Validation Cycle Finished. Status: {'✅ PASSED' if status and sla_passed else '❌ FAILED'}")
    
    if not (status and sla_passed):
        sys.exit(1)

if __name__ == "__main__":
    main()
