import json
from datetime import datetime

def generate_final_report(status, metrics, tests_summary):
    report = {
        "report_id": datetime.now().strftime("%Y%m%d%H%M%S"),
        "timestamp": datetime.now().isoformat(),
        "overall_status": "PASSED" if status else "FAILED",
        "metrics": metrics,
        "test_results": tests_summary
    }

    report_path = "test_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=4)
    
    print(f"\n📄 Final Validation Report Generated: {report_path}")
    return report_path

if __name__ == "__main__":
    generate_final_report(True, {"throughput": 100}, {"e2e": "PASSED"})
