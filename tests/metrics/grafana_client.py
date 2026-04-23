import requests
import os
from tests.config import GRAFANA_URL, GRAFANA_API_KEY, GRAFANA_DASHBOARD_UID

def capture_dashboard_snapshot():
    print(f"[GRAFANA] Capturing snapshot for dashboard {GRAFANA_DASHBOARD_UID}...")
    
    # Render API endpoint (requires grafana-image-renderer)
    url = f"{GRAFANA_URL}/render/d/{GRAFANA_DASHBOARD_UID}"
    
    headers = {
        "Authorization": f"Bearer {GRAFANA_API_KEY}"
    }

    try:
        # Note: You can append params for time range, width, height etc.
        # ?from=now-1h&to=now&width=1000&height=500
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            file_name = f"tests/dashboards/snapshot_{GRAFANA_DASHBOARD_UID}.png"
            with open(file_name, "wb") as f:
                f.write(response.content)
            print(f"[GRAFANA] Snapshot saved: {file_name}")
            return file_name
        else:
            print(f"[GRAFANA] Failed to capture snapshot: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"[GRAFANA] Error: {e}")
        return None

if __name__ == "__main__":
    capture_dashboard_snapshot()
