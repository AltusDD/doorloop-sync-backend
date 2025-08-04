
import requests
import os

BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com")
API_KEY = os.getenv("DOORLOOP_API_KEY")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

ENDPOINTS = [
    "accounts", "activity-logs", "applications", "communications", "files", "inspections",
    "insurance-policies", "lease/charges", "lease/credits", "lease-payments", "leases",
    "notes", "owners", "payments", "portfolios", "properties", "recurring-charges",
    "recurring-credits", "reports", "tasks", "tenants", "units", "users", "vendors"
]

def test_endpoint(endpoint):
    url = f"{BASE_URL}/api/v1/{endpoint}?page=1"
    print(f"🔍 Testing {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        print(f"✅ Status Code: {response.status_code}")

        if response.headers.get("Content-Type", "").startswith("application/json"):
            print(f"📦 JSON keys: {list(response.json().keys())[:5]}")
        else:
            print("⚠️ Non-JSON response received")
            print(response.text[:300])  # Print part of the body
    except Exception as e:
        print(f"❌ Request failed: {e}")

if __name__ == "__main__":
    for ep in ENDPOINTS:
        print("="*60)
        test_endpoint(ep)
