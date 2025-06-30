import os
import requests

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://app.doorloop.com/api")
HEADERS = {"Authorization": f"Bearer {DOORLOOP_API_KEY}"}

ENDPOINTS = [
    "properties", "units", "tenants", "leases", "lease-payments", "lease-charges", "lease-credits",
    "vendors", "tasks", "users", "reports", "recurring-credits", "recurring-charges",
    "notes", "expenses", "communications", "attachments", "applications", "activity-logs", "accounts"
]

def fetch_all_doorloop_data():
    all_data = {}
    for endpoint in ENDPOINTS:
        print(f"📡 Fetching {endpoint}...")
        response = requests.get(f"{DOORLOOP_API_BASE_URL}/{endpoint}", headers=HEADERS)
        response.raise_for_status()
        all_data[endpoint] = response.json()
        print(f"✅ Finished fetching {endpoint} — {len(all_data[endpoint].get('data', []))} records.")
    return all_data
