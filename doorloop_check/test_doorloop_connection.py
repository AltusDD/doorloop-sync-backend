
import os
import requests

API_KEY = os.getenv("DOORLOOP_API_KEY")
BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://app.doorloop.com/api/").rstrip("/")

headers = {"Authorization": f"Bearer {API_KEY}"}

test_endpoints = [
    "properties", "leases", "units", "tenants", "owners"
]

for endpoint in test_endpoints:
    url = f"{BASE_URL}/api/{endpoint}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(f"[✅] {endpoint}: {response.headers.get('Content-Type')}")
    else:
        print(f"[❌] {endpoint} - {response.status_code} - {response.text[:150]}")
