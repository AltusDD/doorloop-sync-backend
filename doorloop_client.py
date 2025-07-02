import os
import requests
from urllib.parse import urljoin

DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")

if not DOORLOOP_API_BASE_URL or not DOORLOOP_API_KEY:
    raise EnvironmentError("Missing DoorLoop API configuration in environment variables.")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json",
}

def fetch_all_records(endpoint):
    print(f"🌐 Fetching from endpoint: {endpoint}")
    all_data = []
    page_number = 1
    page_size = 100

    while True:
        url = f"{DOORLOOP_API_BASE_URL}{endpoint}?page_number={page_number}&page_size={page_size}&sort_by=createdAt&descending=true"
        print(f"➡️ Requesting: {url}")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        json_data = response.json()
        data = json_data.get("data", [])
        if not data:
            break

        all_data.extend(data)

        total = json_data.get("total", 0)
        if len(all_data) >= total:
            break

        page_number += 1

    print(f"📦 Total records fetched from {endpoint}: {len(all_data)}")
    return all_data
