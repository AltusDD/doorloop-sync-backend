import os
import requests

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/api")

def fetch_data_from_doorloop(endpoint):
    headers = {
        "Authorization": f"Bearer {DOORLOOP_API_KEY}",
        "Accept": "application/json"
    }

    all_data = []
    page_number = 1
    page_size = 100

    while True:
        url = f"{DOORLOOP_API_BASE_URL}{endpoint}?page_number={page_number}&page_size={page_size}&sort_by=createdAt&descending=true"
        print(f"🌐 Fetching from endpoint: {endpoint}")
        print(f"➡️ Requesting: {url}")

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"❌ Error fetching page {page_number} of {endpoint}: {response.text}")
            break

        json_data = response.json()
        data = json_data.get("data", [])
        total = json_data.get("total", "unknown")
        all_data.extend(data)

        if len(data) < page_size:
            break
        page_number += 1

    print(f"📦 Total records fetched from {endpoint}: {len(all_data)}")
    return all_data