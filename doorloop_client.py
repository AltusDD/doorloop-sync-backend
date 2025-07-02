
import requests
import os

DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.environ.get("DOORLOOP_BASE_URL")
HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "accept": "application/json"
}

def fetch_data_from_doorloop(endpoint):
    all_data = []
    page_number = 1
    page_size = 100  # max items per page (DoorLoop default or limit)

    print(f"🌐 Fetching from endpoint: {endpoint}")

    while True:
        url = f"{DOORLOOP_BASE_URL}{endpoint}?page_number={page_number}&page_size={page_size}"
        print(f"➡️ Requesting: {url}")

        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()

            data = response.json()
            if not isinstance(data, list):
                print(f"⚠️ Unexpected response format: {data}")
                break

            print(f"✅ Page {page_number} - Fetched {len(data)} records")
            all_data.extend(data)

            if len(data) < page_size:
                break  # last page

            page_number += 1

        except Exception as e:
            print(f"❌ Error fetching page {page_number} of {endpoint}: {e}")
            break

    print(f"📦 Total records fetched from {endpoint}: {len(all_data)}")
    return all_data
