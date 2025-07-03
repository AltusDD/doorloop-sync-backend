import os
import requests

BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")  # ✅ THIS is the correct variable name
API_KEY = os.getenv("DOORLOOP_API_KEY")

if not BASE_URL:
    raise ValueError("❌ Environment variable DOORLOOP_API_BASE_URL is not set.")
if not API_KEY:
    raise ValueError("❌ Environment variable DOORLOOP_API_KEY is not set.")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

def fetch_all(endpoint, params=None):
    # Fetch paginated DoorLoop data
    url = f"{BASE_URL}/{endpoint}"
    results = []
    page = 1
    while True:
        full_params = {"page_number": page, "page_size": 100}
        if params:
            full_params.update(params)
        response = requests.get(url, headers=HEADERS, params=full_params)
        if not response.ok:
            raise Exception(f"❌ Failed to fetch {endpoint}: {response.status_code} - {response.text}")
        page_data = response.json()
        if not page_data or not isinstance(page_data, list):
            break
        results.extend(page_data)
        if len(page_data) < 100:
            break
        page += 1
    return results
