
import os
import requests

DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.environ.get("DOORLOOP_BASE_URL")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Content-Type": "application/json"
}

def fetch_data_from_doorloop(endpoint):
    results = []
    page = 1
    page_size = 100

    while True:
        url = f"{DOORLOOP_BASE_URL}{endpoint}?page_number={page}&page_size={page_size}"
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 405:
            raise ValueError(f"405 Method Not Allowed for URL: {url}")
        elif "application/json" not in response.headers.get("Content-Type", ""):
            raise ValueError(f"Non-JSON response from DoorLoop API for {endpoint}")

        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list):
            raise ValueError(f"Expected list, got {type(data)} for {endpoint}")

        results.extend(data)

        if len(data) < page_size:
            break

        page += 1

    return results
