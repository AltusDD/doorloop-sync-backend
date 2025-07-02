
import requests
import os

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.getenv("DOORLOOP_BASE_URL")

def fetch_all_records(endpoint):
    if not DOORLOOP_API_KEY or not DOORLOOP_BASE_URL:
        raise EnvironmentError("Missing DoorLoop API configuration in environment variables.")

    headers = {
        "Authorization": f"Bearer {DOORLOOP_API_KEY}",
        "Content-Type": "application/json"
    }

    all_data = []
    page = 1
    page_size = 100

    while True:
        url = f"{DOORLOOP_BASE_URL}{endpoint}?page_number={page}&page_size={page_size}&sort_by=createdAt&descending=true"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch page {page} of {endpoint}: {response.text}")
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        if len(data) < page_size:
            break
        page += 1

    return all_data
