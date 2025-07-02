
import os
import requests

DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.environ.get("DOORLOOP_API_BASE_URL") or os.environ.get("DOORLOOP_BASE_URL")

if not DOORLOOP_API_KEY or not DOORLOOP_BASE_URL:
    raise EnvironmentError("❌ Missing DoorLoop API configuration in environment variables.")

def fetch_data_from_doorloop(endpoint):
    headers = {
        "Authorization": f"Bearer {DOORLOOP_API_KEY}",
        "Accept": "application/json"
    }
    url = f"{DOORLOOP_BASE_URL}{endpoint}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()
