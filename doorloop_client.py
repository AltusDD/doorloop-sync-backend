import os
import requests

DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.environ.get("DOORLOOP_API_BASE_URL")

if not DOORLOOP_API_KEY or not DOORLOOP_API_BASE_URL:
    raise EnvironmentError("Missing DoorLoop API configuration in environment variables.")

def fetch_data_from_doorloop(endpoint):
    url = f"{DOORLOOP_API_BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {DOORLOOP_API_KEY}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()