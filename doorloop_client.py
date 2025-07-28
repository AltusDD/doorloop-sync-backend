
import os
import requests

DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Content-Type": "application/json"
}

def fetch_properties():
    print("🔄 Fetching properties from DoorLoop...")
    response = requests.get(f"{DOORLOOP_API_BASE_URL}/v1/properties", headers=HEADERS)
    response.raise_for_status()
    return response.json()

def fetch_units():
    print("🔄 Fetching units from DoorLoop...")
    response = requests.get(f"{DOORLOOP_API_BASE_URL}/v1/units", headers=HEADERS)
    response.raise_for_status()
    return response.json()
