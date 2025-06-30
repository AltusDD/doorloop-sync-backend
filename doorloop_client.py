import os
import requests
from dotenv import load_dotenv

load_dotenv()

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
BASE_URL = "https://api.doorloop.com/v1"

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Content-Type": "application/json"
}

def fetch_all_entities(endpoint):
    """
    Fetches all paginated records from the specified DoorLoop API endpoint.
    """
    url = f"{BASE_URL}{endpoint}"
    all_records = []
    page = 1

    while True:
        response = requests.get(url, headers=HEADERS, params={"page": page})
        response.raise_for_status()

        data = response.json()
        records = data.get("data", [])

        if not records:
            break

        all_records.extend(records)

        if not data.get("hasMore", False):
            break

        page += 1

    return all_records
