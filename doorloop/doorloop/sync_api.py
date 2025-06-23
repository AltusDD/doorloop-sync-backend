import os
import requests

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
BASE_URL = "https://api.doorloop.com/v1"

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Content-Type": "application/json"
}

def fetch_all(endpoint: str) -> list[dict]:
    """Fetch all paginated results from a DoorLoop endpoint."""
    results = []
    page = 1

    while True:
        response = requests.get(
            f"{BASE_URL}/{endpoint}?page={page}",
            headers=HEADERS
        )
        response.raise_for_status()
        data = response.json()

        if not data.get("data"):
            break

        results.extend(data["data"])
        page += 1

    return results
