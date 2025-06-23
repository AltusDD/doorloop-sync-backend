import os
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://app.doorloop.com/api"
API_KEY = os.getenv("DOORLOOP_API_KEY")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

def sync():
    page = 1
    total_synced = 0
    all_vendors = []

    while True:
        response = requests.get(
            f"{BASE_URL}/vendors",
            headers=HEADERS,
            params={"page": page}
        )

        if response.status_code != 200:
            print(f"[ERROR] DoorLoop /vendors failed at page {page}: {response.status_code}")
            return {"status": "error", "page": page, "code": response.status_code}

        data = response.json()
        batch = data.get("data", [])
        total = data.get("total", 0)

        if not batch:
            break

        print(f"[INFO] Page {page}: {len(batch)} vendors")
        all_vendors.extend(batch)
        total_synced += len(batch)

        if len(all_vendors) >= total:
            break

        page += 1

    print(f"[SUCCESS] Synced {total_synced} vendors from DoorLoop.")
    return {"status": "success", "synced": total_synced}
