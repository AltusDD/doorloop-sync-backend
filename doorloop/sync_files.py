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
    all_files = []

    while True:
        response = requests.get(
            f"{BASE_URL}/files",
            headers=HEADERS,
            params={"page": page}
        )

        if response.status_code != 200:
            print(f"[ERROR] DoorLoop /files failed at page {page}: {response.status_code}")
            return {"status": "error", "page": page, "code": response.status_code}

        data = response.json()
        batch = data.get("data", [])
        total = data.get("total", 0)

        if not batch:
            break

        print(f"[INFO] Page {page}: {len(batch)} files")
        all_files.extend(batch)
        total_synced += len(batch)

        if len(all_files) >= total:
            break

        page += 1

    print(f"[SUCCESS] Synced {total_synced} files from DoorLoop.")
    return {"status": "success", "synced": total_synced}
