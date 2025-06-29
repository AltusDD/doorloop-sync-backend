import sys
sys.path.append("C:/Users/Dionr/OneDrive/Documents/GitHub/doorloop_sync_backend")

import os
import requests
from dotenv import load_dotenv
from altus_supabase.client import upsert_records

load_dotenv()

BASE_URL = "https://app.doorloop.com/api"
API_KEY = os.getenv("DOORLOOP_API_KEY")
HEADERS = { "Authorization": f"Bearer {API_KEY}" }

def sync():
    page = 1
    total_synced = 0
    all_data = []

    while True:
        response = requests.get(
            f"{BASE_URL}/users",
            headers=HEADERS,
            params={"page": page}
        )
        if response.status_code != 200:
            print(f"[ERROR] DoorLoop /users failed at page {page}: {response.status_code}")
            return {"status": "error", "page": page, "code": response.status_code}

        data = response.json()
        batch = data.get("data", [])
        total = data.get("total", 0)

        if not batch:
            break

        print(f"[INFO] Page {page}: {len(batch)} records from /users")
        all_data.extend(batch)
        total_synced += len(batch)

        if len(all_data) >= total:
            break
        page += 1

    print(f"[SUCCESS] Synced {total_synced} from /users")
    upsert = upsert_records("users", all_data)
    print(f"[UPSERT] Supabase result: {upsert}")
    return {"status": "success", "synced": total_synced}
