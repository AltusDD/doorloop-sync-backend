import os
import requests
import json

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

headers = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def insert_raw_data(data, endpoint=None):
    if not data or not isinstance(data, list):
        print("❌ insert_raw_data was called with empty or invalid data.")
        return

    url = f"{SUPABASE_URL}/rest/v1/doorloop_raw_leases"  # TEMP: hardcoded for /leases testing
    payload = []

    for item in data:
        if not isinstance(item, dict):
            print(f"⚠️ Skipping non-dict item: {item}")
            continue

        record = {
            "doorloop_id": item.get("id"),
            "_raw_payload": item
        }
        if endpoint:
            record["endpoint"] = endpoint

        payload.append(record)

    print(f"📤 Inserting {len(payload)} records into doorloop_raw_leases...")

    res = requests.post(url, headers=headers, data=json.dumps(payload))
    if res.status_code >= 300:
        print(f"❌ Supabase insert error: {res.status_code} - {res.text}")
    else:
        print(f"✅ Successfully inserted {len(payload)} records.")
