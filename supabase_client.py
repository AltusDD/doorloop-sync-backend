import os
import requests
import time

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {"apikey": SUPABASE_SERVICE_ROLE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}", "Content-Type": "application/json"}

def insert_raw_data(all_data):
    for endpoint, payload in all_data.items():
        table = f"doorloop_raw_{endpoint.replace('-', '_')}"
        data = payload.get("data", [])
        if not data:
            print(f"⚠️ No data for {endpoint}")
            continue
        print(f"📥 Inserting {len(data)} records into {table}...")
        for record in data:
            body = {**record, "_raw_payload": record}
            resp = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=body)
            if not resp.ok:
                print(f"❌ Failed inserting into {table}: {resp.status_code} — {resp.text}")
            time.sleep(0.1)
        print(f"✅ Completed insert for {table}")
