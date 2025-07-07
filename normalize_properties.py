
# normalize_properties.py

import os
import json
import requests

# Environment variables should be injected via GitHub Actions or Azure
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment variables.")

headers = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def fetch_view_data(view_name):
    url = f"{SUPABASE_URL}/rest/v1/{view_name}?select=*"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Failed to fetch data from {view_name}: {resp.text}")
    return resp.json()

def upsert_data(table_name, records):
    if not records:
        print("No records to upsert.")
        return
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=doorloop_id"
    resp = requests.post(url, headers=headers, data=json.dumps(records))
    if resp.status_code not in (200, 201):
        raise Exception(f"Upsert failed: {resp.text}")
    print(f"✅ Upserted {len(records)} records into {table_name}")

def main():
    print("📥 Fetching from view: get_full_properties_view")
    data = fetch_view_data("get_full_properties_view")
    print(f"📦 Records fetched: {len(data)}")
    upsert_data("properties", data)

if __name__ == "__main__":
    main()
