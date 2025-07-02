
import os
import requests
import logging
import json
from dynamic_schema_patcher import ensure_table_columns_exist

DOORLOOP_API_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/api/")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS_DOORLOOP = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json"
}
HEADERS_SUPABASE = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def fetch_records(endpoint):
    url = f"{DOORLOOP_API_URL}{endpoint}?page_size=100"
    all_records = []
    page = 1

    while True:
        paged_url = f"{url}&page_number={page}"
        res = requests.get(paged_url, headers=HEADERS_DOORLOOP)
        if res.status_code != 200:
            print(f"❌ Failed to fetch {endpoint}: {res.status_code}")
            break
        page_data = res.json().get("data", [])
        if not page_data:
            break
        all_records.extend(page_data)
        page += 1

    print(f"📦 {endpoint}: {len(all_records)} records")
    return all_records

def upsert_to_supabase(table_name, records):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    for record in records:
        response = requests.post(url, headers=HEADERS_SUPABASE, data=json.dumps(record))
        if response.status_code not in (200, 201):
            print(f"❌ Failed insert into {table_name}: {response.status_code} {response.text}")

def sync_all():
    endpoints_url = "https://api.doorloop.com/reference/introduction"
    known_endpoints = [
        "accounts", "users", "properties", "units", "leases", "tenants",
        "lease-payments", "lease-returned-payments", "lease-charges", "lease-credits",
        "portfolios", "tasks", "owners", "vendors", "expenses", "vendor-bills",
        "vendor-credits", "reports", "communications", "notes", "files"
    ]

    for endpoint in known_endpoints:
        print(f"🔄 Syncing: /{endpoint}")
        records = fetch_records(endpoint)
        if not records:
            continue

        table_name = f"doorloop_raw_{endpoint.replace('-', '_')}"
        for record in records:
            ensure_table_columns_exist(table_name, record)
        upsert_to_supabase(table_name, records)
        print(f"✅ Completed: /{endpoint}\n")

if __name__ == "__main__":
    sync_all()
