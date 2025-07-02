
import os
import requests
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load .env if running locally (optional)
load_dotenv()

# Read secrets from environment (Azure / GitHub or local .env)
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL") or os.getenv("DOORLOOP_BASE_URL")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("SUPABASE_DB_CONNECTION_STRING")

# Extract Postgres connection components
parsed_url = urlparse(SUPABASE_DB_URL)
DB_NAME = parsed_url.path[1:]
DB_USER = parsed_url.username
DB_PASS = parsed_url.password
DB_HOST = parsed_url.hostname
DB_PORT = parsed_url.port

HEADERS = {"Authorization": DOORLOOP_API_KEY}
DOORLOOP_ENDPOINTS = [
    "accounts", "users", "properties", "units", "leases", "tenants", "lease-payments",
    "lease-returned-payments", "lease-charges", "lease-credits", "portfolios", "tasks",
    "owners", "vendors", "expenses", "vendor-bills", "vendor-credits", "reports",
    "communications", "notes", "files"
]

def ensure_table(cursor, table):
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{table}" (
            id TEXT PRIMARY KEY,
            data JSONB,
            last_updated TIMESTAMP DEFAULT now()
        )
    ''')

def upsert_record(cursor, table, record):
    record_id = record.get("id")
    if not record_id:
        return
    cursor.execute(f'''
        INSERT INTO "{table}" (id, data)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, last_updated = now()
    ''', (record_id, record))

def sync_endpoint(endpoint):
    print(f"🔄 Syncing `{endpoint}` ...")
    page = 1
    page_size = 100
    all_records = []

    while True:
        url = f"{DOORLOOP_API_BASE_URL}/{endpoint}?page_number={page}&page_size={page_size}&sort_by=createdAt&descending=true"
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"❌ Error fetching `{endpoint}`: {response.status_code}")
            break

        records = response.json().get("data", [])
        if not records:
            break

        all_records.extend(records)
        if len(records) < page_size:
            break

        page += 1

    print(f"📦 Fetched {len(all_records)} records from `{endpoint}`.")
    return all_records

def main():
    print("🚀 Starting full DoorLoop → Supabase sync...")

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    for endpoint in DOORLOOP_ENDPOINTS:
        table_name = endpoint.replace("-", "_")
        try:
            ensure_table(cur, table_name)
            records = sync_endpoint(endpoint)
            for record in records:
                upsert_record(cur, table_name, record)
            conn.commit()
        except Exception as e:
            print(f"🔥 Error syncing `{endpoint}` → {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("✅ Sync complete.")

if __name__ == "__main__":
    main()
