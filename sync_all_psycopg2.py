
import os
import json
import logging
import psycopg2
import requests

# Load DB creds from environment
PG_HOST = os.getenv("SUPABASE_DB_HOST")
PG_PORT = os.getenv("SUPABASE_DB_PORT", "5432")
PG_DB = os.getenv("SUPABASE_DB_NAME", "postgres")
PG_USER = os.getenv("SUPABASE_DB_USER")
PG_PASS = os.getenv("SUPABASE_DB_PASSWORD")

DOORLOOP_API_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/api/")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")

HEADERS_DOORLOOP = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json"
}

def infer_pg_type(value):
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "bigint"
    elif isinstance(value, float):
        return "double precision"
    elif isinstance(value, dict):
        return "jsonb"
    elif isinstance(value, list):
        return "jsonb"
    else:
        return "text"

def ensure_columns(conn, table_name, record):
    with conn.cursor() as cur:
        cur.execute(f"""SELECT column_name FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s""", (table_name,))
        existing_cols = {row[0] for row in cur.fetchall()}

        for field, value in record.items():
            if field not in existing_cols:
                pg_type = infer_pg_type(value)
                try:
                    cur.execute(f'ALTER TABLE public.{table_name} ADD COLUMN IF NOT EXISTS "{field}" {pg_type};')
                    print(f"🛠️ Added column '{field}' to {table_name}")
                except Exception as e:
                    print(f"❌ Error adding column '{field}': {e}")

        conn.commit()

def fetch_doorloop_data(endpoint):
    url = f"{DOORLOOP_API_URL}{endpoint}?page_size=100"
    all_records = []
    page = 1
    while True:
        response = requests.get(f"{url}&page_number={page}", headers=HEADERS_DOORLOOP)
        if response.status_code != 200:
            print(f"❌ Error fetching {endpoint}: {response.status_code}")
            break
        batch = response.json().get("data", [])
        if not batch:
            break
        all_records.extend(batch)
        page += 1
    return all_records

def insert_records(conn, table_name, records):
    with conn.cursor() as cur:
        for r in records:
            keys = list(r.keys())
            vals = [r[k] for k in keys]
            placeholders = ', '.join(['%s'] * len(keys))
            columns = ', '.join(f'"{k}"' for k in keys)
            sql = f'INSERT INTO public.{table_name} ({columns}) VALUES ({placeholders})'
            try:
                cur.execute(sql, vals)
            except Exception as e:
                print(f"❌ Insert failed: {e}")
        conn.commit()

def sync_table(endpoint):
    table = f"doorloop_raw_{endpoint.replace('-', '_')}"
    data = fetch_doorloop_data(endpoint)
    if not data:
        print(f"⚠️ No data for {endpoint}")
        return

    print(f"📦 Syncing {len(data)} records into {table}")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    ensure_columns(conn, table, data[0])
    insert_records(conn, table, data)
    conn.close()

def sync_all():
    endpoints = [
        "accounts", "users", "properties", "units", "leases", "tenants",
        "lease-payments", "lease-returned-payments", "lease-charges", "lease-credits",
        "portfolios", "tasks", "owners", "vendors", "expenses", "vendor-bills",
        "vendor-credits", "reports", "communications", "notes", "files"
    ]
    for endpoint in endpoints:
        sync_table(endpoint)

if __name__ == "__main__":
    sync_all()
