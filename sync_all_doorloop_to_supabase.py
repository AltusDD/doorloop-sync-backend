import os
import requests
import psycopg2
from psycopg2.extras import Json
import time
from datetime import datetime
import logging

# Setup logging
LOG_FILE = "doorloop_sync.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def log_success(message):
    logging.info("✅ " + message)

def log_error(message):
    logging.error("❌ " + message)

def get_env_var(name):
    value = os.environ.get(name)
    if not value:
        log_error(f"Missing required environment variable: {name}")
        exit(1)
    return value

DOORLOOP_API_KEY = get_env_var("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = get_env_var("DOORLOOP_API_BASE_URL")
SUPABASE_DB_URL = get_env_var("SUPABASE_DB_URL")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}"
}

ENDPOINTS = [
    "accounts",
    "users",
    "properties",
    "units",
    "leases",
    "tenants",
    "lease-payments",
    "lease-returned-payments",
    "lease-charges",
    "lease-credits",
    "portfolios",
    "tasks",
    "owners",
    "vendors",
    "expenses",
    "vendor-bills",
    "vendor-credits",
    "reports",
    "communications",
    "notes",
    "files"
]

def fetch_all(endpoint):
    all_data = []
    page = 1
    while True:
        url = f"{DOORLOOP_API_BASE_URL}/{endpoint}?page_number={page}&page_size=100"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 404:
            log_error(f"404: {endpoint} not found")
            break
        elif resp.status_code != 200:
            log_error(f"Error fetching {endpoint}: {resp.status_code}")
            break
        data = resp.json().get("data", [])
        if not data:
            break
        all_data.extend(data)
        page += 1
    log_success(f"Fetched {len(all_data)} records from {endpoint}")
    return all_data

def sync_to_supabase(endpoint, records):
    if not records:
        log_success(f"No data to insert for {endpoint}.")
        return
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        cur = conn.cursor()
        table_name = f"dl__{endpoint.replace('-', '_')}"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                data JSONB,
                last_updated TIMESTAMP
            )
        """)
        for record in records:
            cur.execute(f"""
                INSERT INTO {table_name} (id, data, last_updated)
                VALUES (%s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET data = EXCLUDED.data, last_updated = EXCLUDED.last_updated
            """, (record.get("id"), Json(record), datetime.utcnow()))
        conn.commit()
        cur.close()
        conn.close()
        log_success(f"Inserted {len(records)} records into {table_name}.")
    except Exception as e:
        log_error(f"Error inserting into Supabase for {endpoint}: {str(e)}")

if __name__ == "__main__":
    start = time.time()
    log_success("🚀 Starting DoorLoop → Supabase sync")
    for endpoint in ENDPOINTS:
        log_success(f"🔄 Syncing: {endpoint}")
        records = fetch_all(endpoint)
        sync_to_supabase(endpoint, records)
    elapsed = time.time() - start
    log_success(f"🎉 Sync complete in {elapsed:.2f} seconds.")
