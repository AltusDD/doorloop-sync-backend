
import os
import json
import logging
import requests

# Setup
logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

RAW_TABLES = [
    "doorloop_raw_properties",
    "doorloop_raw_units",
    "doorloop_raw_tenants",
    "doorloop_raw_owners",
    "doorloop_raw_leases",
    "doorloop_raw_lease_payments",
    "doorloop_raw_lease_charges",
    "doorloop_raw_lease_credits",
    "doorloop_raw_vendors",
    "doorloop_raw_tasks",
    "doorloop_raw_files",
    "doorloop_raw_notes",
    "doorloop_raw_communications",
    "doorloop_raw_applications",
    "doorloop_raw_inspections",
    "doorloop_raw_insurance_policies",
    "doorloop_raw_recurring_charges",
    "doorloop_raw_recurring_credits",
    "doorloop_raw_accounts",
    "doorloop_raw_users",
    "doorloop_raw_portfolios",
    "doorloop_raw_reports",
    "doorloop_raw_activity_logs",
]

def fetch_sample_payload(table):
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=payload_json&limit=1"
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        items = r.json()
        if items and "payload_json" in items[0]:
            return items[0]["payload_json"]
        raise ValueError(f"No payload found in {table}")
    except Exception as e:
        raise RuntimeError(f"Failed to fetch sample from {table}: {e}")

def extract_fields(payload):
    if not isinstance(payload, dict):
        raise ValueError("Expected JSON object")
    clean_keys = []
    for key in payload.keys():
        if key.isidentifier() and key.lower() not in ["processorfee", "leasedeposititem"]:
            clean_keys.append(key)
    return clean_keys

def generate_view_sql(table, fields):
    view_name = table.replace("doorloop_raw_", "")
    select_clause = ",\n    ".join([f"raw.payload_json->>'{f}' AS \"{f}\"" for f in fields])
    return f"""CREATE OR REPLACE VIEW public.{view_name} AS
SELECT
    raw.id AS id,
    {select_clause}
FROM public.{table} AS raw;
"""


def run():
    for table in RAW_TABLES:
        try:
            logging.info(f"🔧 Processing view for: {table}")
            sample = fetch_sample_payload(table)
            fields = extract_fields(sample)
            sql = generate_view_sql(table, fields)
            url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
            r = requests.post(url, headers=HEADERS, json={"sql": sql})
            if r.status_code != 200:
                raise RuntimeError(f"SQL execution failed: {r.text}")
            logging.info(f"✅ View created for {table}")
        except Exception as e:
            logging.error(f"❌ Failed for {table}: {e}")

if __name__ == "__main__":
    run()
