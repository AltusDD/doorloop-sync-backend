
import os
import json
import logging
import requests

# Logging setup
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

def get_sample_payload(table):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select=_raw_payload&limit=1"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        raise Exception(f"Failed to fetch sample from {table}: {r.text}")
    data = r.json()
    if not data or "_raw_payload" not in data[0]:
        raise Exception(f"No payload found in {table}")
    return data[0]["_raw_payload"]

def build_view_sql(raw_table, keys):
    view_name = raw_table.replace("doorloop_raw_", "")
    lines = [f"DROP VIEW IF EXISTS public.{view_name} CASCADE;"]
    lines.append(f"CREATE OR REPLACE VIEW public.{view_name} AS")
    select_lines = [f"    {raw_table}.id AS id"]
    for key in keys:
        select_lines.append(f"    , {raw_table}._raw_payload->>'{key}' AS {key}")
    lines.append("SELECT")
    lines.extend(select_lines)
    lines.append(f"FROM {raw_table};")
    return "\n".join(lines)

def execute_sql(sql):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    r = requests.post(url, headers=HEADERS, json={"sql": sql})
    if r.status_code != 200:
        raise Exception(f"SQL execution failed: {r.text}")
    return r.text

def main():
    for table in RAW_TABLES:
        try:
            logging.info(f"🔧 Processing view for: {table}")
            payload = get_sample_payload(table)
            keys = [k for k in payload.keys() if k not in ['id', 'processorFee', 'leaseDepositItem']]
            sql = build_view_sql(table, keys)
            logging.info(f"📤 Executing SQL for view: {table}")
            execute_sql(sql)
        except Exception as e:
            logging.error(f"❌ Failed for {table}: {e}")

if __name__ == "__main__":
    main()
