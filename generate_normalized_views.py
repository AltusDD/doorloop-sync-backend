
import os
import json
import logging
import requests

logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

RAW_TABLES_TO_VIEW = [
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

def get_doorloop_raw_tables():
    return RAW_TABLES_TO_VIEW

def get_table_columns(table_name):
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql}
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()
        return [row["column_name"] for row in data]
    except Exception as e:
        logging.error(f"❌ Error fetching columns for {table_name}: {e}")
        return []

def build_view_sql(raw_table_name, columns):
    view_name = raw_table_name.replace("doorloop_raw_", "")
    select_clause = ",
    ".join([f'"{col}"' for col in columns])
    return f"""
CREATE OR REPLACE VIEW public."{view_name}" AS
SELECT
    {select_clause}
FROM public."{raw_table_name}";
"""

def execute_sql(sql_command):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql_command}
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"❌ Error executing SQL: {e}")
        return None

def main():
    logging.info("🔍 Fetching raw tables...")
    tables = get_doorloop_raw_tables()

    if not tables:
        logging.warning("⚠️ No raw tables found. Exiting.")
        return

    for raw_table in tables:
        logging.info(f"🔧 Processing view for: {raw_table}")
        columns = get_table_columns(raw_table)
        if not columns:
            logging.warning(f"⚠️ No columns found for {raw_table}. Skipping.")
            continue
        sql = build_view_sql(raw_table, columns)
        result = execute_sql(sql)
        if result is not None:
            logging.info(f"✅ View created: {raw_table.replace('doorloop_raw_', '')}")

if __name__ == "__main__":
    main()
