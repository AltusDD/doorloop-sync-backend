import os
import json
import logging
import requests

# Setup logging
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
    "doorloop_raw_expenses",
    "doorloop_raw_vendor_bills",
    "doorloop_raw_vendor_credits",
    "doorloop_raw_lease_reversed_payments",
    "doorloop_raw_property_groups",
]

def get_table_columns(table_name):
    sql = f"""
        SELECT json_agg(t) FROM (
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        ) t
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql}

    logging.info(f"📡 Fetching columns for: {table_name}")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict) and "json_agg" in data[0]:
            column_list = data[0]["json_agg"]
            if column_list is None:
                logging.warning(f"⚠️ No columns returned in json_agg for {table_name}")
                return []
        else:
            logging.warning(f"⚠️ Unexpected response format for {table_name}: {data}")
            return []

        final_columns = [row["column_name"] for row in column_list if "column_name" in row]
        logging.info(f"✅ Columns for {table_name}: {final_columns}")
        return final_columns

    except Exception as e:
        logging.error(f"❌ Error fetching columns for {table_name}: {type(e).__name__}: {e}")
        return []

def build_view_sql(table_name, columns):
    view_name = table_name.replace("doorloop_raw_", "")
    quoted_columns = [f'"{col}"' for col in columns]
    select_clause = ",\n    ".join(quoted_columns)
    sql = f"""
CREATE OR REPLACE VIEW public."{view_name}" AS
SELECT
    {select_clause}
FROM public."{table_name}";
"""
    return sql

def execute_sql_via_rpc(sql_command):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql_command}
    try:
        logging.debug(f"📤 Executing SQL: {sql_command.strip().splitlines()[0]}")
        response = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status()
        logging.info("✅ SQL executed successfully.")
    except Exception as e:
        logging.error(f"❌ SQL execution failed: {type(e).__name__}: {e}")

def run():
    logging.info("🔍 Fetching raw tables...")
    for table in RAW_TABLES:
        logging.info(f"🔄 Processing table: {table}")
        try:
            columns = get_table_columns(table)
            if not columns:
                logging.warning(f"⚠️ Skipping {table}: No columns found.")
                continue
            view_sql = build_view_sql(table, columns)
            execute_sql_via_rpc(view_sql)
        except Exception as e:
            logging.error(f"❌ Failed to process {table}: {type(e).__name__}: {e}")

if __name__ == "__main__":
    run()
