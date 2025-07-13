import os
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

def get_table_columns(table_name):
    url = f"{SUPABASE_URL}/rest/v1/rpc/get_table_columns_rpc"
    payload = {
        "schema_name": "public",
        "table_name": table_name
    }

    logging.info(f"🔍 Fetching columns for {table_name} via RPC...")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()

        columns_data = response.json()
        if isinstance(columns_data, list) and all("column_name" in col for col in columns_data):
            return [col["column_name"] for col in columns_data]
        else:
            logging.error(f"⚠️ Unexpected response structure: {columns_data}")
            return []
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Request error: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error in get_table_columns: {e}")
        raise

def build_view_sql(raw_table_name, columns):
    view_name = raw_table_name.replace("doorloop_raw_", "")
    select_clause = ",\n    ".join([f'"{col}"' for col in columns])
    return f"""
CREATE OR REPLACE VIEW public."{view_name}" AS
SELECT
    {select_clause}
FROM public."{raw_table_name}";
"""

def execute_sql_via_rpc(sql_command):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql_command}

    logging.info("📤 Executing SQL via RPC...")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status()
        logging.info(f"✅ RPC executed: {response.status_code} -> {response.text[:100]}")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ SQL RPC error: {e}")
        raise

def run():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logging.error("❌ Missing required environment variables.")
        raise EnvironmentError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set.")

    for table in RAW_TABLES_TO_VIEW:
        try:
            logging.info(f"🔧 Processing: {table}")
            columns = get_table_columns(table)

            if not columns:
                logging.warning(f"⚠️ Skipping {table} — no columns found.")
                continue

            view_sql = build_view_sql(table, columns)
            execute_sql_via_rpc(view_sql)
            logging.info(f"✅ View created for {table}")
        except Exception as e:
            logging.error(f"❌ Failed to generate view for {table}: {e}")

if __name__ == "__main__":
    run()
