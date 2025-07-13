import os
import json
import logging
import requests

# Logging setup
logging.basicConfig(level=logging.INFO)

# Supabase environment config (injected via GitHub or Azure secrets)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

# Tables to normalize
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
    """
    Queries Supabase for all column names in a given table.
    Uses custom RPC (PostgREST function) to get information_schema columns.
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/columns_in_table"
    payload = {
        "schema": "public",
        "table_name": table_name
    }
    logging.info(f"🔍 Fetching columns for {table_name}")
    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch columns: {response.text}")
    columns_data = response.json()
    return [col["column_name"] for col in columns_data]

def build_view_sql(raw_table_name, columns):
    """
    Constructs the SQL for the normalized view by selecting all columns from the raw table.
    """
    view_name = raw_table_name.replace("doorloop_raw_", "")
    quoted_columns = [f'"{col}"' for col in columns]

    sql = [
        f"DROP VIEW IF EXISTS public.{view_name} CASCADE;",
        f"CREATE OR REPLACE VIEW public.{view_name} AS",
        f"SELECT {', '.join(quoted_columns)}",
        f"FROM public.{raw_table_name};"
    ]
    return "\n".join(sql)

def execute_sql_via_rpc(sql_command):
    """
    Sends the SQL string to Supabase to execute using the execute_sql RPC function.
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql_command}
    logging.info(f"📤 Executing SQL: {sql_command.splitlines()[1]}")
    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code != 200:
        logging.error(f"❌ SQL execution failed: {response.text}")
        raise Exception(f"SQL error: {response.text}")
    return response.text

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise EnvironmentError("❌ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set in environment.")

    for table in RAW_TABLES_TO_VIEW:
        try:
            logging.info(f"🔧 Processing view for: {table}")
            columns = get_table_columns(table)

            if not columns:
                logging.warning(f"⚠️ Skipping view for {table}, no columns found.")
                continue

            sql = build_view_sql(table, columns)
            execute_sql_via_rpc(sql)
            logging.info(f"✅ View created: public.{table.replace('doorloop_raw_', '')}")

        except Exception as e:
            logging.error(f"❌ Failed for {table}: {e}")

if __name__ == "__main__":
    main()
