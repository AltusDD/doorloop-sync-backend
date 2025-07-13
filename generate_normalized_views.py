# generate_normalized_views.py

import os
import logging
import requests

# Setup logging
logging.basicConfig(level=logging.INFO)

# Supabase env vars from GitHub or Azure
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

# Tables to generate views for
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

# Reserved columns to ignore
EXCLUDED_COLUMNS = {"processorFee", "inserted_at", "updated_at", "id"}  # Add more as needed

def get_table_columns(table_name):
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
    return [col['column_name'] for col in columns_data if col['column_name'] not in EXCLUDED_COLUMNS]

def build_view_sql(raw_table_name, columns):
    view_name = raw_table_name.replace("doorloop_raw_", "")
    quoted_cols = [f'"{col}"' for col in columns]

    sql = [
        f"DROP VIEW IF EXISTS public.{view_name} CASCADE;",
        f"CREATE OR REPLACE VIEW public.{view_name} AS",
        f"SELECT {', '.join(quoted_cols)}",
        f"FROM public.{raw_table_name};"
    ]

    return "\n".join(sql)

def execute_sql(sql):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql}
    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code != 200:
        raise Exception(f"SQL execution failed: {response.text}")
    return response.text

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise EnvironmentError("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    for raw_table in RAW_TABLES_TO_VIEW:
        try:
            logging.info(f"🔧 Processing view for: {raw_table}")
            columns = get_table_columns(raw_table)
            logging.info(f"✅ Columns used in view: {columns}")

            if not columns:
                logging.warning(f"⚠️ No usable columns found in {raw_table}. Skipping.")
                continue

            sql = build_view_sql(raw_table, columns)
            logging.info(f"📤 Executing SQL: CREATE OR REPLACE VIEW public.{raw_table.replace('doorloop_raw_', '')} AS")
            execute_sql(sql)

            logging.info(f"✅ View created for {raw_table}")
        except Exception as e:
            logging.error(f"❌ Failed for {raw_table}: {e}")

if __name__ == "__main__":
    main()
