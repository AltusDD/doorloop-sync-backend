# generate_normalized_views.py

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

# List of all doorloop_raw_* tables for which to generate views
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
    Fetches column names for a given table from Supabase's information_schema
    by executing a direct SQL query via the /rpc/execute_sql endpoint.
    """
    sql_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """

    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql" # Supabase RPC endpoint for executing arbitrary SQL
    payload = {"sql": sql_query}

    logging.info(f"DEBUG: Fetching columns for {table_name} via RPC from {url}")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status() # Raise HTTPError for 4xx/5xx responses

        columns_data = response.json()

        # Supabase RPC for SELECT returns a list of lists if successful, e.g., [['column_name_1'], ['column_name_2']]
        # Or it might return JSON like: [{"column_name": "id"}, {"column_name": "doorloop_id"}]
        # We need to parse it robustly.

        if isinstance(columns_data, list) and columns_data and isinstance(columns_data[0], list):
            # Format: [['col1'], ['col2']]
            return [col[0] for col in columns_data]
        elif isinstance(columns_data, list) and columns_data and isinstance(columns_data[0], dict) and 'column_name' in columns_data[0]:
            # Format: [{'column_name': 'col1'}, {'column_name': 'col2'}]
            return [col['column_name'] for col in columns_data]
        else:
            logging.error(f"ERROR: Unexpected RPC response format for columns from {table_name}: {columns_data}")
            return [] # Return empty list on unexpected format

    except requests.exceptions.RequestException as e:
        logging.error(f"ERROR: Failed to fetch columns for {table_name} via RPC: {e.response.status_code if e.response else ''} -> {e.response.text if e.response else str(e)}")
        raise # Re-raise to be caught by calling function
    except Exception as e:
        logging.error(f"ERROR: Unexpected error in get_table_columns for {table_name}: {e}")
        raise

def build_view_sql(raw_table_name, columns):
    """
    Builds the CREATE OR REPLACE VIEW SQL statement dynamically.
    """
    view_name = raw_table_name.replace("doorloop_raw_", "") # e.g., properties, units

    # Quote all column names to handle potential SQL keywords or special characters
    quoted_columns = [f'"{col}"' for col in columns]

    select_clause = ", ".join(quoted_columns)

    sql = f"""
CREATE OR REPLACE VIEW public."{view_name}" AS
SELECT
    {select_clause}
FROM public."{raw_table_name}";
"""
    return sql

def execute_sql_via_rpc(sql_command):
    """
    Executes SQL commands via Supabase's /rpc/execute_sql endpoint.
    This is for DDL (CREATE VIEW)
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql_command}

    logging.info(f"DEBUG_EXEC_SQL: Executing SQL via RPC: {sql_command.splitlines()[0].strip()}...")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status()

        logging.info(f"DEBUG_EXEC_SQL: SQL RPC response: {response.status_code} -> {response.text[:200]}...")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"ERROR_EXEC_SQL: Failed to execute SQL via RPC: {e.response.status_code if e.response else ''} -> {e.response.text if e.response else str(e)}")
        raise

def run():
    # --- Environment Variable Check ---
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logging.error("❌ CRITICAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
        raise ValueError("Missing Supabase environment variables.")

    for table in RAW_TABLES_TO_VIEW:
        try:
            logging.info(f"🔧 Processing view for: {table}")

            columns = get_table_columns(table)

            if not columns:
                logging.warning(f"⚠️ No columns found for {table}. Skipping view creation.")
                continue

            sql_view_create = build_view_sql(table, columns)

            logging.info(f"📤 Executing view creation for {table}...")
            execute_sql_via_rpc(sql_view_create)
            logging.info(f"✅ View 'public.{table.replace('doorloop_raw_', '')}' created/replaced successfully for {table}.")
        except Exception as e:
            logging.error(f"❌ Failed to process {table} for view generation: {type(e).__name__}: {e}")
            # Do not raise, continue to next table to process all possible views.

if __name__ == "__main__":
    run()