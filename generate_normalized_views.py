import os
import json
import logging
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

logging.basicConfig(level=logging.INFO)

def get_table_columns(table_name):
    """Fetches the list of columns for a given table using Supabase RPC execute_sql."""
    sql_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    response = requests.post(url, headers=HEADERS, json={"sql": sql_query})
    response.raise_for_status()
    try:
        return [row['column_name'] for row in response.json()]
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Non-JSON response from Supabase: {response.text}") from e

def create_view_sql(raw_table):
    columns = get_table_columns(raw_table)
    if 'payload_json' not in columns:
        raise ValueError(f"payload_json column missing from table {raw_table}")

    view_name = raw_table.replace("doorloop_raw_", "")
    select_parts = [f"payload_json->>'{col}' AS "{col}"" for col in columns if col not in ['id', 'doorloop_id', 'payload_json', 'created_at']]
    select_parts.insert(0, "id")
    if 'doorloop_id' in columns:
        select_parts.insert(1, "doorloop_id")
    select_clause = ",
    ".join(select_parts)

    sql = f"""
    CREATE OR REPLACE VIEW public.{view_name} AS
    SELECT
        {select_clause}
    FROM {raw_table};
    """.strip()
    return sql

def create_view(raw_table):
    sql = create_view_sql(raw_table)
    logging.info(f"📤 Executing SQL for {raw_table}: {sql}")
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    response = requests.post(url, headers=HEADERS, json={"sql": sql})
    if not response.ok:
        raise RuntimeError(f"Failed to execute SQL: {response.text}")
    logging.info(f"✅ View created or replaced for {raw_table}")

def main():
    raw_tables = [
        "doorloop_raw_properties",
        "doorloop_raw_units",
        "doorloop_raw_tenants",
        "doorloop_raw_leases",
        "doorloop_raw_owners",
        "doorloop_raw_vendors"
    ]
    for table in raw_tables:
        try:
            logging.info(f"🔧 Processing view for: {table}")
            create_view(table)
        except Exception as e:
            logging.error(f"❌ Failed for {table}: {e}")

if __name__ == "__main__":
    main()
