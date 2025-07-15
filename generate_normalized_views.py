import os
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

def execute_sql_query(sql: str):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"sql_text": sql.strip()}
    logger.info(f"📤 Executing SQL: {payload['sql_text']}")
    response = requests.post(url, headers=headers, json=payload)
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"❌ Error calling execute_sql RPC: {e}")
        raise
    return response.json()

def get_raw_table_names():
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%' ORDER BY table_name;"
    try:
        return [row["table_name"] for row in execute_sql_query(sql)]
    except Exception as e:
        logger.warning(f"⚠️ Falling back to hardcoded table list due to error: {e}")
        return [
            "doorloop_raw_properties",
            "doorloop_raw_units",
            "doorloop_raw_leases",
            "doorloop_raw_tenants",
            "doorloop_raw_lease_payments",
            "doorloop_raw_lease_charges",
            "doorloop_raw_owners",
            "doorloop_raw_vendors"
        ]

def get_table_columns(table_name: str):
    sql = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
    try:
        return [row["column_name"] for row in execute_sql_query(sql)]
    except Exception as e:
        logger.error(f"❌ Failed to fetch columns for {table_name}: {e}")
        return []

def create_or_replace_view(table_name: str, columns: list[str]):
    if not columns:
        return
    view_name = table_name.replace("doorloop_raw_", "doorloop_normalized_")
    col_list = ", ".join(columns)
    sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT {col_list} FROM {table_name};"
    try:
        execute_sql_query(sql)
        logger.info(f"✅ View {view_name} created/replaced successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to create view {view_name}: {e}")

def main():
    logger.info("🔍 Starting view generation from raw tables...")
    raw_tables = get_raw_table_names()
    for table in raw_tables:
        logger.info(f"🔄 Processing {table}...")
        columns = get_table_columns(table)
        create_or_replace_view(table, columns)

if __name__ == "__main__":
    main()
