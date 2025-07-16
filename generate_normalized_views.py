import os
import requests
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def fetch_table_list():
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE 'doorloop_raw_%'
        ORDER BY table_name;
    """
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
        headers=HEADERS,
        json={"sql_text": sql}
    )
    if response.status_code != 200:
        logger.error(f"Failed to fetch table list: {response.text}")
        return []
    return [row['table_name'] for row in response.json()]

def fetch_column_names_exact(table_name):
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND table_schema = 'public'
        ORDER BY ordinal_position;
    """
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
        headers=HEADERS,
        json={"sql_text": sql}
    )
    if response.status_code != 200:
        logger.error(f"Failed to fetch columns for {table_name}: {response.text}")
        return []
    return [f'"{row["column_name"]}"' for row in response.json()]

def create_view_safe(raw_table):
    view_name = raw_table.replace("doorloop_raw_", "doorloop_normalized_")
    columns = fetch_column_names_exact(raw_table)
    if not columns:
        logger.warning(f"⚠️ Skipping {raw_table}, no columns found.")
        return

    select_clause = ", ".join(columns)
    sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT {select_clause}
        FROM {raw_table};
    """
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
        headers=HEADERS,
        json={"sql_text": sql}
    )

    if response.status_code == 200:
        logger.info(f"✅ View created: {view_name}")
    else:
        logger.error(f"❌ Error creating view for {raw_table}: {response.text}")

def main():
    logger.info("🔍 Starting view generation from raw tables...")
    tables = fetch_table_list()
    for table in tables:
        logger.info(f"🔄 Processing {table}...")
        create_view_safe(table)

if __name__ == "__main__":
    main()
