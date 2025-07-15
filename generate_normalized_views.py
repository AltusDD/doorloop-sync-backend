import os
import sys
import requests
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Safety Check
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logger.critical("❌ SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")
    sys.exit(1)

EXCLUDED_COLUMNS = {"_links", "created_at", "updated_at"}

def execute_sql_query(sql: str):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    payload = {"sql_text": sql}
    try:
        logger.info(f"📤 Executing SQL via requests: {sql.splitlines()[0]}...")
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error executing SQL: {type(e).__name__} - {str(e)}")
        if hasattr(e, 'response'):
            logger.error(f"Response text: {e.response.text}")
        raise

def get_raw_table_names():
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
        ORDER BY table_name;
    """
    result = execute_sql_query(sql)
    return [row["table_name"] for row in result]

def get_columns_for_table(table_name: str):
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    result = execute_sql_query(sql)
    return [row["column_name"] for row in result if row["column_name"] not in EXCLUDED_COLUMNS]

def create_view_sql(raw_table: str, columns: list):
    normalized_table = raw_table.replace("doorloop_raw_", "doorloop_normalized_")
    col_str = ", ".join(columns)
    return f"""
        CREATE OR REPLACE VIEW {normalized_table} AS
        SELECT {col_str} FROM {raw_table};
    """

def main():
    logger.info("🔍 Starting view generation from raw tables...")
    try:
        raw_tables = get_raw_table_names()
    except Exception as e:
        logger.critical(f"❌ Failed to fetch raw tables: {e}")
        return

    for raw_table in raw_tables:
        logger.info(f"🔄 Processing {raw_table}...")
        try:
            columns = get_columns_for_table(raw_table)
            if not columns:
                logger.warning(f"⚠️ No usable columns found for {raw_table}. Skipping...")
                continue
            logger.info(f"🧱 {raw_table}: {len(columns)} columns found")
            view_sql = create_view_sql(raw_table, columns)
            execute_sql_query(view_sql)
            logger.info(f"✅ View created: doorloop_normalized_{raw_table.split('_')[-1]}")
        except Exception as e:
            logger.error(f"❌ Failed to process {raw_table}: {e}")

if __name__ == "__main__":
    main()