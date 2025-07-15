import os
import logging
import requests
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def quote_identifier(identifier):
    if identifier.lower() in {"to", "from", "end"} or not identifier.isidentifier():
        return f'"{identifier}"'
    return identifier

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
        logger.info("✅ SQL executed successfully (requests)")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error executing SQL: {type(e).__name__} - {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response text: {e.response.text}")
        raise

def get_raw_table_names():
    sql = """SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
ORDER BY table_name;"""
    result = execute_sql_query(sql)
    return [row["table_name"] for row in result]

def get_column_names(table_name):
    sql = f"""SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = '{table_name}'
ORDER BY ordinal_position;"""
    result = execute_sql_query(sql)
    return [row["column_name"] for row in result]

def generate_view_sql(raw_table, columns):
    view_name = raw_table.replace("doorloop_raw_", "doorloop_normalized_")
    cleaned_columns = [quote_identifier(c) for c in columns if c not in {"_links", "created_at", "updated_at"}]
    columns_sql = ", ".join(cleaned_columns)
    return f"""CREATE OR REPLACE VIEW {view_name} AS
SELECT {columns_sql} FROM {raw_table};"""

def main():
    logger.info("🔍 Starting view generation from raw tables...")
    raw_tables = get_raw_table_names()
    for raw_table in raw_tables:
        try:
            logger.info(f"🔄 Processing {raw_table}...")
            columns = get_column_names(raw_table)
            logger.info(f"🧱 {raw_table}: {len(columns)} columns found")
            sql = generate_view_sql(raw_table, columns)
            execute_sql_query(sql)
        except Exception as e:
            logger.error(f"❌ Failed to process {raw_table}: {e}")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.error("❌ Missing required environment variables: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")
        sys.exit(1)
    main()
