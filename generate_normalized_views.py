
import os
import sys
import logging
import requests

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Required environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logger.error("❌ SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set as environment variables.")
    sys.exit(1)

# Constants
EXECUTE_SQL_ENDPOINT = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

def execute_sql_query(sql: str):
    payload = {"sql_text": sql}
    try:
        logger.info(f"📤 Executing SQL via requests: {sql.splitlines()[0]}...")
        response = requests.post(EXECUTE_SQL_ENDPOINT, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status()
        logger.info("✅ SQL executed successfully (requests)")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error executing SQL: {type(e).__name__} - {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response text: {e.response.text}")
        raise

def get_raw_table_names():
    sql = '''
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name LIKE 'doorloop_raw_%'
    ORDER BY table_name;
    '''
    results = execute_sql_query(sql)
    return [row['table_name'] for row in results]

def get_table_columns(table_name: str):
    sql = f'''
    SELECT column_name FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position;
    '''
    results = execute_sql_query(sql)
    return [row['column_name'] for row in results]

def create_normalized_view(table_name: str, columns: list[str]):
    escaped_columns = [f'"{col}"' for col in columns]
    view_name = f"doorloop_normalized_{table_name.replace('doorloop_raw_', '')}"

    sql = f'''
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT {', '.join(escaped_columns)}
    FROM {table_name};
    '''
    try:
        logger.info(f"📤 Executing SQL via requests: CREATE OR REPLACE VIEW {view_name} AS...")
        execute_sql_query(sql)
        logger.info(f"✅ View created: {view_name}")
    except Exception as e:
        logger.error(f"❌ Failed to process {table_name}: {e}")

def main():
    logger.info("🔍 Starting view generation from raw tables...")
    raw_tables = get_raw_table_names()
    for table in raw_tables:
        logger.info(f"🔄 Processing {table}...")
        try:
            columns = get_table_columns(table)
            logger.info(f"🧱 {table}: {len(columns)} columns found")
            create_normalized_view(table, columns)
        except Exception as e:
            logger.error(f"❌ Failed to process {table}: {e}")

if __name__ == "__main__":
    main()
