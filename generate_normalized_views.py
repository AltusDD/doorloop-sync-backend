import os
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

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
        logger.info(f"✅ SQL executed successfully (requests)")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error executing SQL: {type(e).__name__} - {str(e)}")
        if hasattr(e, 'response'):
            logger.error(f"Response text: {e.response.text}")
        raise

def get_raw_table_names():
    sql = '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
        ORDER BY table_name;
    '''
    return [row["table_name"] for row in execute_sql_query(sql)]

def get_table_columns(table_name):
    sql = f'''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
    '''
    return [row["column_name"] for row in execute_sql_query(sql)]

def create_normalized_view(table_name, columns):
    view_name = table_name.replace("doorloop_raw_", "doorloop_normalized_")
    escaped_columns = [f'"{col}"' for col in columns]
    select_clause = ', '.join(escaped_columns)
    sql = f'''
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT {select_clause}
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
    for table_name in raw_tables:
        logger.info(f"🔄 Processing {table_name}...")
        try:
            columns = get_table_columns(table_name)
            logger.info(f"🧱 {table_name}: {len(columns)} columns found")
            create_normalized_view(table_name, columns)
        except Exception as e:
            logger.error(f"❌ Skipping table {table_name} due to error: {e}")

if __name__ == "__main__":
    main()
