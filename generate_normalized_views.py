import os
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise EnvironmentError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

SUPABASE_RPC_URL = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}


def execute_sql(sql: str) -> dict:
    logger.info(f"📤 Executing SQL via requests: {sql.splitlines()[0]}...")
    response = requests.post(SUPABASE_RPC_URL, headers=HEADERS, json={"sql_text": sql})
    if response.ok:
        logger.info("✅ SQL executed successfully (requests)")
        return response.json()
    else:
        logger.error(f"❌ Error executing SQL: HTTPError - {response.status_code} {response.reason}")
        logger.error(f"Response text: {response.text}")
        raise requests.HTTPError(response.text)


def get_raw_table_names():
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
        ORDER BY table_name;
    """
    result = execute_sql(sql)
    return [row["table_name"] for row in result]


def get_column_names(table_name):
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    result = execute_sql(sql)
    return [row["column_name"] for row in result]


def escape_identifier(identifier: str) -> str:
    if identifier.lower() in {"from", "to", "end", "user", "name", "data"} or not identifier.islower():
        return f'"{identifier}"'
    return identifier


def generate_view_sql(table_name, columns):
    view_name = table_name.replace("doorloop_raw_", "doorloop_normalized_")
    escaped_columns = [escape_identifier(col) for col in columns]
    columns_sql = ", ".join(escaped_columns)
    return f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT {columns_sql}
    FROM {table_name};
    """


def main():
    logger.info("🔍 Starting view generation from raw tables...")
    table_names = get_raw_table_names()

    for table in table_names:
        logger.info(f"🔄 Processing {table}...")
        try:
            columns = get_column_names(table)
            logger.info(f"🧱 {table}: {len(columns)} columns found")
            sql = generate_view_sql(table, columns)
            execute_sql(sql)
            logger.info(f"✅ View created: {table.replace('doorloop_raw_', 'doorloop_normalized_')}")
        except Exception as e:
            logger.error(f"❌ Failed to process {table}: {e}")


if __name__ == "__main__":
    main()
