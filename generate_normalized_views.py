import os
import json
import logging
import requests

logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.critical("❌ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing from environment variables.")
    raise EnvironmentError("Missing required Supabase environment variables.")

SUPABASE_RPC_URL = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/execute_sql"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


def execute_sql_query(sql_text):
    try:
        payload = {"sql_text": sql_text}
        logging.debug(f"🔍 Executing SQL via Supabase RPC: {sql_text}")
        response = requests.post(SUPABASE_RPC_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error calling execute_sql RPC: {e}")
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


def get_table_columns(table_name):
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
          AND column_name NOT IN ('id', 'data', 'source_endpoint', 'inserted_at')
        ORDER BY ordinal_position;
    """
    result = execute_sql_query(sql)
    return [row["column_name"] for row in result]


def create_or_replace_view(table_name, columns):
    view_name = table_name.replace("doorloop_raw_", "doorloop_")
    select_clauses = [f"data->>'{col}' AS {col}" for col in columns]
    sql = f"""
        CREATE OR REPLACE VIEW public.{view_name} AS
        SELECT
            id,
            source_endpoint,
            inserted_at,
            {", ".join(select_clauses)}
        FROM {table_name}
        WHERE active IS DISTINCT FROM false;
    """
    execute_sql_query(sql)
    logging.info(f"✅ View '{view_name}' created/replaced successfully.")


def main():
    logging.info("🔍 Starting view generation from raw tables...")
    try:
        logging.info("📥 Querying for raw table names...")
        raw_tables = get_raw_table_names()
        for table in raw_tables:
            logging.info(f"🔄 Processing table: {table}")
            columns = get_table_columns(table)
            if not columns:
                logging.warning(f"⚠️ No extractable columns found for {table}. Skipping...")
                continue
            create_or_replace_view(table, columns)
    except Exception as e:
        logging.error(f"❌ Fatal error during view generation: {e}")
        raise


if __name__ == "__main__":
    main()
