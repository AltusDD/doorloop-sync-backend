import os
import json
import logging
import requests

logging.basicConfig(level=logging.INFO)

# ⚙️ Fallback: Inline hardcoded values as backup (only used if env vars missing)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ssexobxvtuxwnblwplzh.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNzZXhvYnh2dHV4d25ibHdwbHpoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTUyODYxNSwiZXhwIjoyMDY1MTA0NjE1fQ.0r10o7ULOE4pCW_GrmND0rfRXMsPZGRU7aGGSDE4sKM")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.critical("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")
    raise EnvironmentError("Missing Supabase credentials.")

SUPABASE_RPC_URL = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/execute_sql"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


def execute_sql_query(sql_text):
    payload = {"sql_text": sql_text}
    logging.info(f"📤 Executing SQL:\n{sql_text.strip()}")
    try:
        response = requests.post(SUPABASE_RPC_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        if not response.text.strip():
            logging.warning("⚠️ Empty response body received from RPC.")
            return []
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
    return [row["table_name"] for row in execute_sql_query(sql)]


def get_table_columns(table_name):
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
          AND column_name NOT IN ('id', 'data', 'source_endpoint', 'inserted_at')
        ORDER BY ordinal_position;
    """
    return [row["column_name"] for row in execute_sql_query(sql)]


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
    logging.info(f"✅ Created view: {view_name}")


def main():
    logging.info("🔍 Starting view generation from raw tables...")
    try:
        raw_tables = get_raw_table_names()
        logging.info(f"📦 Found {len(raw_tables)} raw tables.")

        for table in raw_tables:
            logging.info(f"🔄 Processing: {table}")
            try:
                columns = get_table_columns(table)
                if not columns:
                    logging.warning(f"⚠️ No usable columns in {table}. Skipping...")
                    continue
                create_or_replace_view(table, columns)
            except Exception as e:
                logging.error(f"❌ Failed on {table}: {e}")
    except Exception as e:
        logging.critical(f"❌ Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
