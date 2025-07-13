import os
import requests
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

ALLOWED_COLUMNS = {
    "id", "data", "source_endpoint", "inserted_at", "batch",
    "created_at", "updated_at", "_raw_payload"
}

def get_raw_tables():
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name LIKE 'doorloop_raw_%';
    """
    payload = {"sql": sql}
    response = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/execute_sql", headers=HEADERS, json=payload)
    response.raise_for_status()
    return [row["table_name"] for row in response.json()]

def get_table_columns(table_name):
    logging.info(f"📡 Fetching columns for: {table_name}")
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    payload = {"sql": sql}
    response = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/execute_sql", headers=HEADERS, json=payload)
    response.raise_for_status()
    data = response.json()
    columns = [row["column_name"] for row in data if row["column_name"] in ALLOWED_COLUMNS]
    logging.info(f"✅ Columns for {table_name}: {columns}")
    return columns

def build_view_sql(table_name, columns):
    view_name = table_name.replace("doorloop_raw_", "view_")
    select_clause = ", ".join(f'"{col}"' for col in columns)
    sql = f"""
    CREATE OR REPLACE VIEW public."{view_name}" AS
    SELECT {select_clause}
    FROM public."{table_name}";
    """
    return sql.strip()

def execute_sql(sql):
    payload = {"sql": sql}
    response = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/execute_sql", headers=HEADERS, json=payload)
    response.raise_for_status()

def main():
    logging.info("🔍 Fetching raw tables...")
    try:
        raw_tables = get_raw_tables()
    except Exception as e:
        logging.error(f"❌ Error fetching raw tables: {e}")
        return

    for table in raw_tables:
        logging.info(f"🔄 Processing table: {table}")
        try:
            columns = get_table_columns(table)
            if not columns:
                logging.warning(f"⚠️ No valid columns found for {table}, skipping.")
                continue
            sql = build_view_sql(table, columns)
            execute_sql(sql)
            logging.info(f"✅ View created for {table}")
        except Exception as e:
            logging.error(f"❌ Failed to process {table}: {e}")

if __name__ == "__main__":
    main()