import os
import requests
import json
import logging

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise EnvironmentError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

logging.basicConfig(level=logging.INFO)

def get_doorloop_raw_tables():
    url = f"{SUPABASE_URL}/rest/v1/information_schema.tables?select=table_name&table_schema=public"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    tables = response.json()
    return [t["table_name"] for t in tables if t["table_name"].startswith("doorloop_raw_")]

def get_columns_for_table(table_name):
    url = f"{SUPABASE_URL}/rest/v1/information_schema.columns?select=column_name,data_type&table_name=eq.{table_name}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

def build_create_view_sql(table_name, columns):
    entity = table_name.replace("doorloop_raw_", "")
    select_clause = ""
    for col in columns:
        col_name = col["column_name"]
        select_clause += f'"{col_name}", '
    select_clause = select_clause.rstrip(", ")
    return f'CREATE OR REPLACE VIEW public.{entity} AS SELECT {select_clause} FROM public.{table_name};'

def execute_sql(sql):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql}
    response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if not response.ok:
        logging.error(f"❌ Failed to execute SQL: {sql}")
        logging.error(f"🔻 Error: {response.status_code} — {response.text}")
    else:
        logging.info("✅ View successfully created.")

def main():
    logging.info("🔍 Fetching raw tables...")
    tables = get_doorloop_raw_tables()

    for table in tables:
        try:
            logging.info(f"🔄 Processing table: {table}")
            columns = get_columns_for_table(table)
            if not columns:
                logging.warning(f"⚠️ No columns found for {table}, skipping.")
                continue
            sql = build_create_view_sql(table, columns)
            execute_sql(sql)
        except Exception as e:
            logging.error(f"❌ Failed to process {table}: {e}")

if __name__ == "__main__":
    main()
