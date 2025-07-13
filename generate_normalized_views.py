import os
import requests
import json
import logging

# Load secrets from GitHub or Azure
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
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE 'doorloop_raw_%';
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql}
    response = requests.post(url, headers=HEADERS, data=json.dumps(payload))

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        logging.error("❌ Supabase returned an empty response body for get_doorloop_raw_tables.")
        logging.error(f"🔻 Status Code: {response.status_code} — Text: {response.text}")
        return []

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception as e:
            logging.error(f"❌ Failed to parse stringified JSON: {e}")
            return []

    if not data:
        logging.warning("⚠️ No raw tables found in schema.")
        return []

    return [row["table_name"] for row in data if "table_name" in row]

def get_columns_for_table(table_name):
    sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql}
    response = requests.post(url, headers=HEADERS, data=json.dumps(payload))

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        logging.error(f"❌ Supabase returned an empty response body for table {table_name}.")
        logging.error(f"🔻 Status Code: {response.status_code} — Text: {response.text}")
        return []

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception as e:
            logging.error(f"❌ Failed to parse stringified JSON for {table_name}: {e}")
            return []

    if not data:
        logging.warning(f"⚠️ No columns found for table {table_name}.")
        return []

    return data

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
        logging.error(f"❌ Failed to execute SQL:\n{sql}")
        logging.error(f"🔻 Error: {response.status_code} — {response.text}")
    else:
        logging.info("✅ View successfully created.")

def main():
    logging.info("🔍 Fetching raw tables...")
    tables = get_doorloop_raw_tables()

    if not tables:
        logging.warning("⚠️ No raw tables found. Exiting.")
        return

    for table in tables:
        try:
            logging.info(f"🔄 Processing table: {table}")
            columns = get_columns_for_table(table)
            if not columns:
                logging.warning(f"⚠️ Skipping table {table} due to missing columns.")
                continue
            sql = build_create_view_sql(table, columns)
            execute_sql(sql)
        except Exception as e:
            logging.error(f"❌ Failed to process {table}: {e}")

if __name__ == "__main__":
    main()