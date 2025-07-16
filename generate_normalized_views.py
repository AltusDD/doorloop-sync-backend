
import os
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SCHEMA = "public"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def execute_sql(sql: str):
    logging.info("📤 Executing SQL via requests: ...")
    try:
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
            headers=HEADERS,
            json={"sql_text": sql}
        )
        response.raise_for_status()
        logging.info("✅ SQL executed successfully.")
        return response.json()
    except requests.exceptions.HTTPError as e:
        logging.error(f"❌ Error executing SQL: HTTPError - {e}")
        logging.error(f"Response text: {response.text}")
    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}")

def get_column_names(table_name):
    query = f'''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{SCHEMA}' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    '''
    result = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
        headers=HEADERS,
        json={"sql_text": query}
    )
    if result.status_code == 200:
        return [row['column_name'] for row in result.json()]
    else:
        logging.error(f"⚠️ Failed to fetch columns for {table_name}. Response: {result.text}")
        return []

def automate_view_creation(schema_name, table_mappings):
    for raw_table, view_name in table_mappings.items():
        logging.info(f"🔄 Processing {raw_table}...")
        columns = get_column_names(raw_table)
        if not columns:
            logging.warning(f"⚠️ Skipping {raw_table}, no columns found.")
            continue

        col_list = ", ".join(columns)
        sql = f'''
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT {col_list} FROM {raw_table};
        '''
        execute_sql(sql)

if __name__ == "__main__":
    logging.info("🔍 Starting view generation from raw tables...")
    schema = "public"
    mappings = {
        "doorloop_raw_properties": "doorloop_normalized_properties",
        "doorloop_raw_units": "doorloop_normalized_units",
        "doorloop_raw_leases": "doorloop_normalized_leases",
        "doorloop_raw_tenants": "doorloop_normalized_tenants",
        "doorloop_raw_owners": "doorloop_normalized_owners",
        "doorloop_raw_payments": "doorloop_normalized_payments"
    }
    automate_view_creation(schema, mappings)
