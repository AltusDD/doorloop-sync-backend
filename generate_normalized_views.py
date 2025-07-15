
import os
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
RPC_ENDPOINT = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def execute_sql_query(sql):
    logger.info("📤 Executing SQL:\n%s", sql)
    response = requests.post(RPC_ENDPOINT, headers=HEADERS, json={"sql_text": sql})
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error("❌ Error calling execute_sql RPC: %s", e)
        raise
    return response.json()

def get_raw_table_names():
    try:
        sql = '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
        ORDER BY table_name;
        '''
        return [row["table_name"] for row in execute_sql_query(sql)]
    except Exception as e:
        logger.warning("⚠️ Falling back to hardcoded table list due to error: %s", str(e))
        return [
            "doorloop_raw_properties",
            "doorloop_raw_units",
            "doorloop_raw_leases",
            "doorloop_raw_tenants",
            "doorloop_raw_lease_payments",
            "doorloop_raw_lease_charges",
            "doorloop_raw_owners",
            "doorloop_raw_vendors"
        ]

def get_columns_for_table(table_name):
    try:
        sql = f'''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        '''
        return [row["column_name"] for row in execute_sql_query(sql)]
    except Exception as e:
        logger.error("❌ Failed to fetch columns for %s: %s", table_name, str(e))
        return []

def create_or_replace_view(table_name, columns):
    view_name = table_name.replace("doorloop_raw_", "doorloop_normalized_")
    select_clause = ", ".join([f'"{col}"' for col in columns])
    sql = f'CREATE OR REPLACE VIEW "{view_name}" AS SELECT {select_clause} FROM "{table_name}";'
    try:
        execute_sql_query(sql)
        logger.info("✅ View %s created or replaced successfully.", view_name)
    except Exception as e:
        logger.error("❌ Failed to create view %s: %s", view_name, str(e))

def main():
    logger.info("🔍 Starting view generation from raw tables...")
    try:
        raw_tables = get_raw_table_names()
        for table in raw_tables:
            logger.info("🔄 Processing %s...", table)
            columns = get_columns_for_table(table)
            if columns:
                create_or_replace_view(table, columns)
    except Exception as e:
        logger.critical("❌ Fatal error: %s", str(e))

if __name__ == "__main__":
    main()
