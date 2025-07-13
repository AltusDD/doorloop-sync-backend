
import os
import requests
import logging

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_table_columns(table_name):
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
        headers=HEADERS,
        json={"sql": sql}
    )
    response.raise_for_status()
    return [row["column_name"] for row in response.json() if row["column_name"] != "payload_json"]

def generate_view_sql(raw_table, columns):
    select_clause = ",
    ".join([f"{raw_table}.{col}" for col in columns])
    return f"""
    CREATE OR REPLACE VIEW public.{raw_table.replace('doorloop_raw_', '')} AS
    SELECT
        {select_clause}
    FROM
        public.{raw_table} AS {raw_table};
    """

def main():
    raw_tables = [
        "doorloop_raw_properties",
        "doorloop_raw_units",
        "doorloop_raw_tenants",
        "doorloop_raw_owners",
        "doorloop_raw_leases",
        "doorloop_raw_lease_payments",
        "doorloop_raw_lease_charges",
        "doorloop_raw_lease_credits"
    ]

    for table in raw_tables:
        try:
            logger.info(f"🔧 Processing view for: {table}")
            columns = get_table_columns(table)
            logger.info(f"✅ Columns used in view: {columns}")
            sql = generate_view_sql(table, columns)
            logger.info(f"📤 Executing SQL: {sql}")
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
                headers=HEADERS,
                json={"sql": sql}
            )
            response.raise_for_status()
            logger.info(f"✅ View {table.replace('doorloop_raw_', '')} created/replaced successfully.")
        except Exception as e:
            logger.error(f"❌ Failed for {table}: {e}")

if __name__ == "__main__":
    main()
