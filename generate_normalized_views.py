import os
import requests
import logging

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_table_columns(table_name):
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
        headers={
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json"
        },
        json={"sql": query}
    )
    response.raise_for_status()
    result = response.json()
    return [row['column_name'] for row in result]

def generate_view_sql(raw_table):
    view_name = raw_table.replace("doorloop_raw_", "")
    columns = get_table_columns(raw_table)

    select_parts = [f"payload_json->>'{col}' AS "{col}"" for col in columns if col not in ['id', 'doorloop_id', 'payload_json', 'created_at']]
    select_parts.insert(0, "id")
    if 'doorloop_id' in columns:
        select_parts.insert(1, "doorloop_id")
    select_clause = ",
    ".join(select_parts)

    sql = f"""
    CREATE OR REPLACE VIEW public.{view_name} AS
    SELECT
        {select_clause}
    FROM {raw_table};
    """.strip()

    return sql

def main():
    target_tables = [
        "doorloop_raw_properties",
        "doorloop_raw_units",
        "doorloop_raw_tenants",
        "doorloop_raw_owners",
        "doorloop_raw_leases"
    ]

    for table in target_tables:
        try:
            logger.info(f"🔧 Processing view for: {table}")
            sql = generate_view_sql(table)
            logger.info(f"📤 Executing SQL: {sql}")

            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
                headers={
                    "apikey": SUPABASE_SERVICE_ROLE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "Content-Type": "application/json"
                },
                json={"sql": sql}
            )
            response.raise_for_status()
            logger.info(f"✅ View created/replaced successfully: {table}")
        except Exception as e:
            logger.error(f"❌ Failed for {table}: {e}")

if __name__ == "__main__":
    main()
