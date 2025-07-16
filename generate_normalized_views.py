import os
import json
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

SQL_ENDPOINT = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"

RAW_TABLES = [
    "doorloop_raw_properties",
    "doorloop_raw_units",
    "doorloop_raw_leases",
    "doorloop_raw_tenants",
    "doorloop_raw_payments",
    "doorloop_raw_owners"
]

def get_sql_for(table):
    view_name = table.replace("raw", "normalized")
    fallback_created = "data->>'createdAt' AS created_at"  # Safe fallback
    fallback_updated = "data->>'updatedAt' AS updated_at"
    
    select_fields = [
        "id",
        fallback_created,
        fallback_updated,
        "data->>'name' AS name",
        "data->>'status' AS status",
        "data->>'type' AS type"
    ]
    
    # Special case for units
    if table == "doorloop_raw_units":
        select_fields.append("data->>'createdAt' AS source_endpoint")

    select_clause = ",\n    ".join(select_fields)

    return f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        {select_clause}
    FROM {table};
    """

def run():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.error("❌ Missing environment variables SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")
        return

    for table in RAW_TABLES:
        sql = get_sql_for(table)
        logger.info(f"📤 Sending SQL for {table}...")

        payload = {"sql": sql}
        response = requests.post(SQL_ENDPOINT, headers=HEADERS, data=json.dumps(payload))

        if response.status_code == 200:
            logger.info(f"✅ View created for {table}")
        else:
            logger.warning(f"⚠️ Failed to create view for {table}: {response.status_code} - {response.text}")

if __name__ == "__main__":
    run()
