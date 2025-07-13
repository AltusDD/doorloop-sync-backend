
import os
import json
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def fetch_sample_payload(table):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select=payload_json&limit=1"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    if not data or 'payload_json' not in data[0]:
        raise ValueError(f"No payload found in {table}")
    return data[0]['payload_json']

def generate_view_sql(table, columns):
    view_name = table.replace("doorloop_raw_", "")
    column_exprs = [f"payload_json->>'{col}' AS "{col}"" for col in columns]
    column_sql = ",
    ".join(column_exprs)
    return f"""CREATE OR REPLACE VIEW public.{view_name} AS
SELECT
    id,
    doorloop_id,
    {column_sql}
FROM {table};
"""


def main():
    tables = [
        "doorloop_raw_properties",
        "doorloop_raw_units",
        "doorloop_raw_tenants",
        "doorloop_raw_owners",
        "doorloop_raw_leases",
        "doorloop_raw_lease_payments",
        "doorloop_raw_lease_charges",
        "doorloop_raw_lease_credits",
        "doorloop_raw_vendors",
        "doorloop_raw_tasks",
        "doorloop_raw_files",
        "doorloop_raw_notes",
        "doorloop_raw_communications",
        "doorloop_raw_applications",
        "doorloop_raw_inspections",
        "doorloop_raw_insurance_policies",
        "doorloop_raw_recurring_charges",
        "doorloop_raw_recurring_credits",
        "doorloop_raw_accounts",
        "doorloop_raw_users",
        "doorloop_raw_portfolios",
        "doorloop_raw_reports",
        "doorloop_raw_activity_logs"
    ]

    for table in tables:
        try:
            logger.info(f"🔧 Processing view for: {table}")
            payload = fetch_sample_payload(table)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object, got {type(payload)}")

            columns = list(payload.keys())
            view_sql = generate_view_sql(table, columns)

            logger.info(f"📤 Executing SQL: {view_sql}")
        except Exception as e:
            logger.error(f"❌ Failed for {table}: {e}")

if __name__ == "__main__":
    main()
