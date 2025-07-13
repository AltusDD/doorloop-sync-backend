
import os
import logging
import requests

# Setup logging
logging.basicConfig(level=logging.INFO)

# Supabase environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise EnvironmentError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

# Raw tables to process
RAW_TABLES_TO_VIEW = [
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
    "doorloop_raw_activity_logs",
]

def get_columns_from_information_schema(table_name):
    url = f"{SUPABASE_URL}/rest/v1?select=column_name&schema=public&table_name=eq.{table_name}&table_schema=eq.public"
    query_url = f"{SUPABASE_URL}/rest/v1/information_schema.columns?select=column_name&table_name=eq.{table_name}&table_schema=eq.public"
    response = requests.get(query_url, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch columns: {response.text}")
    return [col['column_name'] for col in response.json() if col['column_name'] not in ['id']]

def build_view_sql(raw_table_name, columns):
    view_name = raw_table_name.replace("doorloop_raw_", "")
    quoted_columns = [f'"{col}"' for col in columns]
    sql = [f"DROP VIEW IF EXISTS public.{view_name} CASCADE;"]
    sql.append(f"CREATE OR REPLACE VIEW public.{view_name} AS")
    sql.append(f"SELECT {', '.join(quoted_columns)}")
    sql.append(f"FROM public.{raw_table_name};")
    return "\n".join(sql)

def execute_sql(sql):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    response = requests.post(url, headers=HEADERS, json={"sql": sql})
    if response.status_code != 200:
        raise Exception(f"SQL execution failed: {response.text}")
    return response.text

def main():
    for raw_table in RAW_TABLES_TO_VIEW:
        try:
            logging.info(f"🔧 Processing view for: {raw_table}")
            logging.info(f"🔍 Fetching columns for {raw_table}")
            columns = get_columns_from_information_schema(raw_table)
            if not columns:
                raise Exception("No columns found for table")
            sql = build_view_sql(raw_table, columns)
            logging.info(f"📤 Executing SQL: CREATE OR REPLACE VIEW public.{raw_table.replace('doorloop_raw_', '')} AS")
            execute_sql(sql)
            logging.info(f"✅ Successfully created view for {raw_table}")
        except Exception as e:
            logging.error(f"❌ Failed for {raw_table}: {e}")

if __name__ == "__main__":
    main()
