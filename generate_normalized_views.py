import os
import json
import logging
import requests

# Logging setup
logging.basicConfig(level=logging.INFO)

# Environment: Supabase config via GitHub Actions or Azure environment
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

# Table(s) to process
RAW_TABLES = [
    "doorloop_raw_properties",
]

# Known nested structures to flatten
NESTED_FIELDS = {
    "address": ["street1", "street2", "city", "state", "zip"],
}

def get_sample_json(table_name):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?select=data&limit=1"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    data = res.json()
    if not data or not isinstance(data[0], dict) or "data" not in data[0]:
        raise Exception(f"No valid 'data' found in {table_name}")
    return data[0]["data"]

def extract_fields(data_json):
    fields = []
    for key, value in data_json.items():
        if isinstance(value, dict) and key in NESTED_FIELDS:
            for subkey in NESTED_FIELDS[key]:
                fields.append((f"{key}->{subkey}", f"{key}_{subkey}"))
        elif isinstance(value, (str, int, float, bool, type(None))):
            if key == "id":  # Avoid duplicate column name with raw.id
                fields.append((key, "doorloop_id"))  # alias it
            else:
                fields.append((key, key))
    return fields

def build_view_sql(raw_table, fields):
    view_name = raw_table.replace("doorloop_raw_", "normalized_")
    sql = [f"DROP VIEW IF EXISTS public.{view_name} CASCADE;"]
    sql.append(f"CREATE VIEW public.{view_name} AS")
    sql.append("SELECT")
    sql.append("    raw.id AS sync_id,")

    for path, alias in fields:
        if "->" in path:
            outer, inner = path.split("->")
            sql.append(f"    raw.data->'{outer}'->>'{inner}' AS {alias},")
        else:
            sql.append(f"    raw.data->>'{path}' AS {alias},")

    sql.append("    raw.data AS _raw_payload")
    sql.append(f"FROM public.{raw_table} raw")
    sql.append("WHERE raw.data IS NOT NULL;")
    return "\n".join(sql)

def execute_sql(sql):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    response = requests.post(url, headers=HEADERS, data=json.dumps({"sql": sql}))
    if response.status_code != 200:
        raise Exception(f"Failed to execute SQL: {response.text}")
    return response.text

def main():
    for table in RAW_TABLES:
        try:
            logging.info(f"🔍 Inspecting table: {table}")
            data_json = get_sample_json(table)
            fields = extract_fields(data_json)
            sql = build_view_sql(table, fields)
            logging.info(f"📤 Executing view creation for {table}...")
            result = execute_sql(sql)
            logging.info(f"✅ View created for {table}: {result}")
        except Exception as e:
            logging.error(f"❌ Failed to process {table}: {e}")

if __name__ == "__main__":
    main()
