import os
import json
import logging
import requests
from urllib.parse import urljoin

# Logging
logging.basicConfig(level=logging.INFO)

# Supabase configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

# Only normalize properties for now
RAW_TABLES = [
    "doorloop_raw_properties",
]

# Known nested field mappings
NESTED_FIELDS = {
    "address": ["street1", "street2", "city", "state", "zip"],
}

def get_sample_json(table_name):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?select=data_json&limit=1"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    data = res.json()
    if not data or not isinstance(data[0], dict):
        raise Exception(f"No valid data_json found in {table_name}")
    return data[0]["data_json"]

def extract_fields(data_json):
    fields = []
    for key, value in data_json.items():
        if isinstance(value, dict) and key in NESTED_FIELDS:
            for subkey in NESTED_FIELDS[key]:
                fields.append((f"{key}->{subkey}", f"{key}_{subkey}"))
        elif isinstance(value, (str, int, float, bool, type(None))):
            fields.append((key, key))
    return fields

def build_view_sql(raw_table, fields):
    view_name = raw_table.replace("doorloop_raw_", "normalized_")
    sql_lines = [f"DROP VIEW IF EXISTS {view_name} CASCADE;", f"CREATE VIEW {view_name} AS", "SELECT", "    raw.id,"]
    sql_lines.append("    raw.inserted_at,")
    sql_lines.append("    raw.updated_at,")
    sql_lines.append("    raw.data_json->>'id' AS doorloop_id,")

    for path, alias in fields:
        if "->" in path:
            outer, inner = path.split("->")
            sql_lines.append(f"    raw.data_json->'{outer}'->>'{inner}' AS {alias},")
        else:
            sql_lines.append(f"    raw.data_json->>'{path}' AS {alias},")

    sql_lines.append("    raw.data_json AS _raw_payload")
    sql_lines.append(f"FROM {raw_table} raw")
    sql_lines.append("WHERE raw.data_json IS NOT NULL;")

    return "\n".join(sql_lines)

def execute_sql(sql):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    payload = {"sql": sql}
    response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if response.status_code != 200:
        raise Exception(f"Failed to execute SQL: {response.text}")
    return response.text

def main():
    for table in RAW_TABLES:
        try:
            logging.info(f"🔍 Inspecting table: {table}")
            sample_json = get_sample_json(table)
            fields = extract_fields(sample_json)
            sql = build_view_sql(table, fields)
            logging.info(f"📤 Executing view creation for {table}...")
            response = execute_sql(sql)
            logging.info(f"✅ View created for {table}: {response}")
        except Exception as e:
            logging.error(f"❌ Failed to process {table}: {e}")

if __name__ == "__main__":
    main()
