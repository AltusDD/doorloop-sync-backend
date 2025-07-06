
import psycopg2
import json
import logging
from dateutil.parser import isoparse
import os

logging.basicConfig(level=logging.INFO)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
SUPABASE_DB_USER = os.getenv("SUPABASE_DB_USER")
SUPABASE_DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
SUPABASE_DB_NAME = os.getenv("SUPABASE_DB_NAME")
SUPABASE_DB_PORT = os.getenv("SUPABASE_DB_PORT", 5432)

conn = psycopg2.connect(
    host=SUPABASE_DB_URL,
    dbname=SUPABASE_DB_NAME,
    user=SUPABASE_DB_USER,
    password=SUPABASE_DB_PASSWORD,
    port=SUPABASE_DB_PORT
)
conn.autocommit = True
cursor = conn.cursor()

def infer_sql_type(value):
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "bigint"
    elif isinstance(value, float):
        return "numeric"
    elif isinstance(value, str):
        try:
            isoparse(value)
            return "timestamp with time zone"
        except:
            return "text"
    elif isinstance(value, (dict, list)):
        return "jsonb"
    elif value is None:
        return "text"
    else:
        return "text"

def normalize_table(raw_table, normalized_table):
    logging.info(f"📦 Normalizing {raw_table} → {normalized_table}")
    cursor.execute(f"SELECT id, data FROM {raw_table}")
    rows = cursor.fetchall()

    all_fields = {}
    for row in rows:
        data = row[1]
        if isinstance(data, dict):
            for key, value in data.items():
                if key not in all_fields:
                    all_fields[key] = infer_sql_type(value)

    for field, sql_type in all_fields.items():
        try:
            cursor.execute(f'''
                ALTER TABLE {normalized_table}
                ADD COLUMN IF NOT EXISTS "{field}" {sql_type}
            ''')
            logging.info(f"✅ Added column {field} → {sql_type}")
        except Exception as e:
            logging.warning(f"⚠️ Column {field} skipped: {e}")

    for row in rows:
        id_val = row[0]
        data = row[1]
        columns = ['id']
        values = [f"'{id_val}'"]
        for k, v in data.items():
            if v is None:
                continue
            val = json.dumps(v) if isinstance(v, (dict, list)) else f"'{str(v).replace("'", "''")}'"
            columns.append(f'"{k}"')
            values.append(val)
        insert_sql = f'''
            INSERT INTO {normalized_table} ({", ".join(columns)})
            VALUES ({", ".join(values)})
            ON CONFLICT (id) DO NOTHING;
        '''
        try:
            cursor.execute(insert_sql)
        except Exception as e:
            logging.warning(f"⚠️ Insert failed for id={id_val}: {e}")

# List your normalization targets here
tables = [
    ("doorloop_raw_properties", "properties"),
    ("doorloop_raw_owners", "owners"),
    ("doorloop_raw_units", "units"),
    ("doorloop_raw_leases", "leases"),
    ("doorloop_raw_tenants", "tenants"),
    ("doorloop_raw_vendors", "vendors"),
    ("doorloop_raw_lease_credits", "lease_credits"),
    ("doorloop_raw_vendor_bills", "vendor_bills"),
]

for raw, normalized in tables:
    normalize_table(raw, normalized)
