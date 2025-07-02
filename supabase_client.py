
# supabase_client.py
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_table_columns(table_name):
    try:
        response = supabase.table(table_name).select("*").limit(1).execute()
        if response.data and isinstance(response.data, list):
            return list(response.data[0].keys())
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch columns for table {table_name}: {e}")
    return []

def upsert_records(table_name, records):
    if not records:
        return

    allowed_keys = get_table_columns(table_name)
    if not allowed_keys:
        print(f"❌ Skipping upsert — unable to determine valid columns for {table_name}")
        return

    filtered_records = [
        {k: v for k, v in record.items() if k in allowed_keys}
        for record in records
    ]

    try:
        response = supabase.table(table_name).upsert(filtered_records).execute()
        if response.status_code >= 400:
            print(f"❌ Upsert failed: {response.status_code} → {response.data}")
        else:
            print(f"✅ Upserted {len(filtered_records)} records to {table_name}")
    except Exception as e:
        print(f"❌ Failed to upsert data to {table_name}: {e}")
