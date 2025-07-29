
import os
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Define required schema per table (normalized tables only)
REQUIRED_SCHEMAS = {
    "doorloop_normalized_properties": {
        "doorloop_id": "text",
        "addressCity": "text",
        "addressState": "text",
        "addressStreet1": "text",
        "propertyType": "text",
        "class": "text",
        "active": "boolean"
    },
    "doorloop_normalized_units": {
        "doorloop_id": "text",
        "sqft": "numeric",
        "bedrooms": "int",
        "bathrooms": "int"
    },
    "doorloop_normalized_leases": {
        "doorloop_id": "text",
        "tenant_id": "uuid",
        "status": "text",
        "start_date": "date",
        "end_date": "date"
    },
    "doorloop_normalized_tenants": {
        "doorloop_id": "text",
        "full_name": "text",
        "lease_id": "uuid"
    },
    "doorloop_normalized_owners": {
        "doorloop_id": "text",
        "full_name": "text",
        "property_id": "uuid"
    }
}

def ensure_columns_exist():
    for table, columns in REQUIRED_SCHEMAS.items():
        for column_name, data_type in columns.items():
            try:
                alter_stmt = f'ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS "{column_name}" {data_type};'
                print(f"🛠️ Executing: {alter_stmt}")
                supabase.rpc("execute_sql", {"sql": alter_stmt}).execute()
            except Exception as e:
                print(f"❌ Error on {table}.{column_name}: {e}")

if __name__ == "__main__":
    ensure_columns_exist()
