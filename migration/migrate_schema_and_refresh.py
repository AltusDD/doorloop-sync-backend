
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def execute_sql(sql: str):
    response = supabase.rpc("execute_sql", {"sql": sql}).execute()
    if hasattr(response, "get") and response.get("data", {}).get("error"):
        print("❌ ERROR:", response["data"])
    else:
        print("✅ SUCCESS")

sql_commands = [
    "ALTER TABLE public.doorloop_normalized_properties ADD COLUMN IF NOT EXISTS addressCity TEXT;",
    "ALTER TABLE public.doorloop_normalized_units ADD COLUMN IF NOT EXISTS marketRent NUMERIC;",
    "ALTER TABLE public.doorloop_normalized_units ADD COLUMN IF NOT EXISTS sqft INTEGER;",
    "ALTER TABLE public.doorloop_normalized_tenants ADD COLUMN IF NOT EXISTS full_name TEXT;",
    "ALTER TABLE public.doorloop_normalized_owners ADD COLUMN IF NOT EXISTS full_name TEXT;",
    "ALTER TABLE public.doorloop_normalized_leases ADD COLUMN IF NOT EXISTS doorloop_id TEXT;"
]

for sql in sql_commands:
    print(f"🛠️ Running: {sql}")
    execute_sql(sql)

# Optional: refresh views
refresh_sql = "REFRESH MATERIALIZED VIEW get_full_properties_view;"
print(f"🔄 Refreshing: {refresh_sql}")
execute_sql(refresh_sql)
