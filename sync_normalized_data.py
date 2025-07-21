import os
from supabase import create_client, SupabaseException

supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

print("🧪 SUPABASE_URL =", repr(supabase_url))
print("🔐 SERVICE ROLE KEY is set =", bool(supabase_key))

try:
    supabase = create_client(supabase_url, supabase_key)
    print("✅ Supabase client created successfully.")
except SupabaseException as e:
    print("❌ SupabaseException:", str(e))
    raise
except Exception as e:
    print("❌ General Exception:", str(e))
    raise
