import os
from datetime import datetime
from supabase import create_client, SupabaseException
from supabase_ingest_client import sync_and_store_normalized_data

print(f"🚀 Starting normalized sync — {datetime.now().isoformat()}")

supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

print("🔍 SUPABASE_URL:", repr(supabase_url))
print("🔐 SERVICE ROLE KEY is set:", bool(supabase_key))

try:
    supabase = create_client(supabase_url, supabase_key)
    print("✅ Supabase client initialized.")
except SupabaseException as e:
    print("❌ SupabaseException:", str(e))
    raise
except Exception as e:
    print("❌ General Exception during client init:", str(e))
    raise

try:
    sync_and_store_normalized_data(supabase)
    print("🎉 Normalized data sync completed successfully.")
except Exception as e:
    print("❌ ERROR during normalized sync:", str(e))
    raise
