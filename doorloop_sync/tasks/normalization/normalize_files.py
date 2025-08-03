from doorloop_sync.clients.supabase_client import SupabaseClient

def run():
    print("🧪 Normalization placeholder — implementation required.")

    # Example: initialize Supabase client
    supabase_client = SupabaseClient()
    
    # Load data (replace with actual logic if needed later)
    raw_table_name = "doorloop_raw_files"
    normalized_table_name = "doorloop_normalized_files"
    
    raw_records = supabase_client.fetch_all(raw_table_name)
    if not raw_records:
        print("✅ No raw files data to normalize. Task complete.")
        return

    # You can implement normalization here when ready
    print(f"ℹ️ Found {len(raw_records)} raw file records (not yet normalized).")
