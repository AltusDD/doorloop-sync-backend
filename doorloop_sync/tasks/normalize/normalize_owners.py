
def run(supabase_client):
    print("🧠 Running normalize_owners task...")
    records = supabase_client.fetch("doorloop_raw_owners")
    print(f"Fetched {len(records)} raw records from doorloop_raw_owners")
