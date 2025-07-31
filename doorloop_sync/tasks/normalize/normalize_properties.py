
def run(supabase_client):
    print("🧠 Running normalize_properties task...")
    records = supabase_client.fetch("doorloop_raw_properties")
    print(f"Fetched {len(records)} raw records from doorloop_raw_properties")
