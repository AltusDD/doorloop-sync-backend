
def run(supabase_client):
    print("🧠 Running normalize_units task...")
    records = supabase_client.fetch("doorloop_raw_units")
    print(f"Fetched {len(records)} raw records from doorloop_raw_units")
