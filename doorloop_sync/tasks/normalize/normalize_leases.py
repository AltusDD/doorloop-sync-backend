
def run(supabase_client):
    print("🧠 Running normalize_leases task...")
    records = supabase_client.fetch("doorloop_raw_leases")
    print(f"Fetched {len(records)} raw records from doorloop_raw_leases")
