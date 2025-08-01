
def run(supabase_client):
    print("🧠 Running normalize_tenants task...")
    records = supabase_client.fetch("doorloop_raw_tenants")
    print(f"Fetched {len(records)} raw records from doorloop_raw_tenants")
