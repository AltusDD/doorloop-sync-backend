from supabase_client import supabase

def normalize_properties():
    print("🧠 normalize_properties() running...")

    # Fetch raw records
    response = supabase.table("doorloop_raw_properties").select("*").execute()
    records = response.data or []
    print(f"🔍 Found {len(records)} raw properties to normalize.")

    # Transform data (example transformation)
    transformed = []
    for record in records:
        transformed.append({
            "id": record.get("id"),
            "doorloop_id": record.get("doorloop_id"),
            "name": record.get("name"),
            "type": record.get("type"),
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at")
        })

    if transformed:
        supabase.table("doorloop_normalized_properties").upsert(transformed).execute()
        print(f"✅ Inserted {len(transformed)} into doorloop_normalized_properties")
    else:
        print("⚠️ No records to insert")
