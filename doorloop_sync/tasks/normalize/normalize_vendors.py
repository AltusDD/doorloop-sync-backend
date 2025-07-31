
def run(supabase_client):
    print("🧠 Running normalize_vendors task...")
    raw_data = supabase_client.fetch("doorloop_raw_vendors")
    print(f"Fetched {{len(raw_data)}} records from doorloop_raw_vendors")
    normalized = []

    for record in raw_data:
        normalized.append({{
            "id": record.get("id"),
            "name": record.get("name"),
            "category": record.get("category"),
            "phone": record.get("phone"),
            "email": record.get("email"),
            "status": record.get("status"),
            "created_at": record.get("createdAt"),
            "updated_at": record.get("updatedAt")
        }})

    print("✅ Normalized records (first 1):", normalized[:1])

    # Insert into normalized table
    table_name = "doorloop_normalized_vendors"
    supabase_client.upsert(table_name, normalized)
    print(f"📤 Upserted {{len(normalized)}} records into {{table_name}}")

    # Trigger KPI recalculation
    try:
        supabase_client.rpc("compute_kpis_for", {{"entity": "vendors"}})
        print("📊 Triggered KPI recalculation for: vendors")
    except Exception as e:
        print("⚠️ KPI recalculation failed:", str(e))
