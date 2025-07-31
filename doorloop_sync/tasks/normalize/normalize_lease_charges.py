
def run(supabase_client):
    print("🧠 Running normalize_lease_charges task...")
    raw_data = supabase_client.fetch("doorloop_raw_lease_charges")
    print(f"Fetched {{len(raw_data)}} records from doorloop_raw_lease_charges")
    normalized = []

    for record in raw_data:
        normalized.append({{
            "id": record.get("id"),
            "lease_id": record.get("leaseId"),
            "amount": float(record.get("amount", 0)),
            "date": record.get("date"),
            "method": record.get("method"),
            "created_at": record.get("createdAt"),
            "updated_at": record.get("updatedAt")
        }})

    # Print example or insert to DB here
    print("✅ Normalized records (first 1):", normalized[:1])

    # Insert into normalized table
    table_name = "doorloop_normalized_lease_charges"
    supabase_client.upsert(table_name, normalized)
    print(f"📤 Upserted {{len(normalized)}} records into {{table_name}}")

    # Trigger KPI recalculation
    try:
        supabase_client.rpc("compute_kpis_for", {{"entity": "lease_charges"}})
        print("📊 Triggered KPI recalculation for: lease_charges")
    except Exception as e:
        print("⚠️ KPI recalculation failed:", str(e))
