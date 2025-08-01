
def run(supabase_client):
    print("🧠 Running normalize_tasks task...")
    raw_data = supabase_client.fetch("doorloop_raw_tasks")
    print(f"Fetched {{len(raw_data)}} records from doorloop_raw_tasks")
    normalized = []

    for record in raw_data:
        normalized.append({{
            "id": record.get("id"),
            "title": record.get("title"),
            "status": record.get("status"),
            "priority": record.get("priority"),
            "property_id": record.get("propertyId"),
            "due_date": record.get("dueDate"),
            "created_at": record.get("createdAt"),
            "updated_at": record.get("updatedAt")
        }})

    print("✅ Normalized records (first 1):", normalized[:1])

    # Insert into normalized table
    table_name = "doorloop_normalized_tasks"
    supabase_client.upsert(table_name, normalized)
    print(f"📤 Upserted {{len(normalized)}} records into {{table_name}}")

    # Trigger KPI recalculation
    try:
        supabase_client.rpc("compute_kpis_for", {{"entity": "tasks"}})
        print("📊 Triggered KPI recalculation for: tasks")
    except Exception as e:
        print("⚠️ KPI recalculation failed:", str(e))
