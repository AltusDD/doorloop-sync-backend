def sync_table(raw_table, normalized_table, field_map):
    print(f"🔄 Syncing {raw_table} → {normalized_table}")

    raw_data = supabase.table(raw_table).select("*").execute().data
    records_to_insert = []

    for record in raw_data:
        new_record = {}

        # ✅ ALWAYS generate a UUID for internal use
        new_record["id"] = str(uuid.uuid4())

        # ✅ Store the DoorLoop _id separately
        new_record["doorloop_id"] = record.get("_id")

        # ✅ Populate mapped fields
        for raw_field, normalized_field in field_map.items():
            if normalized_field not in ("id", "doorloop_id"):
                new_record[normalized_field] = record.get(raw_field)

        records_to_insert.append(new_record)

    if records_to_insert:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
        print(f"✅ Synced {len(records_to_insert)} records to {normalized_table}")
    else:
        print("⚠️ No records to sync.")
