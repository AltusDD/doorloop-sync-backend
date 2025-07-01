def upsert_raw_doorloop_data(endpoint, records, supabase_url, supabase_key):
    import supabase
    from supabase import create_client

    supabase = create_client(supabase_url, supabase_key)
    table_name = f"doorloop_raw_{endpoint.strip('/').replace('-', '_')}"

    for record in records:
        data = {
            "doorloop_id": record.get("id"),
            "name": record.get("name"),
            "_raw_payload": record,
        }
        try:
            response = supabase.table(table_name).upsert(data).execute()
            if hasattr(response, 'status_code') and response.status_code >= 400:
                print(f"❌ Record {record.get('id')}: Failed with status {response.status_code}")
            else:
                print(f"✅ Record {record.get('id')}: Upserted successfully.")
        except Exception as e:
            print(f"❌ Record {record.get('id')}: Exception during upsert → {e}")
