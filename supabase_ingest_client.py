def upsert_data(self, table_name, records):
    if not records:
        logger.warning(f"⚠️ No records to upsert for {table_name}")
        return

    # Step 1: Collect superset of keys
    all_keys = set()
    for record in records:
        all_keys.update(record.keys())

    # Step 2: Normalize all records to have same keys
    normalized_records = []
    for record in records:
        normalized = {key: record.get(key, None) for key in all_keys}
        normalized_records.append(normalized)

    # Step 3: Send to Supabase
    url = f"{self.supabase_url}/rest/v1/{table_name}?on_conflict=id"
    headers = {
        "apikey": self.service_role_key,
        "Authorization": f"Bearer {self.service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    response = requests.post(url, headers=headers, json=normalized_records)

    if response.status_code == 409:
        logger.warning(f"⚠️ Supabase 409 Conflict for {table_name}: {response.text}")
    elif response.status_code != 201:
        logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} → {response.text}")
        response.raise_for_status()
    else:
        logger.info(f"✅ {len(records)} records upserted to {table_name}")
