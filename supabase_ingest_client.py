def upsert_data(self, table_name, records):
    import decimal
    import json

    def sanitize(record):
        clean = {}
        for k, v in record.items():
            if isinstance(v, float):
                # Convert float to string to avoid bigint crash
                clean[k] = str(v)
            elif isinstance(v, decimal.Decimal):
                clean[k] = str(v)
            elif isinstance(v, dict) or isinstance(v, list):
                clean[k] = json.dumps(v)  # serialize nested structures
            else:
                clean[k] = v
        return clean

    try:
        if not records:
            logger.warning(f"⚠️ No records to upsert for {table_name}")
            return

        clean_records = [sanitize(r) for r in records]
        response = requests.post(
            f"{self.url}/rest/v1/{table_name}?on_conflict=id",
            headers=self.headers,
            json=clean_records
        )

        if response.status_code in [200, 201, 204]:
            logger.info(f"✅ Upsert successful for {table_name}")
        else:
            logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} → {response.text}")
            response.raise_for_status()
    except Exception as e:
        logger.exception(f"🔥 Exception during upsert → {e}")
        raise
