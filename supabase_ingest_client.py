import requests
import logging
import json

logger = logging.getLogger(__name__)

class SupabaseIngestClient:
    def __init__(self, supabase_url, service_role_key):
        self.supabase_url = supabase_url
        self.service_role_key = service_role_key

    def upsert_data(self, table_name, records):
        if not records:
            logger.warning(f"⚠️ No records to upsert for {table_name}")
            return

        normalized_records = []
        for record in records:
            if not isinstance(record, dict):
                continue

            # 🔄 Normalize and store full raw payload
            normalized_record = {
                "id": str(record.get("id")),  # ID must be text, Supabase expects string key
                "data": record,               # full original payload under 'data' jsonb field
                "batch": "default"            # optional: useful for batch tracking
            }
            normalized_records.append(normalized_record)

        url = f"{self.supabase_url}/rest/v1/{table_name}?on_conflict=id"
        headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

        try:
            response = requests.post(url, headers=headers, json=normalized_records)
        except Exception as e:
            logger.error(f"❌ Request to Supabase failed for {table_name}: {e}")
            raise

        if response.status_code == 201:
            logger.info(f"✅ {len(normalized_records)} records upserted to {table_name}")
        elif response.status_code == 409:
            logger.warning(f"⚠️ Supabase 409 Conflict for {table_name}: {response.text}")
        elif response.status_code == 200:
            if response.text.strip():
                logger.warning(f"⚠️ Supabase 200 OK with content for {table_name}")
                logger.debug(f"📦 Response body:\n{response.text}")
            else:
                logger.info(f"ℹ️ Supabase 200 OK with empty body — likely merge/no-op for {table_name}")
        else:
            logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} → {response.text}")
            response.raise_for_status()

        logger.debug(f"📤 Payload sample for {table_name}:\n{json.dumps(normalized_records[:2], indent=2)}")
