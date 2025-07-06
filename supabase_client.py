import logging
import requests
import json
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url: str, service_role_key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def upsert_data(self, table: str, records: list):
        if not records:
            logger.info(f"⚠️ No records to upsert for table: {table}")
            return

        # 🔧 Normalize all records to have identical keys
        all_keys = set(k for record in records for k in record)
        normalized_records = []
        for record in records:
            fixed = {}
            for key in all_keys:
                value = record.get(key, None)

                # 💡 Convert float-like strings to Decimal-safe format
                if isinstance(value, str):
                    try:
                        if "." in value and value.replace(".", "", 1).isdigit():
                            value = str(Decimal(value))
                    except InvalidOperation:
                        pass  # Leave value unchanged if it's not a number

                fixed[key] = value
            normalized_records.append(fixed)

        url = f"{self.url}/rest/v1/{table}?on_conflict=id"
        logger.debug(f"📤 Posting {len(normalized_records)} records to {url}")
        try:
            r = requests.post(url, headers=self.headers, data=json.dumps(normalized_records))
            r.raise_for_status()
            logger.info(f"✅ Supabase upsert succeeded for {table}")
        except requests.exceptions.HTTPError as e:
            if r.status_code == 409:
                logger.warning(f"⚠️ Supabase 409 Conflict for {table}: {r.text}")
            else:
                logger.error(f"❌ Supabase insert failed for {table}: {r.status_code} → {r.text}")
                raise
