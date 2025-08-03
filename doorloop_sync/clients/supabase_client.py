
from supabase import create_client
import logging

class SupabaseClient:
    def __init__(self):
        from doorloop_sync.config import config
        self.supabase = create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_ROLE_KEY)
        logging.info("✅ SupabaseClient initialized.")

    def upsert(self, table, data):
        if not data:
            return
        # Collect all possible keys
        all_keys = set()
        for record in data:
            all_keys.update(record.keys())
        all_keys = list(all_keys)

        # Normalize all rows to include all keys
        unified_data = []
        for record in data:
            unified_record = {key: record.get(key, None) for key in all_keys}
            unified_data.append(unified_record)

        self.supabase.table(table).upsert(unified_data).execute()

    def get_raw(self, entity):
        table_name = f"doorloop_raw_{entity}"
        response = self.supabase.table(table_name).select("*").execute()
        return response.data or []

    def upsert_normalized(self, entity, data):
        table_name = f"doorloop_normalized_{entity}"
        self.upsert(table_name, data)
