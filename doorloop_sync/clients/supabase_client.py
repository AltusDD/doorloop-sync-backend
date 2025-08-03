import os
from supabase import create_client, Client
import logging
from postgrest.exceptions import APIError

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, supabase_url=None, service_role_key=None):
        supabase_url = supabase_url or os.getenv("SUPABASE_URL")
        service_role_key = service_role_key or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not supabase_url or not service_role_key:
            raise ValueError("Supabase URL or service role key missing.")

        self.supabase: Client = create_client(supabase_url, service_role_key)
        logger.info("✅ SupabaseClient initialized.")

    def upsert(self, table: str, data: list):
        if not data:
            return
        unified_data = self._unify_keys(data)
        try:
            self.supabase.table(table).upsert(unified_data).execute()
        except APIError as e:
            logger.error(f"❌ Failed to upsert data to {table}: {e.args[0]}")
            raise

    def get_raw(self, entity: str) -> list:
        table = f"doorloop_raw_{entity}"
        response = self.supabase.table(table).select("*").execute()
        return response.data or []

    def _unify_keys(self, data: list) -> list:
        """Ensure all dictionaries have the same keys to satisfy PGRST102 errors."""
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())
        unified = []
        for row in data:
            unified.append({key: row.get(key, None) for key in all_keys})
        return unified
