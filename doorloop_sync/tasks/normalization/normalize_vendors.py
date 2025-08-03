import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

def run():
    supabase_client = SupabaseClient(...)
    normalized_table_name = "doorloop_normalized_vendors"
    raw_table_name = "doorloop_raw_vendors"

    schema_resp = supabase_client.supabase.table(normalized_table_name).select('*').limit(1).execute()
    if not schema_resp or not hasattr(schema_resp, 'data') or not schema_resp.data:
        logging.error(f"Could not fetch schema for {normalized_table_name}")
        return
    schema_fields = set(schema_resp.data[0].keys())

    raw_resp = supabase_client.supabase.table(raw_table_name).select('*').execute()
    raw_records = raw_resp.data if raw_resp and hasattr(raw_resp, 'data') else []

    normalized_records = []
    for r in raw_records:
        norm = {k: v for k, v in r.items() if k in schema_fields}
        normalized_records.append(norm)

    resp = supabase_client.upsert(normalized_table_name, normalized_records)
    if not resp or (hasattr(resp, 'status_code') and resp.status_code >= 400):
        logging.error(f"Upsert error: {getattr(resp, 'data', None)}")
        logging.error(f"Payload: {normalized_records}")
# silent_update
