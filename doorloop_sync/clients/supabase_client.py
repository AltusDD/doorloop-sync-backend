import logging

class SupabaseClient:
    def __init__(self, supabase):
        self.supabase = supabase

    def upsert(self, table, data):
        try:
            resp = self.supabase.table(table).upsert(data).execute()
            if hasattr(resp, 'status_code') and resp.status_code >= 400:
                logging.error(f"Upsert failed for table {table}: {getattr(resp, 'data', None)}")
                logging.error(f"Payload: {data}")
            return resp
        except Exception as e:
            logging.error(f"Exception during upsert to {table}: {e}")
            logging.error(f"Payload: {data}")
            return None

    def select(self, table, *args, **kwargs):
        try:
            resp = self.supabase.table(table).select(*args, **kwargs).execute()
            return resp
        except Exception as e:
            logging.error(f"Exception during select from {table}: {e}")
            return None