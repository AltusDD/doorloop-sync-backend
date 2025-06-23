import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_records(table: str, records: list, pk: str = "id"):
    if not records:
        return {"status": "empty", "inserted": 0}
    client = get_client()
    result = client.table(table).upsert(records, on_conflict=pk).execute()
    return {"status": "success", "inserted": len(records), "raw": result}
