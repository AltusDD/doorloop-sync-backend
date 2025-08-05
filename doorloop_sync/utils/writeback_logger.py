# doorloop_sync/utils/writeback_logger.py

from doorloop_sync.clients.supabase_client import SupabaseClient
from datetime import datetime
import uuid

def log_write_attempt(
    user_id: str,
    action: str,
    entity: str,
    entity_id: str,
    status: str,
    message: str = ""
):
    """
    Writes a log entry to the writeback_logs table in Supabase.

    Args:
        user_id (str): Who triggered the write (e.g., 'admin_123').
        action (str): Type of action (POST, PATCH, DELETE).
        entity (str): The DoorLoop object affected (e.g., 'lease').
        entity_id (str): ID of the affected object.
        status (str): 'success' or 'fail'.
        message (str): Optional context (e.g., error message).
    """
    supabase = SupabaseClient()
    log_entry = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "action": action,
        "entity": entity,
        "entity_id": entity_id,
        "status": status,
        "message": message,
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        supabase.client.table("writeback_logs").insert(log_entry).execute()
    except Exception as e:
        print(f"[WRITEBACK LOG ERROR] Could not log to Supabase: {e}")
