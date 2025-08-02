import uuid
from datetime import datetime
from doorloop_sync.config import supabase_client

def log_audit_event(entity, message, status="info", entity_type="general", batch_id=None):
    """
    Logs an audit event to the Supabase audit_log table.
    """
    event = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "entity": entity,
        "entity_type": entity_type,
        "message": message,
        "status": status,
        "batch_id": batch_id or str(uuid.uuid4()),
    }

    try:
        response = supabase_client.table("audit_log").insert(event).execute()
        print(f"📝 Audit logged: {event['entity']} - {event['message']}")
    except Exception as e:
        print(f"⚠️ Failed to log audit event: {e}")
def log_audit(event):
    print(f'📝 Audit log: {event}')
