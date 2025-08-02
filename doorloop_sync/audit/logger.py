# doorloop_sync/audit/logger.py

import os
from supabase import create_client
from datetime import datetime

def log_audit_event(entity: str, status: str, error: bool, metadata: dict = None):
    try:
        client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "entity": entity,
            "status": status,
            "error": error,
            "metadata": metadata or {}
        }
        client.table("audit_logs").insert(payload).execute()
        print(f"[📋] Audit log: {payload}")
    except Exception as e:
        print(f"[❌] Failed to log audit event: {e}")
