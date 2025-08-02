# doorloop_sync/audit/logger.py

import datetime
import json

def log_audit_event(entity, status, *, error=False, metadata=None):
    timestamp = datetime.datetime.utcnow().isoformat()
    print(json.dumps({
        "timestamp": timestamp,
        "entity": entity,
        "status": status,
        "error": error,
        "metadata": metadata or {}
    }))
