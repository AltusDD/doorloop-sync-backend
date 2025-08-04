
# PATCH_SILENT_TAG: writeback_logger_update
import os
import requests
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def log_writeback_audit(user_id, email, entity_type, action, payload):
    data = {
        "user_id": user_id,
        "email": email,
        "entity_type": entity_type,
        "action": action,
        "payload": payload,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat()
    }
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }
    res = requests.post(f"{SUPABASE_URL}/rest/v1/writeback_audit_log", json=data, headers=headers)
    if res.status_code not in [200, 201]:
        print(f"❌ Failed to log audit: {res.status_code} - {res.text}")
