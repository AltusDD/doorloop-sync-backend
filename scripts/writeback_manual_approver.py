
# PATCH_SILENT_TAG: writeback_manual_approver
import os
import requests
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def approve_writeback(entry_id, approver):
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "status": "approved",
        "approved_by": approver,
        "approved_at": datetime.utcnow().isoformat()
    }
    res = requests.patch(
        f"{SUPABASE_URL}/rest/v1/writeback_audit_log?id=eq.{entry_id}",
        json=payload, headers=headers
    )
    if res.status_code not in [200, 204]:
        print(f"❌ Approval failed: {res.status_code} - {res.text}")
    else:
        print(f"✅ Writeback {entry_id} approved by {approver}")
