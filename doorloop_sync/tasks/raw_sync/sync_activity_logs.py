"""
This module syncs Activity logs from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Activity logs...")
        records = doorloop.get_all("/activity-logs")
        supabase.upsert("doorloop_raw_activity_logs", records)
        print(f"✅ Synced {len(records)} Activity logs")
    except Exception as e:
        print(f"❌ Error syncing Activity logs: {e}")
