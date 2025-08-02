"""
This module syncs Tasks from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Tasks...")
        records = doorloop.get_all("/tasks")
        supabase.upsert("doorloop_raw_tasks", records)
        print(f"✅ Synced {len(records)} Tasks")
    except Exception as e:
        print(f"❌ Error syncing Tasks: {e}")
