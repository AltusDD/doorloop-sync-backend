"""
This module syncs Files from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Files...")
        records = doorloop.get_all("/files")
        supabase.upsert("doorloop_raw_files", records)
        print(f"✅ Synced {len(records)} Files")
    except Exception as e:
        print(f"❌ Error syncing Files: {e}")
