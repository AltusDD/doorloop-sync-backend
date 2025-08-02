"""
This module syncs Notes from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Notes...")
        records = doorloop.get_all("/notes")
        supabase.upsert("doorloop_raw_notes", records)
        print(f"✅ Synced {len(records)} Notes")
    except Exception as e:
        print(f"❌ Error syncing Notes: {e}")
