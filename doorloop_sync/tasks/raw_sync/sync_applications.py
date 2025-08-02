"""
This module syncs Applications from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Applications...")
        records = doorloop.get_all("/applications")
        supabase.upsert("doorloop_raw_applications", records)
        print(f"✅ Synced {len(records)} Applications")
    except Exception as e:
        print(f"❌ Error syncing Applications: {e}")
