"""
This module syncs Users from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Users...")
        records = doorloop.get_all("/users")
        supabase.upsert("doorloop_raw_users", records)
        print(f"✅ Synced {len(records)} Users")
    except Exception as e:
        print(f"❌ Error syncing Users: {e}")
