"""
This module syncs Owners from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Owners...")
        records = doorloop.get_all("/owners")
        supabase.upsert("doorloop_raw_owners", records)
        print(f"✅ Synced {len(records)} Owners")
    except Exception as e:
        print(f"❌ Error syncing Owners: {e}")
