"""
This module syncs Communications from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Communications...")
        records = doorloop.get_all("/communications")
        supabase.upsert("doorloop_raw_communications", records)
        print(f"✅ Synced {len(records)} Communications")
    except Exception as e:
        print(f"❌ Error syncing Communications: {e}")
