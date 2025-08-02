"""
This module syncs Units from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Units...")
        records = doorloop.get_all("/units")
        supabase.upsert("doorloop_raw_units", records)
        print(f"✅ Synced {len(records)} Units")
    except Exception as e:
        print(f"❌ Error syncing Units: {e}")
