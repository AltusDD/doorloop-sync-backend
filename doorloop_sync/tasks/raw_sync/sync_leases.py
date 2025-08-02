"""
This module syncs Leases from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Leases...")
        records = doorloop.get_all("/leases")
        supabase.upsert("doorloop_raw_leases", records)
        print(f"✅ Synced {len(records)} Leases")
    except Exception as e:
        print(f"❌ Error syncing Leases: {e}")
