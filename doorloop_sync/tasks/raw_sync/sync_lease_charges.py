"""
This module syncs Lease charges from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Lease charges...")
        records = doorloop.get_all("/lease-charges")
        supabase.upsert("doorloop_raw_lease_charges", records)
        print(f"✅ Synced {len(records)} Lease charges")
    except Exception as e:
        print(f"❌ Error syncing Lease charges: {e}")
