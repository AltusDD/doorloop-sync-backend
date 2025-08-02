"""
This module syncs Lease credits from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Lease credits...")
        records = doorloop.get_all("/lease-credits")
        supabase.upsert("doorloop_raw_lease_credits", records)
        print(f"✅ Synced {len(records)} Lease credits")
    except Exception as e:
        print(f"❌ Error syncing Lease credits: {e}")
