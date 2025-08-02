"""
This module syncs Lease payments from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Lease payments...")
        records = doorloop.get_all("/lease-payments")
        supabase.upsert("doorloop_raw_lease_payments", records)
        print(f"✅ Synced {len(records)} Lease payments")
    except Exception as e:
        print(f"❌ Error syncing Lease payments: {e}")
