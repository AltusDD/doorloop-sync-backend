"""
This module syncs Payments from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Payments...")
        records = doorloop.get_all("/payments")
        supabase.upsert("doorloop_raw_payments", records)
        print(f"✅ Synced {len(records)} Payments")
    except Exception as e:
        print(f"❌ Error syncing Payments: {e}")
