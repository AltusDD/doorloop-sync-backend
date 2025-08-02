"""
This module syncs Recurring charges from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Recurring charges...")
        records = doorloop.get_all("/recurring-charges")
        supabase.upsert("doorloop_raw_recurring_charges", records)
        print(f"✅ Synced {len(records)} Recurring charges")
    except Exception as e:
        print(f"❌ Error syncing Recurring charges: {e}")
