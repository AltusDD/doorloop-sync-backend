"""
This module syncs Recurring credits from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Recurring credits...")
        records = doorloop.get_all("/recurring-credits")
        supabase.upsert("doorloop_raw_recurring_credits", records)
        print(f"✅ Synced {len(records)} Recurring credits")
    except Exception as e:
        print(f"❌ Error syncing Recurring credits: {e}")
