"""
This module syncs Portfolios from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Portfolios...")
        records = doorloop.get_all("/portfolios")
        supabase.upsert("doorloop_raw_portfolios", records)
        print(f"✅ Synced {len(records)} Portfolios")
    except Exception as e:
        print(f"❌ Error syncing Portfolios: {e}")
