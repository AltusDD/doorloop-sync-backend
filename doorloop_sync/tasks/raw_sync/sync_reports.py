"""
This module syncs Reports from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Reports...")
        records = doorloop.get_all("/reports")
        supabase.upsert("doorloop_raw_reports", records)
        print(f"✅ Synced {len(records)} Reports")
    except Exception as e:
        print(f"❌ Error syncing Reports: {e}")
