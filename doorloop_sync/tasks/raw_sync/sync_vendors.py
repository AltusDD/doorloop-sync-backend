"""
This module syncs Vendors from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Vendors...")
        records = doorloop.get_all("/vendors")
        supabase.upsert("doorloop_raw_vendors", records)
        print(f"✅ Synced {len(records)} Vendors")
    except Exception as e:
        print(f"❌ Error syncing Vendors: {e}")
