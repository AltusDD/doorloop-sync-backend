"""
This module syncs Inspections from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Inspections...")
        records = doorloop.get_all("/inspections")
        supabase.upsert("doorloop_raw_inspections", records)
        print(f"✅ Synced {len(records)} Inspections")
    except Exception as e:
        print(f"❌ Error syncing Inspections: {e}")
