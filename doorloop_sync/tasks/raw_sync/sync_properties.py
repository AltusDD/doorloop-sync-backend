"""
This module syncs Properties from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Properties...")
        records = doorloop.get_all("/properties")
        supabase.upsert("doorloop_raw_properties", records)
        print(f"✅ Synced {len(records)} Properties")
    except Exception as e:
        print(f"❌ Error syncing Properties: {e}")
