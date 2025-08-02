"""
This module syncs Tenants from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Tenants...")
        records = doorloop.get_all("/tenants")
        supabase.upsert("doorloop_raw_tenants", records)
        print(f"✅ Synced {len(records)} Tenants")
    except Exception as e:
        print(f"❌ Error syncing Tenants: {e}")
