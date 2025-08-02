"""
This module syncs Insurance policies from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Insurance policies...")
        records = doorloop.get_all("/insurance-policies")
        supabase.upsert("doorloop_raw_insurance_policies", records)
        print(f"✅ Synced {len(records)} Insurance policies")
    except Exception as e:
        print(f"❌ Error syncing Insurance policies: {e}")
