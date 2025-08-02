"""
This module syncs Accounts from DoorLoop API into Supabase.
"""

from doorloop_sync.config import get_doorloop_client, get_supabase_client

supabase = get_supabase_client()
doorloop = get_doorloop_client()

def run():
    try:
        print("🔄 Syncing Accounts...")
        records = doorloop.get_all("/accounts")
        supabase.upsert("doorloop_raw_accounts", records)
        print(f"✅ Synced {len(records)} Accounts")
    except Exception as e:
        print(f"❌ Error syncing Accounts: {e}")
