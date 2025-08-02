# doorloop_sync/config.py

import os
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

_doorloop_client = None
_supabase_client = None

def get_doorloop_client():
    global _doorloop_client
    if _doorloop_client is None:
        _doorloop_client = DoorLoopClient(
            api_key=os.environ["DOORLOOP_API_KEY"],
            base_url=os.environ["DOORLOOP_API_BASE_URL"]
        )
    return _doorloop_client

def get_supabase_client():
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = SupabaseIngestClient()
    return _supabase_client
