import os
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def get_doorloop_client():
    return DoorLoopClient(api_key=os.getenv("DOORLOOP_API_KEY"), base_url=os.getenv("DOORLOOP_API_BASE_URL"))

def get_supabase_client():
    return SupabaseIngestClient(
        supabase_url=os.getenv("SUPABASE_URL"),
        supabase_service_role_key=os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
    )