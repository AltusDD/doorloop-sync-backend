import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger("ETL_Orchestrator")

def get_doorloop_client():
    return DoorLoopClient()

def get_supabase_client():
    return SupabaseClient()