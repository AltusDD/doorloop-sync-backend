from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.utils.supabase_tools import upsert_raw_records

def sync():
    doorloop = DoorLoopClient()
    endpoint = '/api/tenants'
    all_records = doorloop.get_all(endpoint)
    upsert_raw_records('doorloop_raw_tenants', all_records)
