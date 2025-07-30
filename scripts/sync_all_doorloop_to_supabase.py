# Placeholder for full pipeline logic
from doorloop_sync.normalization import run_all_normalizations
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

supabase_client = SupabaseIngestClient()
run_all_normalizations(supabase_client)
