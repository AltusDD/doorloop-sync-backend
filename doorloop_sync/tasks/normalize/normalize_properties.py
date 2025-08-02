from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run(supabase: SupabaseIngestClient):
    print("🚀 Running normalize_properties")
    from doorloop_sync.utils.load_raw_data import load_raw_data
    from doorloop_sync.utils.transformers.properties import transform_properties

    raw_properties = load_raw_data("doorloop_raw_properties", supabase)
    normalized = transform_properties(raw_properties)

    supabase.upsert_records("doorloop_normalized_properties", normalized)
    print(f"✅ Normalized {len(normalized)} properties.")