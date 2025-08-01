from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run(supabase: SupabaseIngestClient):
    print("🚀 Running normalize_units")
    from doorloop_sync.utils.load_raw_data import load_raw_data
    from doorloop_sync.utils.transformers.units import transform_units

    raw_units = load_raw_data("doorloop_raw_units", supabase)
    normalized = transform_units(raw_units)

    supabase.upsert_records("doorloop_normalized_units", normalized)
    print(f"✅ Normalized {len(normalized)} units.")