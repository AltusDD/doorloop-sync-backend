import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_units():
    supabase = SupabaseClient()
    raw_units = supabase.fetch_all('doorloop_raw_units')
    if not raw_units:
        logger.warning("No raw units found to normalize.")
        return

    normalized_units = []
    for item in raw_units:
        record = item.get('data', {})
        normalized_units.append({
            'doorloop_id': record.get('id'),
            'unit_number': record.get('name'),
            'beds': record.get('beds'),
            'baths': record.get('baths'),
            'sq_ft': record.get('size'),
            'rent_amount': record.get('marketRent'),
            'doorloop_property_id': record.get('property'),
        })
    
    if normalized_units:
        # ✅ FIX: Explicitly set the conflict column to 'doorloop_id'
        supabase.upsert('units', normalized_units, on_conflict_column='doorloop_id')
        logger.info(f"Successfully normalized and upserted {len(normalized_units)} units.")