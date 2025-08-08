import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_units():
    """
    Fetches raw unit data, transforms it, and upserts it into the
    final 'units' table.
    """
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
            'name': record.get('name'),
            'beds': record.get('beds'),
            'baths': record.get('baths'),
            'size': record.get('size'),
            # ✅ FIX: Changed key to match the database blueprint
            'rent_amount': record.get('marketRent'),
            'doorloop_property_id': record.get('property'),
        })
    
    if normalized_units:
        supabase.upsert('units', normalized_units, on_conflict_column='doorloop_id')
        logger.info(f"Successfully normalized and upserted {len(normalized_units)} units.")
