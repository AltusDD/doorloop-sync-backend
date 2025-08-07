import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_properties():
    """
    Fetches raw property data, transforms it, and upserts it into the
    final 'properties' table.
    """
    supabase = SupabaseClient()
    raw_properties = supabase.fetch_all('doorloop_raw_properties')
    if not raw_properties:
        logger.warning("No raw properties found to normalize.")
        return

    normalized_properties = []
    for item in raw_properties:
        record = item.get('data', {})
        address = record.get('address', {})
        normalized_properties.append({
            'doorloop_id': record.get('id'),
            'name': record.get('name'),
            'type': record.get('type'),
            'class': record.get('class'),
            'active': record.get('active'),
            'address_street1': address.get('street1'),
            'address_city': address.get('city'),
            'address_state': address.get('state'),
            'address_zip': address.get('zip'),
        })
    
    if normalized_properties:
        supabase.upsert('properties', normalized_properties)
        logger.info(f"Successfully normalized and upserted {len(normalized_properties)} properties.")