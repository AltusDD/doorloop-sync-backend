import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_leases():
    """
    Fetches raw lease data, transforms it, and upserts it into the
    final 'leases' table.
    """
    supabase = SupabaseClient()
    raw_leases = supabase.fetch_all('doorloop_raw_leases')
    if not raw_leases:
        logger.warning("No raw leases found to normalize.")
        return

    normalized_leases = []
    for item in raw_leases:
        record = item.get('data', {})
        # Assuming the first unit and tenant are the primary ones for this lease
        primary_unit_id = record.get('units', [None])[0]
        primary_tenant_id = record.get('tenants', [None])[0]

        normalized_leases.append({
            'doorloop_id': record.get('id'),
            'name': record.get('name'),
            'start_date': record.get('start'),
            'end_date': record.get('end'),
            'term': record.get('term'),
            'status': record.get('status'),
            'total_balance_due': record.get('totalBalanceDue'),
            'doorloop_property_id': record.get('property'),
            'doorloop_primary_unit_id': primary_unit_id,
            'doorloop_primary_tenant_id': primary_tenant_id,
        })

    if normalized_leases:
        supabase.upsert('leases', normalized_leases)
        logger.info(f"Successfully normalized and upserted {len(normalized_leases)} leases.")