from doorloop.api import fetch_all
from altus_supabase.client import upsert_records

FIELDS = [
    'name',
    'type',
    'class',
    'address_street1',
    'address_street2',
    'address_city',
    'address_state',
    'address_zip',
    'owners',
    'amenities',
    'notes'
]

def sync():
    all_data = fetch_all("properties")
    cleaned = []

    for record in all_data:
        flat = {}
        for field in FIELDS:
            flat[field] = record.get(field)
        cleaned.append(flat)

    return upsert_records("properties", cleaned)
