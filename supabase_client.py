import requests
import os
import logging
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

logger = logging.getLogger(__name__)

def to_snake_case(name):
    return ''.join(['_' + c.lower() if c.isupper() else c for c in name]).lstrip('_')

def upsert_data(table_name, records, primary_key_field="id"):
    if not records:
        logger.info(f"No records for {table_name}")
        return

    transformed = []
    for record in records:
        item = {}
        for k, v in record.items():
            key = to_snake_case(k)
            if key == "class":
                key = "class_name"
            if isinstance(v, datetime):
                item[key] = v.isoformat()
            else:
                item[key] = v
        transformed.append(item)

    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict={primary_key_field}"
    try:
        r = requests.post(url, headers=HEADERS, json=transformed)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Insert failed for {table_name}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        raise
