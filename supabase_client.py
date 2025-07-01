import os
import json
import requests
import logging
from typing import List, Dict, Any

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

def upsert_raw_doorloop_data(endpoint: str, records: List[Dict[str, Any]], supabase_url: str, supabase_service_role_key: str):
    """
    Upload DoorLoop records to both the generic raw_doorloop_data table and the specific mirror table
    like doorloop_raw_properties, doorloop_raw_units, etc.
    """
    if not supabase_service_role_key or not supabase_url:
        raise ValueError("Supabase URL or Service Role Key missing.")

    headers = {
        "apikey": supabase_service_role_key,
        "Authorization": f"Bearer {supabase_service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    base_rest_url = f"{supabase_url}/rest/v1"
    raw_table_url = f"{base_rest_url}/raw_doorloop_data"
    mirror_table = f"doorloop_raw_{endpoint.strip('/').replace('-', '_')}"
    mirror_table_url = f"{base_rest_url}/{mirror_table}"

    _logger.info(f"[SYNC] Upserting {len(records)} records into raw_doorloop_data and {mirror_table}")

    raw_payload = []
    mirror_payload = []

    for r in records:
        if 'id' not in r:
            _logger.warning(f"[SKIP] Record missing ID: {r}")
            continue

        raw_payload.append({
            "endpoint": endpoint,
            "entity_dl_id": str(r.get("id")),
            "payload_json": r,
            "payload_hash": "not_implemented"
        })

        mirror_payload.append({
            "doorloop_id": str(r.get("id")),
            "_raw_payload": r
        })

    try:
        if raw_payload:
            r1 = requests.post(raw_table_url, headers=headers, json=raw_payload, timeout=60)
            r1.raise_for_status()
            _logger.info("[SUCCESS] Raw payload uploaded.")
        if mirror_payload:
            r2 = requests.post(mirror_table_url, headers=headers, json=mirror_payload, timeout=60)
            r2.raise_for_status()
            _logger.info(f"[SUCCESS] Mirror table {mirror_table} uploaded.")
    except requests.exceptions.RequestException as e:
        _logger.error(f"[FAILURE] Error posting to Supabase: {e}")
        raise