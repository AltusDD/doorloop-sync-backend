# supabase_client.py

import os
import requests
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional

# === JSON & Date Helpers ===

def _safe_json_dumps(data: Any) -> Optional[str]:
    if data is None:
        return None
    try:
        return json.dumps(data, default=str)
    except TypeError:
        return str(data)

def _convert_date_to_iso(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    if isinstance(date_str, datetime):
        return date_str.isoformat(timespec='seconds')
    try:
        if 'T' in date_str:
            if date_str.endswith('Z'):
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
            elif '+' in date_str:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
            else:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        else:
            dt_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return dt_obj.isoformat(timespec='seconds')
    except Exception:
        return date_str

# === Main Upsert Function ===

def upsert_raw_doorloop_data(
    endpoint: str,
    records: List[Dict[str, Any]],
    supabase_url: str,
    supabase_service_role_key: str
):
    if not endpoint:
        raise ValueError("Missing 'endpoint' argument.")
    if not isinstance(records, list):
        raise TypeError("Records must be a list.")
    if not supabase_url or not supabase_service_role_key:
        raise ValueError("Supabase URL or key missing.")

    headers = {
        "apikey": supabase_service_role_key,
        "Authorization": f"Bearer {supabase_service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    base_rest_url = f"{supabase_url}/rest/v1"
    raw_url = f"{base_rest_url}/raw_doorloop_data"
    mirror_table = endpoint.strip('/').replace('-', '_')
    mirror_url = f"{base_rest_url}/doorloop_raw_{mirror_table}"

    raw_payload = []
    mirror_payload = []

    for record in records:
        entity_dl_id = record.get("id") or record.get("ID")
        if not entity_dl_id:
            print(f"Skipping record with no ID: {record}")
            continue

        # Prepare raw table payload
        raw_payload.append({
            "endpoint": endpoint,
            "entity_dl_id": str(entity_dl_id),
            "payload_json": record,
            "payload_hash": hashlib.sha256(_safe_json_dumps(record).encode()).hexdigest()
        })

        # Base mirror payload
        mirror_payload.append({
            "doorloop_id": str(entity_dl_id),
            "_raw_payload": record,
            "created_at": _convert_date_to_iso(record.get("createdAt")),
            "updated_at": _convert_date_to_iso(record.get("updatedAt"))
        })

    if raw_payload:
        try:
            r = requests.post(raw_url, headers=headers, json=raw_payload, timeout=60)
            r.raise_for_status()
            print(f"✅ Raw table success: {endpoint}")
        except requests.RequestException as e:
            print(f"❌ Raw insert failed: {str(e)}")

    if mirror_payload:
        try:
            r = requests.post(mirror_url, headers=headers, json=mirror_payload, timeout=60)
            r.raise_for_status()
            print(f"✅ Mirror table success: {endpoint}")
        except requests.RequestException as e:
            print(f"❌ Mirror insert failed: {str(e)}")
