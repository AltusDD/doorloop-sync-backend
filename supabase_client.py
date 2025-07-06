import os
import requests
import json
import hashlib
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

def _safe_json_dumps(data: Any) -> Optional[str]:
    if data is None: return None
    try:
        return json.dumps(data, default=str)
    except TypeError as e:
        _logger.warning(f"WARN: Failed to dump data to JSON for JSONB storage: {type(data)} - {data}. Error: {e}")
        return str(data)

def _convert_date_to_iso(date_str: Optional[str]) -> Optional[str]:
    if not date_str: return None
    if isinstance(date_str, datetime): return date_str.isoformat(timespec='seconds')
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
    except ValueError:
        _logger.warning(f"WARN: Could not parse date string '{date_str}'. Returning original string.")
        return date_str

def upsert_raw_doorloop_data(endpoint: str, records: List[Dict[str, Any]], supabase_url: str, supabase_service_role_key: str):
    if not supabase_service_role_key:
        raise ValueError("Supabase service role key is missing.")
    if not supabase_url:
        raise ValueError("Supabase URL is missing.")
    if not endpoint:
        raise ValueError("Endpoint is missing for raw data upsert.")
    if not isinstance(records, list):
        raise TypeError(f"Expected records to be a list, got {type(records)}")

    headers = {
        "apikey": supabase_service_role_key,
        "Authorization": f"Bearer {supabase_service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    base_rest_url = f"{supabase_url}/rest/v1"
    raw_doorloop_data_table_url = f"{base_rest_url}/raw_doorloop_data"
    table_name_raw = f"doorloop_raw_{endpoint.strip('/').replace('-', '_')}"
    mirror_table_url = f"{base_rest_url}/{table_name_raw}"

    _logger.info(f"DEBUG_SUPABASE_CLIENT: Starting raw upsert for {endpoint} ({len(records)} records)")

    prepared_raw_doorloop_data = []
    prepared_mirror_table_data = []

    for i, record in enumerate(records):
        try:
            entity_dl_id = record.get('id', record.get('ID'))
            if not entity_dl_id:
                _logger.warning(f"WARN_RECORD_PROCESS: Record {i+1} from {endpoint} has no identifiable ID. Skipping: {record}")
                continue

            payload_hash = hashlib.sha256(_safe_json_dumps(record).encode('utf-8')).hexdigest()

            prepared_raw_doorloop_data.append({
                "endpoint": endpoint,
                "entity_dl_id": str(entity_dl_id),
                "payload_json": record,
                "payload_hash": payload_hash
            })

            mapped_mirror_record = {"doorloop_id": str(entity_dl_id), "_raw_payload": record}

            # You would paste all the detailed if/elif mappings here as shown in our full ingestion blueprint
            # For brevity, you may continue filling this in with Gemini’s or my guidance.

            prepared_mirror_table_data.append(mapped_mirror_record)

        except Exception as e:
            _logger.error(f"ERROR_RECORD_PROCESS: Failed to process record {i+1} (ID: {record.get('id')}) for {endpoint}: {type(e).__name__}: {str(e)}. Record: {record}")
            continue

    if prepared_raw_doorloop_data:
        try:
            raw_response = requests.post(raw_doorloop_data_table_url, headers=headers, json=prepared_raw_doorloop_data, timeout=60)
            raw_response.raise_for_status()
            print(f"DEBUG_SUPABASE_CLIENT: Raw data upsert successful for {endpoint}.")
        except requests.exceptions.RequestException as e:
            print(f"ERROR_RAW_UPSERT: Failed to upsert raw data for {endpoint}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}")
            raise

    if prepared_mirror_table_data:
        try:
            mirror_response = requests.post(
                mirror_table_url,
                headers=headers,
                json=prepared_mirror_table_data,
                timeout=60
            )
            mirror_response.raise_for_status()
            print(f"DEBUG_SUPABASE_CLIENT: Mirror table {table_name_raw} upsert successful.")
        except requests.exceptions.RequestException as e:
            print(f"ERROR_MIRROR_UPSERT: Failed to upsert to {table_name_raw}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}")
            raise

    print(f"DEBUG_SUPABASE_CLIENT: Completed raw ingestion for {endpoint}.")
