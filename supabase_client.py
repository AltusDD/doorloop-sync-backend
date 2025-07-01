import os
import requests
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any

def _safe_json_dumps(data):
    try:
        return json.dumps(data, default=str)
    except Exception as e:
        print(f"❌ JSON dump failed: {e}")
        return json.dumps({"_json_dump_error": str(e)})

def _convert_date_to_iso(date_str):
    try:
        if not date_str:
            return None
        if isinstance(date_str, datetime):
            return date_str.isoformat(timespec='seconds')
        if 'T' in date_str:
            return datetime.fromisoformat(date_str).isoformat(timespec='seconds')
        return datetime.strptime(date_str, "%Y-%m-%d").isoformat()
    except Exception as e:
        print(f"⚠️ Date parse failed for '{date_str}': {e}")
        return None

def upsert_raw_doorloop_data(endpoint: str, records: List[Dict[str, Any]], supabase_url: str, supabase_service_role_key: str):
    print(f"🔁 upsert_raw_doorloop_data called for endpoint: {endpoint}")
    print(f"📦 Records count: {len(records)}")

    if not all([endpoint, records, supabase_url, supabase_service_role_key]):
        print("❌ Missing required parameter(s).")
        return

    mirror_table_name = endpoint.strip("/").replace("-", "_")
    mirror_url = f"{supabase_url}/rest/v1/doorloop_raw_{mirror_table_name}"
    print(f"📡 Target Supabase table: doorloop_raw_{mirror_table_name}")
    print(f"🔑 Supabase Key (start): {supabase_service_role_key[:5]}...")

    headers = {
        "apikey": supabase_service_role_key,
        "Authorization": f"Bearer {supabase_service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    for i, record in enumerate(records):
        try:
            entity_id = record.get("id", None)
            if not entity_id:
                print(f"⚠️ Skipping record {i+1}: missing 'id'")
                continue

            print(f"➡️ Preparing record {i+1} (ID: {entity_id})")
            mapped_record = {
                "doorloop_id": str(entity_id),
                "name": record.get("name", f"Record_{i+1}"),
                "_raw_payload": record,
                "created_at": _convert_date_to_iso(record.get("createdAt")),
                "updated_at": _convert_date_to_iso(record.get("updatedAt"))
            }

            response = requests.post(mirror_url, headers=headers, json=[mapped_record])
            if response.status_code >= 400:
                print(f"❌ Record {i+1}: Failed with status {response.status_code}")
                try:
                    print(f"   → Error: {response.json()}")
                except Exception:
                    print(f"   → Raw: {response.text}")
            else:
                print(f"✅ Record {i+1}: Inserted successfully.")

        except Exception as e:
            print(f"❌ Unexpected error on record {i+1}: {e}")

    print(f"✅ Finished upserting {len(records)} records to {mirror_table_name}")
