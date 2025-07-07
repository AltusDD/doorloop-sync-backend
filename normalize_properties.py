
import requests
import os
import json
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def fetch_raw_properties():
    response = supabase.table("public.properties").select("*").execute()
    return response.data if response.data else []

def transform_property(record):
    address = record.get("address_json", {}) or {}
    return {
        "doorloop_id": record.get("doorloop_id"),
        "name": record.get("name"),
        "property_type": record.get("type"),
        "class": record.get("class"),
        "active": record.get("active"),
        "address_street1": address.get("street1"),
        "address_street2": address.get("street2"),
        "address_city": address.get("city"),
        "address_state": address.get("state"),
        "address_zip": address.get("zip"),
        "address_country": address.get("country"),
        "address_lat": address.get("lat"),
        "address_lng": address.get("lng"),
        "description": record.get("description"),
        "external_id": record.get("external_id"),
        "manager_id": record.get("manager_id"),
        "purchase_date": record.get("purchase_date"),
        "purchase_price": record.get("purchase_price"),
        "current_value": record.get("current_value"),
        "bedroom_count": record.get("bedroom_count"),
        "num_active_units": record.get("num_active_units"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "raw_payload": record.get("_raw_payload"),
    }

def upsert_properties():
    raw_props = fetch_raw_properties()
    transformed = [transform_property(p) for p in raw_props]
    if transformed:
        supabase.table("normalized_properties").upsert(transformed).execute()
        print(f"✅ Upserted {len(transformed)} properties")
    else:
        print("⚠️ No data to upsert.")

if __name__ == "__main__":
    upsert_properties()
