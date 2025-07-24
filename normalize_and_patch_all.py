import json
import uuid
import requests
from supabase_client import supabase
from utils import camel_to_snake_case, mongodb_id_to_uuid

def transform_property(raw):
    return {
        'id': mongodb_id_to_uuid(raw['_id']),
        'doorloop_id': raw['_id'],
        **{
            camel_to_snake_case(k): v
            for k, v in raw.items()
            if k not in ['_id']
        }
    }

def normalize_properties():
    resp = requests.get("https://api.doorloop.com/properties", headers={"Authorization": f"Bearer {os.getenv('DOORLOOP_API_KEY')}"})
    raw_properties = resp.json().get('data', [])

    transformed = [transform_property(p) for p in raw_properties]
    for prop in transformed:
        supabase.table("doorloop_normalized_properties").upsert(prop).execute()

if __name__ == "__main__":
    normalize_properties()
