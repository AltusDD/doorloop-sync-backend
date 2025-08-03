
import hashlib
import json
import logging

def hash_dict(record: dict) -> str:
    try:
        record_str = json.dumps(record, sort_keys=True)
        return hashlib.sha256(record_str.encode('utf-8')).hexdigest()
    except Exception as e:
        logging.error(f"Error hashing record: {e}")
        raise

def clean_dict(record: dict) -> dict:
    return {k: v for k, v in record.items() if v not in [None, '', [], {}]}
