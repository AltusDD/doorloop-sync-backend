
import hashlib
import json
import re

def hash_dict(d: dict, exclude_keys: list[str] = None) -> str:
    """Creates a consistent hash from a dictionary, excluding specified keys."""
    exclude_keys = exclude_keys or []
    clean_dict = {k: v for k, v in d.items() if k not in exclude_keys}
    serialized = json.dumps(clean_dict, sort_keys=True)
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

def clean_dict(d: dict) -> dict:
    """Recursively clean a dictionary: strips strings and removes blank values."""
    def clean(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            return clean_dict(value)
        if isinstance(value, list):
            return [clean(v) for v in value if v is not None]
        return value

    return {k: clean(v) for k, v in d.items() if v not in [None, "", [], {}]}
