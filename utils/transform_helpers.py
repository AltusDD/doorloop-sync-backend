
import hashlib
import json
import re


def hash_dict(d: dict) -> str:
    """Hashes a dictionary deterministically by sorting keys and encoding it as a string."""
    dict_str = json.dumps(d, sort_keys=True)
    return hashlib.sha256(dict_str.encode("utf-8")).hexdigest()


def clean_dict(d: dict) -> dict:
    """Removes keys with None or empty values and strips strings."""
    return {
        k: v.strip() if isinstance(v, str) else v
        for k, v in d.items()
        if v is not None and v != ""
    }


def normalize_email(email: str) -> str:
    """Returns a lowercased and trimmed email, or empty string if None."""
    return email.strip().lower() if email else ""


def extract_digits(value: str) -> str:
    """Returns only the numeric characters from a string."""
    return re.sub(r"\D", "", value or "")


def normalize_phone(phone: str) -> str:
    """Normalizes a phone number to just digits (no formatting)."""
    return extract_digits(phone)
