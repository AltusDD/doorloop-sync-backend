def flatten_json_keys(data):
    """
    Ensure all JSON records have the same keys by adding missing ones as None.
    This prevents PGRST102 errors on Supabase upsert.
    """
    if not data:
        return data
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())
    return [{key: item.get(key, None) for key in all_keys} for item in data]