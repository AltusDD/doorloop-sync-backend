def extract_id_from_jsonb(jsonb):
    if isinstance(jsonb, dict):
        return jsonb.get("id")
    return None
