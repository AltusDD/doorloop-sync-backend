def standardize_records(records):
    """
    Ensures each record is a flat dictionary. Filters out any invalid or nested objects.
    """
    if not isinstance(records, list):
        return []

    cleaned = []
    for rec in records:
        if isinstance(rec, dict):
            cleaned.append(rec)
    return cleaned
