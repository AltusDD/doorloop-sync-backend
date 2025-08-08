import datetime

def standardize_record(item: dict) -> dict:
    """
    Recursively cleans a dictionary to ensure all values are compatible
    with Supabase's JSONB and other data types.
    """
    if not isinstance(item, dict):
        return item

    prepared_record = {}
    for key, value in item.items():
        if isinstance(value, dict):
            prepared_record[key] = standardize_record(value)
        elif isinstance(value, list):
            prepared_record[key] = [standardize_record(i) for i in value]
        elif isinstance(value, datetime.datetime):
            prepared_record[key] = value.isoformat()
        else:
            prepared_record[key] = value

    return prepared_record