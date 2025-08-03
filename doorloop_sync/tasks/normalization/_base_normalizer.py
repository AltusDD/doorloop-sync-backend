def flatten_record(record, parent_key="", sep="_"):
    """
    Recursively flattens nested dictionaries
    """
    items = []
    for k, v in record.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_record(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def safe_normalize(records):
    return [flatten_record(r) for r in records if isinstance(r, dict)]
# silent_update
