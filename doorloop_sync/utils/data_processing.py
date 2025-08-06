
def clean_record(record: dict) -> dict:
    for k, v in record.items():
        if isinstance(v, str) and v.strip() == "":
            record[k] = None
    return record
