def _convert_date_to_iso(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    if isinstance(date_str, datetime):
        return date_str.isoformat(timespec='seconds')
    try:
        # Try parsing with milliseconds and 'Z' (UTC)
        if date_str.endswith('Z'):
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ') # Fallback for no milliseconds
        elif '+' in date_str:
            # Handle timezone offset with potential milliseconds
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z') # Fallback for no milliseconds
        elif 'T' in date_str:
            # Handle local time with potential milliseconds
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S') # Fallback for no milliseconds
        else:
            dt_obj = datetime.strptime(date_str, '%Y-%m-%d')

        return dt_obj.isoformat(timespec='seconds')
    except ValueError:
        _logger.warning(f"WARN: Could not parse date string '{date_str}'. Returning original string.")
        return date_str