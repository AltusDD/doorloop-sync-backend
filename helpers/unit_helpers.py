
def extract_unit_fields(record):
    payload = record.get("data", {})
    return {
        "doorloop_id": payload.get("id"),
        "property_doorloop_id": payload.get("property"),
        "name": payload.get("name"),
        "status": payload.get("status"),
        "market_rent": payload.get("marketRent"),
        "bedrooms": payload.get("bedrooms"),
        "bathrooms": payload.get("bathrooms"),
        "square_feet": payload.get("squareFeet"),
        "data": payload,
    }
