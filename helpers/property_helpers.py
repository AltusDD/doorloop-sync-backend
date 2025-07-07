
def flatten_property_record(record):
    address = record.get("address", {})
    return {
        "doorloop_id": record.get("id"),
        "name": record.get("name"),
        "property_type": record.get("type"),
        "class": record.get("class"),
        "status": record.get("status"),
        "address_street1": address.get("street1"),
        "address_street2": address.get("street2"),
        "address_city": address.get("city"),
        "address_state": address.get("state"),
        "address_zip": address.get("zipCode"),
        "address_country": address.get("country"),
        "address_lat": address.get("lat"),
        "address_lng": address.get("lng"),
        "address_is_valid": address.get("isValid"),
        "created_at": record.get("createdAt"),
        "updated_at": record.get("updatedAt"),
        "raw_property_json": record
    }

def extract_property_owners(record):
    owners = record.get("owners", [])
    result = []
    for owner in owners:
        result.append({
            "property_doorloop_id": record.get("id"),
            "owner_doorloop_id": owner.get("owner"),
            "ownership_percentage": owner.get("ownershipPercentage"),
            "is_primary": owner.get("isPrimary", False),
            "raw_link_json": owner
        })
    return result

def extract_property_pictures(record):
    pictures = record.get("pictures", [])
    result = []
    for idx, pic in enumerate(pictures):
        result.append({
            "property_doorloop_id": record.get("id"),
            "file_id": pic.get("file"),
            "rank": idx,
            "url": pic.get("url"),
            "raw_picture_json": pic
        })
    return result
