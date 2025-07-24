def transform_lease(record):
    try:
        return {
            "id": record.get("id"),
            "doorloopId": record.get("id"),
            "propertyId": record.get("propertyId"),
            "unitId": record.get("unitId"),
            "tenantId": record.get("tenantId"),
            "startDate": record.get("startDate"),
            "endDate": record.get("endDate"),
            "rentAmount": float(record.get("rentAmount", 0)),
            "status": record.get("status")
        }
    except Exception as e:
        print(f"Error transforming lease: {e}")
        return None