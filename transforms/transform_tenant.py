def transform_tenant(record):
    try:
        return {
            "id": record.get("id"),
            "doorloopId": record.get("id"),
            "firstName": record.get("firstName"),
            "lastName": record.get("lastName"),
            "email": record.get("email"),
            "phone": record.get("phone"),
            "status": record.get("status")
        }
    except Exception as e:
        print(f"Error transforming tenant: {e}")
        return None