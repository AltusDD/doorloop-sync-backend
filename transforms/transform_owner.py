def transform_owner(record):
    try:
        return {
            "id": record.get("id"),
            "doorloopId": record.get("id"),
            "name": record.get("name"),
            "email": record.get("email"),
            "phone": record.get("phone"),
            "status": record.get("status")
        }
    except Exception as e:
        print(f"Error transforming owner: {e}")
        return None