def transform_payment(record):
    try:
        return {
            "id": record.get("id"),
            "doorloopId": record.get("id"),
            "leaseId": record.get("leaseId"),
            "amountPaid": float(record.get("amountPaid", 0)),
            "paymentDate": record.get("paymentDate"),
            "paymentMethod": record.get("paymentMethod"),
            "status": record.get("status")
        }
    except Exception as e:
        print(f"Error transforming payment: {e}")
        return None