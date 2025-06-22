import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(">>> syncTenants triggered")
    try:
        # Placeholder for real logic
        return func.HttpResponse("OK", status_code=200)
    except Exception as e:
        logging.error(f"syncTenants failed: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
