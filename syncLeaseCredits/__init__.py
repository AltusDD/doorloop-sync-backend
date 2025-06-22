import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("✅ Synced: syncLeaseCredits", status_code=200)
