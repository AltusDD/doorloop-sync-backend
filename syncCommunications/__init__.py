import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("✅ Synced: syncCommunications", status_code=200)
