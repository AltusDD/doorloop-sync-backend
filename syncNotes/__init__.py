import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("✅ Synced: syncNotes", status_code=200)
