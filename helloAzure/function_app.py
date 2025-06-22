
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="helloAzure")
@app.route(route="helloAzure", auth_level=func.AuthLevel.ANONYMOUS)
def hello_azure(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("✅ Hello from Azure — your function deployed and works!", status_code=200)
