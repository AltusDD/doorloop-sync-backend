# main.py
# 🚀 FastAPI App to Serve Empire Command Center API Routes

from fastapi import FastAPI
from routes.api_occupancy_dashboard import router as kpi_router
from routes.api_deals import router as deals_router

app = FastAPI(title="Altus Empire Command Center API")

# Mount KPI route
app.include_router(kpi_router)
# Mount Deal routes
app.include_router(deals_router)

@app.get("/")
def health():
    return {"status": "Empire API online"}
