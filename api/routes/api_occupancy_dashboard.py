# api_occupancy_dashboard.py
# 🔌 Serves real-time KPI summary to frontend

from fastapi import APIRouter
from supabase import create_client
import os

router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@router.get("/api/occupancy-dashboard")
def get_kpi_summary():
    response = supabase.table("kpi_summary").select("*").order("run_at", desc=True).limit(1).execute()
    if not response.data:
        return {"error": "No KPI data found."}
    return response.data[0]
