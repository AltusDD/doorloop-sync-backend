# api_deals.py
# 🔌 Routes for Deal Room operations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from supabase import create_client
import os

router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

class Deal(BaseModel):
    name: str
    status: str | None = "draft"

@router.get("/api/deals")
def list_deals():
    response = supabase.table("deals").select("*").execute()
    return response.data

@router.post("/api/deals")
def create_deal(deal: Deal):
    response = supabase.table("deals").insert(deal.dict()).execute()
    if not response.data:
        raise HTTPException(status_code=400, detail="Failed to create deal")
    return response.data[0]

@router.get("/api/deals/{deal_id}")
def get_deal(deal_id: int):
    response = supabase.table("deals").select("*").eq("id", deal_id).single().execute()
    if not response.data:
        raise HTTPException(status_code=404, detail="Deal not found")
    return response.data
