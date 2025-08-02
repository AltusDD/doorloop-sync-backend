import os
import logging
from .clients.doorloop_client import DoorLoopClient
# --- CORRECTED IMPORT ---
# The import now uses the correct, standardized class name 'SupabaseClient'.
from .clients.supabase_client import SupabaseClient

# --- Environment Variables ---
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(name):
    return logging.getLogger(name)

# --- Client Initialization Functions ---
def get_doorloop_client():
    if not DOORLOOP_API_KEY or not DOORLOOP_API_BASE_URL:
        raise ValueError("DoorLoop API credentials are not configured.")
    return DoorLoopClient(DOORLOOP_API_BASE_URL, DOORLOOP_API_KEY)

def get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Supabase credentials are not configured.")
    # --- CORRECTED CLASS NAME ---
    # This now correctly instantiates the 'SupabaseClient' class.
    return SupabaseClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
