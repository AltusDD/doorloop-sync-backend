from sync_utils import sync_and_store  # Assuming shared sync utility
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient

# Initialize clients
doorloop = DoorLoopClient()
supabase = SupabaseClient()

def run_missing_category_sync():
    sync_and_store(doorloop, supabase, "applications", "doorloop_raw_applications")
    sync_and_store(doorloop, supabase, "inspections", "doorloop_raw_inspections")
    sync_and_store(doorloop, supabase, "insurance-policies", "doorloop_raw_insurance_policies")
    sync_and_store(doorloop, supabase, "reports", "doorloop_raw_reports")
    sync_and_store(doorloop, supabase, "activity-logs", "doorloop_raw_activity_logs")
    sync_and_store(doorloop, supabase, "recurring-charges", "doorloop_raw_recurring_charges")
    sync_and_store(doorloop, supabase, "recurring-credits", "doorloop_raw_recurring_credits")

if __name__ == "__main__":
    run_missing_category_sync()
