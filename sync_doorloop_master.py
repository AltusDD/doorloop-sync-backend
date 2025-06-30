import os
from doorloop_client import fetch_all_doorloop_data
from supabase_client import insert_raw_data
from dotenv import load_dotenv

load_dotenv()

def main():
    print("🚀 Starting master DoorLoop sync...")
    all_data = fetch_all_doorloop_data()
    insert_raw_data(all_data)
    print("✅ DoorLoop master sync complete.")

if __name__ == "__main__":
    main()
