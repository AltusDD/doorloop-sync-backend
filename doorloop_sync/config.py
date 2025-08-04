import os
from dotenv import load_dotenv
from doorloop_sync.clients import DoorLoopClient, SupabaseClient

# Load environment variables from a .env file if it exists
load_dotenv()

# --- Client Factory Functions ---

def get_doorloop_client():
    """
    Factory function to create and return an instance of the DoorLoopClient.
    The client self-configures from environment variables.
    """
    return DoorLoopClient()

def get_supabase_client():
    """
    Factory function to create and return an instance of the SupabaseClient.
    The client self-configures from environment variables.
    """
    # FIX: Call the constructor without arguments to match the updated class definition.
    return SupabaseClient()

