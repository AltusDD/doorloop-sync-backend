import os
import sys
import logging
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
    return SupabaseClient()

# --- Logger Configuration ---

def get_logger(name: str) -> logging.Logger:
    """
    Creates and configures a logger instance.

    Args:
        name: The name for the logger, typically __name__ of the calling module.

    Returns:
        A configured logger instance.
    """
    # FIX: Add the missing get_logger function.
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if the logger is already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

