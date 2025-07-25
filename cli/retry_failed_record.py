
import sys

def retry_failed_record(entity_type, doorloop_id):
    # Placeholder retry mechanism
    print(f"Retry requested → Entity: {entity_type}, DoorLoop ID: {doorloop_id}")
    # Replace with actual queue or function trigger logic

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: retry_failed_record.py <entity_type> <doorloop_id>")
        sys.exit(1)
    retry_failed_record(sys.argv[1], sys.argv[2])
