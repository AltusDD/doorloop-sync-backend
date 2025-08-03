
import re
import sys
from collections import defaultdict
from rich.console import Console
from rich.table import Table

def classify_error(error_type, error_message):
    error_message = str(error_message).lower()
    if "decimal" in error_message and "json serializable" in error_message:
        return "Decimal → JSON failure"
    if "invalid input syntax for type bigint" in error_message:
        return f"Schema mismatch (bigint)"
    if "non-json response" in error_message:
        return "DoorLoop API returned non-JSON"
    if "has no attribute 'fetch_all'" in error_message:
        return "Supabase client method error"
    if error_type == "KeyError":
        return f"KeyError: {error_message} missing"
    if error_type == "TypeError" and "'nonetype' is not subscriptable" in error_message:
        return "Nested field access on None"
    return f"{error_type}"

def parse_logs(log_content):
    traceback_pattern = re.compile(
        r"Traceback \(most recent call last\):\n"
        r".*?File \".*?/(normalize_\w+)\.py\".*?\n"
        r".*?"
        r"^([a-zA-Z_]\w*Error): (.*)",
        re.S | re.M
    )

    failures = {}
    matches = traceback_pattern.finditer(log_content)

    for match in matches:
        task_name = match.group(1)
        error_type = match.group(2)
        error_message = match.group(3).strip()

        if task_name not in failures:
            failures[task_name] = classify_error(error_type, error_message)

    return failures

def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <path_to_orchestrator_log_file>")
        sys.exit(1)

    log_file_path = sys.argv[1]

    try:
        with open(log_file_path, 'r') as f:
            log_content = f.read()
    except FileNotFoundError:
        print(f"Error: Log file not found at '{log_file_path}'")
        sys.exit(1)

    failed_tasks = parse_logs(log_content)

    if not failed_tasks:
        print("✅ No failed normalization tasks found in the logs.")
        return

    table = Table(title="ETL Failure Audit Summary", show_header=True, header_style="bold magenta")
    table.add_column("Failed Normalization Task", style="cyan", no_wrap=True)
    table.add_column("Error Type / Root Cause", style="red")

    for task, cause in sorted(failed_tasks.items()):
        table.add_row(task, cause)

    console = Console()
    console.print(table)

if __name__ == "__main__":
    main()
