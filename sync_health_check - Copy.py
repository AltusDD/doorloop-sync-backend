import sys
import re
from rich.console import Console

def check_log_health(log_file_path: str) -> bool:
    """
    Parses a log file to check for critical errors.

    Args:
        log_file_path: The path to the sync_output.log file.

    Returns:
        True if the log is healthy (no critical errors), False otherwise.
    """
    console = Console()
    
    # Define patterns that indicate a definite failure
    error_patterns = [
        re.compile(r"❌", re.IGNORECASE),
        re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE),
        re.compile(r"CRITICAL", re.IGNORECASE),
        re.compile(r"ERROR", re.IGNORECASE),
    ]

    found_errors = []

    try:
        with open(log_file_path, 'r') as f:
            for i, line in enumerate(f, 1):
                for pattern in error_patterns:
                    if pattern.search(line):
                        # We found a line that indicates a real error
                        error_context = {
                            "line_number": i,
                            "line_content": line.strip()
                        }
                        found_errors.append(error_context)
                        break # Move to the next line once an error is found

    except FileNotFoundError:
        console.print(f"[bold red]❌ Health Check Error: Log file not found at '{log_file_path}'[/bold red]")
        return False

    if found_errors:
        console.print("[bold red]❌ Sync Pipeline Health Check FAILED. Found critical errors:[/bold red]")
        for error in found_errors:
            console.print(f"  [yellow]L{error['line_number']}:[/yellow] {error['line_content']}")
        return False
    else:
        console.print("[bold green]✅ Sync Pipeline Health Check PASSED. No critical errors found.[/bold green]")
        return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sync_health_check.py <path_to_log_file>")
        sys.exit(1)
    
    log_file = sys.argv[1]
    is_healthy = check_log_health(log_file)
    
    # Exit with a non-zero status code if errors were found, which will fail the CI step
    if not is_healthy:
        sys.exit(1)

