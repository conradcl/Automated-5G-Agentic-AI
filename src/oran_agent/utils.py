import subprocess
from typing import Any, Dict, List


def run_command(command: List[str], timeout: int = 10) -> Dict[str, Any]:
    """
    Runs a shell command safely as a list of arguments.

    Returns a dictionary so every collector has the same output format.
    """
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        return {
            "ok": result.returncode == 0,
            "return_code": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "command": " ".join(command)
        }

    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "return_code": 124,
            "stdout": "",
            "stderr": f"Command timed out after {timeout} seconds",
            "command": " ".join(command)
        }