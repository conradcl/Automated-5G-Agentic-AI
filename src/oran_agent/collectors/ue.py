import re
from typing import Any, Dict

from oran_agent.utils import run_command


def collect_ue_status() -> Dict[str, Any]:
    result = run_command([
        "ip",
        "addr",
        "show",
        "oaitun_ue1"
    ])

    if not result["ok"]:
        return {
            "ok": False,
            "interface_exists": False,
            "ue_ip": None,
            "error": result["stderr"]
        }

    match = re.search(r"inet\s+(\d+\.\d+\.\d+\.\d+)", result["stdout"])
    ue_ip = match.group(1) if match else None

    return {
        "ok": ue_ip is not None,
        "interface_exists": True,
        "ue_ip": ue_ip
    }