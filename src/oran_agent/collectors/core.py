from typing import Any, Dict

from oran_agent.utils import run_command


IMPORTANT_CORE_CONTAINERS = [
    "oai-nrf",
    "oai-amf",
    "oai-smf",
    "oai-upf",
    "oai-ext-dn",
    "mysql"
]


def collect_core_status() -> Dict[str, Any]:
    result = run_command([
        "docker",
        "ps",
        "--format",
        "{{.Names}}|{{.Status}}"
    ])

    if not result["ok"]:
        return {
            "ok": False,
            "error": result["stderr"],
            "containers": {}
        }

    containers = {}

    for line in result["stdout"].splitlines():
        if "|" not in line:
            continue

        name, status = line.split("|", 1)

        containers[name] = {
            "status": status,
            "running": status.lower().startswith("up"),
            "healthy": "unhealthy" not in status.lower() and status.lower().startswith("up")
        }

    missing = [
        name for name in IMPORTANT_CORE_CONTAINERS
        if name not in containers
    ]

    unhealthy = [
        name for name in IMPORTANT_CORE_CONTAINERS
        if name in containers and not containers[name]["healthy"]
    ]

    return {
        "ok": len(missing) == 0 and len(unhealthy) == 0,
        "missing": missing,
        "unhealthy": unhealthy,
        "containers": containers
    }