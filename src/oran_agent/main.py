import json
from datetime import datetime, timezone

from oran_agent.collectors.core import collect_core_status
from oran_agent.collectors.ue import collect_ue_status
from oran_agent.collectors.traffic import collect_uplink_ping, collect_downlink_ping


def collect_system_state() -> dict:
    core = collect_core_status()
    ue = collect_ue_status()

    state = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "core": core,
        "ue": ue,
        "traffic": {}
    }

    if ue["ok"] and ue["ue_ip"]:
        state["traffic"]["uplink_ping"] = collect_uplink_ping()
        state["traffic"]["downlink_ping"] = collect_downlink_ping(ue["ue_ip"])
    else:
        state["traffic"]["skipped"] = "UE IP not available"

    state["overall_health"] = (
        core.get("ok") is True
        and ue.get("ok") is True
        and state["traffic"].get("uplink_ping", {}).get("ok") is True
        and state["traffic"].get("downlink_ping", {}).get("ok") is True
    )

    return state


def main() -> None:
    state = collect_system_state()
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()