import json
from datetime import datetime, timezone

from oran_agent.collectors.core import collect_core_status
from oran_agent.collectors.ue import collect_ue_status
from oran_agent.collectors.traffic import collect_uplink_ping, collect_downlink_ping
from oran_agent.collectors.kpm import collect_kpm_metrics
from oran_agent.llm.deepseek_client import ask_deepseek_about_state
from oran_agent.diagnostics import build_diagnostics


def collect_system_state() -> dict:
    core = collect_core_status()
    ue = collect_ue_status()
    kpm = collect_kpm_metrics()

    state = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "core": core,
        "ue": ue,
        "traffic": {},
        "kpm": kpm
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

    state["diagnostics"] = build_diagnostics(state)

    return state


def main() -> None:
    state = collect_system_state()

    print("=== Raw System State JSON ===")
    print(json.dumps(state, indent=2))

    answer = ask_deepseek_about_state(
        state,
        "Is the 5G/O-RAN testbed healthy right now?"
    )

    print("\n=== DeepSeek Answer ===")
    print(answer)


if __name__ == "__main__":
    main()