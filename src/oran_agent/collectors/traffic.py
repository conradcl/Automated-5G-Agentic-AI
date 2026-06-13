import re
from typing import Any, Dict

from oran_agent.utils import run_command


EXT_DN_IP = "192.168.70.135"


def parse_ping_output(output: str) -> Dict[str, Any]:
    packet_loss = None
    avg_rtt_ms = None

    loss_match = re.search(r"(\d+(?:\.\d+)?)% packet loss", output)
    if loss_match:
        packet_loss = float(loss_match.group(1))

    rtt_match = re.search(r"rtt min/avg/max/mdev = [\d.]+/([\d.]+)/", output)
    if rtt_match:
        avg_rtt_ms = float(rtt_match.group(1))

    return {
        "packet_loss_percent": packet_loss,
        "avg_rtt_ms": avg_rtt_ms,
        "ok": packet_loss == 0.0
    }


def collect_uplink_ping() -> Dict[str, Any]:
    result = run_command([
        "ping",
        "-c",
        "5",
        EXT_DN_IP,
        "-I",
        "oaitun_ue1"
    ], timeout=20)

    parsed = parse_ping_output(result["stdout"])

    return {
        "ok": result["ok"] and parsed["ok"],
        "direction": "ue_to_ext_dn",
        "parsed": parsed,
        "raw_output": result["stdout"],
        "error": result["stderr"]
    }


def collect_downlink_ping(ue_ip: str) -> Dict[str, Any]:
    result = run_command([
        "docker",
        "exec",
        "oai-ext-dn",
        "ping",
        "-c",
        "5",
        ue_ip
    ], timeout=20)

    parsed = parse_ping_output(result["stdout"])

    return {
        "ok": result["ok"] and parsed["ok"],
        "direction": "ext_dn_to_ue",
        "parsed": parsed,
        "raw_output": result["stdout"],
        "error": result["stderr"]
    }