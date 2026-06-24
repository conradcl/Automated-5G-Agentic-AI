from typing import Any, Dict, List


def add_finding(
    findings: List[Dict[str, Any]],
    component: str,
    status: str,
    severity: str,
    evidence: str,
    suggested_action: str
) -> None:
    findings.append({
        "component": component,
        "status": status,
        "severity": severity,
        "evidence": evidence,
        "suggested_action": suggested_action
    })


def build_diagnostics(state: Dict[str, Any]) -> Dict[str, Any]:
    findings = []

    core = state.get("core", {})
    ue = state.get("ue", {})
    traffic = state.get("traffic", {})

    # Core diagnosis
    if core.get("ok") is True:
        add_finding(
            findings,
            "OAI Core",
            "healthy",
            "info",
            "All required OAI core containers are running and healthy.",
            "No core action needed."
        )
    else:
        missing = core.get("missing", [])
        unhealthy = core.get("unhealthy", [])

        if missing:
            add_finding(
                findings,
                "OAI Core",
                "missing containers",
                "critical",
                "Missing required containers: " + ", ".join(missing),
                "Start the OAI core with: cd ~/oai-cn5g && docker compose up -d"
            )

        if unhealthy:
            add_finding(
                findings,
                "OAI Core",
                "unhealthy containers",
                "critical",
                "Unhealthy containers: " + ", ".join(unhealthy),
                "Inspect container logs with: docker logs <container-name> --tail 100"
            )

        if not missing and not unhealthy:
            add_finding(
                findings,
                "OAI Core",
                "unknown issue",
                "warning",
                "Core status returned false but no specific missing/unhealthy container was identified.",
                "Run docker ps and inspect the OAI core container status manually."
            )

    # UE diagnosis
    if ue.get("ok") is True:
        add_finding(
            findings,
            "UE",
            "connected",
            "info",
            "UE tunnel exists with IP address: " + str(ue.get("ue_ip")),
            "No UE connection action needed."
        )
    else:
        if ue.get("interface_exists") is False:
            add_finding(
                findings,
                "UE",
                "not connected",
                "critical",
                "The oaitun_ue1 interface does not exist.",
                "Start the gNB and nrUE, then verify with: ip addr show oaitun_ue1"
            )
        else:
            add_finding(
                findings,
                "UE",
                "missing IP",
                "critical",
                "The UE interface exists but no UE IP address was found.",
                "Check UE registration and PDU session setup."
            )

    # Traffic diagnosis
    if "skipped" in traffic:
        add_finding(
            findings,
            "Traffic",
            "not tested",
            "warning",
            traffic.get("skipped", "Traffic tests were skipped."),
            "Fix UE connection first, then rerun uplink and downlink ping tests."
        )
    else:
        uplink = traffic.get("uplink_ping", {})
        downlink = traffic.get("downlink_ping", {})

        if uplink.get("ok") is True:
            add_finding(
                findings,
                "Uplink Traffic",
                "working",
                "info",
                "Uplink ping succeeded with "
                + str(uplink.get("parsed", {}).get("packet_loss_percent"))
                + "% packet loss.",
                "No uplink action needed."
            )
        else:
            add_finding(
                findings,
                "Uplink Traffic",
                "failed",
                "critical",
                "Uplink ping failed or had packet loss.",
                "Check UE tunnel, UPF, and external data network reachability."
            )

        if downlink.get("ok") is True:
            add_finding(
                findings,
                "Downlink Traffic",
                "working",
                "info",
                "Downlink ping succeeded with "
                + str(downlink.get("parsed", {}).get("packet_loss_percent"))
                + "% packet loss.",
                "No downlink action needed."
            )
        else:
            add_finding(
                findings,
                "Downlink Traffic",
                "failed",
                "critical",
                "Downlink ping failed or had packet loss.",
                "Check oai-ext-dn, UPF routing, and UE reachability."
            )

    critical_count = sum(1 for finding in findings if finding["severity"] == "critical")
    warning_count = sum(1 for finding in findings if finding["severity"] == "warning")

    if state.get("overall_health") is True:
        status_label = "healthy"
    elif critical_count > 0:
        status_label = "unhealthy"
    elif warning_count > 0:
        status_label = "degraded"
    else:
        status_label = "unknown"

    return {
        "status_label": status_label,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "findings": findings
    }