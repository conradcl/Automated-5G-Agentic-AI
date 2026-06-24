import re
from pathlib import Path
from typing import Any, Dict


DEFAULT_KPM_LOG_PATH = Path("logs/kpm_latest.log")


def parse_kpm_output(text: str) -> Dict[str, Any]:
    """
    Parses KPIMON/xApp output and extracts metric lines like:

    DRB.UEThpDl = 13370.83 [kbps]
    RRU.PrbTotDl = 19091 [PRBs]
    DRB.RlcSduDelayDl = 371.27 [us]

    If a metric appears multiple times, this keeps the latest value.
    """

    metrics = {}

    metric_pattern = re.compile(
        r"(?P<name>[A-Za-z0-9_.]+)\s*=\s*"
        r"(?P<value>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)"
        r"(?:\s*\[(?P<unit>[^\]]+)\])?"
    )

    for line in text.splitlines():
        match = metric_pattern.search(line)

        if not match:
            continue

        name = match.group("name")
        value_text = match.group("value")
        unit = match.group("unit")

        try:
            value = float(value_text)
        except ValueError:
            continue

        metrics[name] = {
            "value": value,
            "unit": unit
        }

    return metrics


def collect_kpm_metrics(log_path: Path = DEFAULT_KPM_LOG_PATH) -> Dict[str, Any]:
    """
    Reads the latest saved KPIMON log and extracts KPM metrics.
    """

    if not log_path.exists():
        return {
            "ok": False,
            "source": str(log_path),
            "error": "KPM log file does not exist. Run KPIMON and save output first.",
            "metrics": {}
        }

    text = log_path.read_text(errors="ignore")
    metrics = parse_kpm_output(text)

    return {
        "ok": len(metrics) > 0,
        "source": str(log_path),
        "metric_count": len(metrics),
        "metrics": metrics
    }