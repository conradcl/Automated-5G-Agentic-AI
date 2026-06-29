import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional


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


def run_kpm_xapp_once(
    timeout: int = 25,
    flexric_dir: Optional[Path] = None,
    log_path: Path = DEFAULT_KPM_LOG_PATH
) -> Dict[str, Any]:
    """
    Runs the existing FlexRIC KPIMON xApp once, saves the output,
    and parses any KPM metrics found.

    This assumes the nearRT-RIC, gNB, and UE are already running.
    """

    if flexric_dir is None:
        flexric_dir = Path.home() / "flexric"

    xapp_path = flexric_dir / "build/examples/xApp/c/monitor/xapp_kpm_moni"

    if not flexric_dir.exists():
        return {
            "ok": False,
            "error": "FlexRIC directory does not exist: " + str(flexric_dir),
            "metrics": {}
        }

    if not xapp_path.exists():
        return {
            "ok": False,
            "error": "KPIMON xApp executable does not exist: " + str(xapp_path),
            "metrics": {}
        }

    command = ["./build/examples/xApp/c/monitor/xapp_kpm_moni"]

    try:
        process = subprocess.Popen(
            command,
            cwd=str(flexric_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        timed_out = False

        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            timed_out = True

        combined_output = stdout

        if stderr:
            combined_output += "\n\n=== STDERR ===\n"
            combined_output += stderr

        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(combined_output)

        metrics = parse_kpm_output(combined_output)

        return {
            "ok": len(metrics) > 0,
            "source": str(log_path),
            "command": " ".join(command),
            "timed_out": timed_out,
            "return_code": process.returncode,
            "metric_count": len(metrics),
            "metrics": metrics,
            "error": "" if len(metrics) > 0 else stderr.strip()
        }

    except OSError as error:
        return {
            "ok": False,
            "source": str(log_path),
            "command": " ".join(command),
            "error": str(error),
            "metrics": {}
        }