"""Environment-driven configuration shared by the lab R1 services.

The MVP runs the DME broker, telemetry producer adapter, and rApp consumer as
separate processes.  Bind addresses control where a process listens; public
base URLs are the addresses advertised to the other processes.  Keeping those
separate also makes the same code usable in containers later.
"""
from __future__ import annotations

import os
from pathlib import Path


_RAPP_DIRECTORY = Path(__file__).resolve().parent


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def _env_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


# R1/DME-lite broker
DME_BIND_HOST = os.environ.get("DME_BIND_HOST", "127.0.0.1")
DME_PORT = _env_int("DME_PORT", 9990)
DME_BASE_URL = os.environ.get("DME_BASE_URL", f"http://127.0.0.1:{DME_PORT}")
EVIDENCE_API_BASE_URL = os.environ.get(
    "EVIDENCE_API_BASE_URL", "http://127.0.0.1:9991"
)

INFO_TYPE_ID = os.environ.get("INFO_TYPE_ID", "oran-health-monitor-kpm-v1")
HTTP_TIMEOUT_S = _env_float("HTTP_TIMEOUT_S", 5.0)

# The deterministic evaluator covers this monitoring path, not whole-testbed
# or subscriber-service health.
ASSESSMENT_SCOPE = "ric_e2_kpm_telemetry_monitoring_path"
ASSESSMENT_SCOPE_LABEL = "RIC/E2 KPM telemetry monitoring path"

# Read-only health rApp consumer
JOB_ID = os.environ.get("JOB_ID", "health-agent-rapp-job-1")
JOB_OWNER = os.environ.get("JOB_OWNER", "health-agent-rapp")
CONSUMER_BIND_HOST = os.environ.get("CONSUMER_BIND_HOST", "127.0.0.1")
CONSUMER_PORT = _env_int("CONSUMER_PORT", 9992)
CONSUMER_BASE_URL = os.environ.get(
    "CONSUMER_BASE_URL", f"http://127.0.0.1:{CONSUMER_PORT}"
)
CONSUMER_CALLBACK_PATH = os.environ.get(
    "CONSUMER_CALLBACK_PATH", "/consumer/health-data"
)
CONSUMER_MAX_BODY_BYTES = _env_int("CONSUMER_MAX_BODY_BYTES", 262144)

# Source freshness is based on the xApp's KPM observation timestamp, never on
# the time at which an adapter happens to deliver or redeliver the message.
STALE_AFTER_S = _env_float("STALE_AFTER_S", 30.0)
MAX_FUTURE_SKEW_S = _env_float("MAX_FUTURE_SKEW_S", 5.0)

# The Health xApp requests these metrics. Override with a comma-separated list
# if the lab E2 node exposes a different KPM measurement set.
REQUIRED_KPM_METRICS = _env_csv(
    "REQUIRED_KPM_METRICS",
    (
        "RRU.PrbTotDl",
        "RRU.PrbTotUl",
        "DRB.UEThpDl",
        "DRB.UEThpUl",
        "DRB.RlcSduDelayDl",
    ),
)

# Optional DeepSeek explanation layer. The model receives structured evidence,
# never the deterministic HealthReport produced by health_checks.py.
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_TEMPERATURE = _env_float("DEEPSEEK_TEMPERATURE", 0.1)
DEEPSEEK_TIMEOUT_S = _env_float("DEEPSEEK_TIMEOUT_S", 20.0)
DEEPSEEK_MAX_TOKENS = _env_int("DEEPSEEK_MAX_TOKENS", 1200)
DEEPSEEK_TRUNCATION_RETRY_MAX_TOKENS = _env_int(
    "DEEPSEEK_TRUNCATION_RETRY_MAX_TOKENS", 2400
)
DEEPSEEK_MAX_QUESTION_CHARS = _env_int("DEEPSEEK_MAX_QUESTION_CHARS", 2000)
DEEPSEEK_MAX_INPUT_CHARS = _env_int("DEEPSEEK_MAX_INPUT_CHARS", 50000)

# Model-selected, read-only evidence tools. Every target and command argument is
# supplied by operator configuration; the model can select only a tool name.
RAPP_READ_TOOLS_ENABLED = _env_bool("RAPP_READ_TOOLS_ENABLED", True)
RAPP_READ_TOOL_MAX_CALLS = _env_int("RAPP_READ_TOOL_MAX_CALLS", 4)
RAPP_READ_TOOL_PLANNER_MAX_TOKENS = _env_int(
    "RAPP_READ_TOOL_PLANNER_MAX_TOKENS", 500
)
RAPP_READ_TOOL_HTTP_TIMEOUT_S = _env_float(
    "RAPP_READ_TOOL_HTTP_TIMEOUT_S", 3.0
)
RAPP_READ_TOOL_HTTP_MAX_BODY_BYTES = _env_int(
    "RAPP_READ_TOOL_HTTP_MAX_BODY_BYTES", 131072
)
RAPP_READ_TOOL_MAX_RESULT_CHARS = _env_int(
    "RAPP_READ_TOOL_MAX_RESULT_CHARS", 30000
)
RAPP_READ_TOOL_HISTORY_LIMIT = _env_int("RAPP_READ_TOOL_HISTORY_LIMIT", 20)
RAPP_READ_TOOL_WINDOW_LIMIT = _env_int("RAPP_READ_TOOL_WINDOW_LIMIT", 3)
RAPP_READ_TOOL_WINDOW_MAX_SAMPLES = _env_int(
    "RAPP_READ_TOOL_WINDOW_MAX_SAMPLES", 2000
)
RAPP_READ_TOOL_WINDOW_MAX_BYTES = _env_int(
    "RAPP_READ_TOOL_WINDOW_MAX_BYTES", 4_000_000
)
RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S = _env_float(
    "RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S", 60.0
)
RAPP_READ_TOOL_SEQUENCE_MAX_SAMPLES = _env_int(
    "RAPP_READ_TOOL_SEQUENCE_MAX_SAMPLES", 600
)
RAPP_READ_TOOL_PING_TARGET = os.environ.get(
    "RAPP_READ_TOOL_PING_TARGET", "192.168.70.135"
)
RAPP_READ_TOOL_PING_INTERFACE = os.environ.get(
    "RAPP_READ_TOOL_PING_INTERFACE", "oaitun_ue1"
)
RAPP_READ_TOOL_PING_COUNT = _env_int("RAPP_READ_TOOL_PING_COUNT", 3)
RAPP_READ_TOOL_PING_REPLY_TIMEOUT_S = _env_int(
    "RAPP_READ_TOOL_PING_REPLY_TIMEOUT_S", 1
)
RAPP_READ_TOOL_COMMAND_TIMEOUT_S = _env_float(
    "RAPP_READ_TOOL_COMMAND_TIMEOUT_S", 8.0
)
RAPP_READ_TOOL_OAI_CONTAINERS = _env_csv(
    "RAPP_READ_TOOL_OAI_CONTAINERS",
    (
        "oai-nrf",
        "oai-amf",
        "oai-smf",
        "oai-upf",
        "oai-ext-dn",
        "mysql",
    ),
)

# Background incident automation. The model is invoked on this fixed cadence
# for advisory diagnosis, but deterministic health checks and policies retain
# exclusive authority over remediation.
RAPP_AUTOMATION_ENABLED = _env_bool("RAPP_AUTOMATION_ENABLED", True)
RAPP_AUTOMATION_POLL_S = _env_float("RAPP_AUTOMATION_POLL_S", 5.0)
RAPP_AUTOMATION_LLM_INTERVAL_S = _env_float(
    "RAPP_AUTOMATION_LLM_INTERVAL_S", 60.0
)
RAPP_AUTOMATION_FAILURES_REQUIRED = _env_int(
    "RAPP_AUTOMATION_FAILURES_REQUIRED", 3
)
RAPP_AUTOMATION_STARTUP_GRACE_S = _env_float(
    "RAPP_AUTOMATION_STARTUP_GRACE_S", 30.0
)
RAPP_AUTOMATION_VERIFY_TIMEOUT_S = _env_float(
    "RAPP_AUTOMATION_VERIFY_TIMEOUT_S", 45.0
)
RAPP_AUTOMATION_COOLDOWN_S = _env_float(
    "RAPP_AUTOMATION_COOLDOWN_S", 120.0
)
RAPP_AUTOMATION_THREAD_ID = os.environ.get(
    "RAPP_AUTOMATION_THREAD_ID", "health-agent-automation"
)
RAPP_AUTOMATION_AUDIT_DB_PATH = os.environ.get(
    "RAPP_AUTOMATION_AUDIT_DB_PATH",
    str(_RAPP_DIRECTORY / "data" / "rapp_memory.sqlite3"),
)
# Optional second-stage xApp recovery. Supported backends are disabled, docker,
# and systemd. Targets are operator-owned and validated before any process runs.
RAPP_AUTOMATION_XAPP_RESTART_BACKEND = os.environ.get(
    "RAPP_AUTOMATION_XAPP_RESTART_BACKEND", "disabled"
).strip().lower()
RAPP_AUTOMATION_XAPP_RESTART_TARGET = os.environ.get(
    "RAPP_AUTOMATION_XAPP_RESTART_TARGET", ""
).strip()

# Durable rApp memory. Structured telemetry samples and advisory conversation
# context are stored locally; no terminal logs or free-form text files are used.
RAPP_MEMORY_ENABLED = _env_bool("RAPP_MEMORY_ENABLED", True)
RAPP_MEMORY_DB_PATH = os.environ.get(
    "RAPP_MEMORY_DB_PATH",
    str(_RAPP_DIRECTORY / "data" / "rapp_memory.sqlite3"),
)
RAPP_DEFAULT_THREAD_ID = os.environ.get(
    "RAPP_DEFAULT_THREAD_ID", "health-agent-cli"
)
RAPP_MAX_THREAD_ID_CHARS = _env_int("RAPP_MAX_THREAD_ID_CHARS", 128)
RAPP_CONVERSATION_CONTEXT_TURNS = _env_int(
    "RAPP_CONVERSATION_CONTEXT_TURNS", 8
)
RAPP_CONVERSATION_RETAINED_TURNS = _env_int(
    "RAPP_CONVERSATION_RETAINED_TURNS", 100
)
RAPP_MEMORY_MAX_CONTEXT_CHARS = _env_int(
    "RAPP_MEMORY_MAX_CONTEXT_CHARS", 30000
)
RAPP_MEMORY_MAX_ADVISORY_CHARS = _env_int(
    "RAPP_MEMORY_MAX_ADVISORY_CHARS", 4000
)

# The Health xApp normally produces one structured observation per second. A
# window is time-based rather than count-based, because delayed/missing network
# deliveries mean a 60-second interval is not guaranteed to contain 60 samples.
TELEMETRY_MEMORY_WINDOW_S = _env_float("TELEMETRY_MEMORY_WINDOW_S", 60.0)
TELEMETRY_MEMORY_WINDOW_CLOSE_GRACE_S = _env_float(
    "TELEMETRY_MEMORY_WINDOW_CLOSE_GRACE_S", 1.0
)
TELEMETRY_MEMORY_POLL_S = _env_float("TELEMETRY_MEMORY_POLL_S", 1.0)
TELEMETRY_MEMORY_INGEST_QUEUE_SIZE = _env_int(
    "TELEMETRY_MEMORY_INGEST_QUEUE_SIZE", 1000
)
TELEMETRY_MEMORY_CONTEXT_WINDOWS = _env_int(
    "TELEMETRY_MEMORY_CONTEXT_WINDOWS", 1
)
TELEMETRY_MEMORY_MAX_SAMPLES_PER_PAYLOAD = _env_int(
    "TELEMETRY_MEMORY_MAX_SAMPLES_PER_PAYLOAD", 120
)
TELEMETRY_MEMORY_MAX_PAYLOAD_CHARS = _env_int(
    "TELEMETRY_MEMORY_MAX_PAYLOAD_CHARS", 30000
)
TELEMETRY_MEMORY_RAW_RETENTION_HOURS = _env_float(
    "TELEMETRY_MEMORY_RAW_RETENTION_HOURS", 24.0
)
TELEMETRY_MEMORY_RETAINED_WINDOWS = _env_int(
    "TELEMETRY_MEMORY_RETAINED_WINDOWS", 1440
)
TELEMETRY_MEMORY_SUMMARY_MAX_TOKENS = _env_int(
    "TELEMETRY_MEMORY_SUMMARY_MAX_TOKENS", 800
)
TELEMETRY_MEMORY_MAX_DIGEST_CHARS = _env_int(
    "TELEMETRY_MEMORY_MAX_DIGEST_CHARS", 8000
)
