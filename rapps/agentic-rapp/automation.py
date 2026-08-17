"""Policy-controlled, prompt-free incident diagnosis and remediation loop."""
from __future__ import annotations

import json
import math
import re
import sqlite3
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import config
import consumer
from health_checks import evaluate_health


AUTOMATIC_QUERY = (
    "This is the scheduled one-minute autonomous assessment. Diagnose the current "
    "RIC/E2 KPM telemetry monitoring path using the available bounded evidence. "
    "State the observed problem and likely affected layer. Do not claim that you "
    "executed, selected, or authorized remediation."
)
_SAFE_TARGET = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}$")
_DELIVERY_FAILURES = frozenset({"telemetry_received", "kpm_data_fresh"})
_XAPP_FAILURES = frozenset({
    "xapp_connected_to_ric",
    "kpm_indications_received",
    "kpm_data_fresh",
})


def validate_automation_configuration() -> None:
    """Fail fast on settings that could cause unsafe or tight-loop behavior."""
    numeric_ranges = (
        ("RAPP_AUTOMATION_POLL_S", config.RAPP_AUTOMATION_POLL_S, 0.1, 300.0),
        (
            "RAPP_AUTOMATION_LLM_INTERVAL_S",
            config.RAPP_AUTOMATION_LLM_INTERVAL_S,
            1.0,
            86400.0,
        ),
        (
            "RAPP_AUTOMATION_STARTUP_GRACE_S",
            config.RAPP_AUTOMATION_STARTUP_GRACE_S,
            0.0,
            3600.0,
        ),
        (
            "RAPP_AUTOMATION_VERIFY_TIMEOUT_S",
            config.RAPP_AUTOMATION_VERIFY_TIMEOUT_S,
            1.0,
            3600.0,
        ),
        (
            "RAPP_AUTOMATION_COOLDOWN_S",
            config.RAPP_AUTOMATION_COOLDOWN_S,
            0.0,
            86400.0,
        ),
    )
    for name, value, minimum, maximum in numeric_ranges:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not minimum <= value <= maximum
        ):
            raise ValueError(f"{name} must be between {minimum:g} and {maximum:g}")
    failures = config.RAPP_AUTOMATION_FAILURES_REQUIRED
    if (
        isinstance(failures, bool)
        or not isinstance(failures, int)
        or not 1 <= failures <= 100
    ):
        raise ValueError("RAPP_AUTOMATION_FAILURES_REQUIRED must be between 1 and 100")
    if config.RAPP_AUTOMATION_XAPP_RESTART_BACKEND not in {
        "disabled", "docker", "systemd"
    }:
        raise ValueError(
            "RAPP_AUTOMATION_XAPP_RESTART_BACKEND must be disabled, docker, or systemd"
        )
    if (
        config.RAPP_AUTOMATION_XAPP_RESTART_BACKEND != "disabled"
        and not _SAFE_TARGET.fullmatch(config.RAPP_AUTOMATION_XAPP_RESTART_TARGET)
    ):
        raise ValueError("RAPP_AUTOMATION_XAPP_RESTART_TARGET is invalid")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AutomationAudit:
    """Durable append-only incident event journal."""

    def __init__(self, database_path: str) -> None:
        path = Path(database_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.touch(mode=0o600, exist_ok=True)
        path.chmod(0o600)
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            str(path), timeout=5, check_same_thread=False
        )
        with self._lock:
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS automation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    details_json TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS automation_incident_idx "
                "ON automation_events(incident_id, id)"
            )
            self._connection.commit()

    def record(self, incident_id: str, event_type: str, details: dict[str, Any]) -> None:
        encoded = json.dumps(details, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock:
            self._connection.execute(
                "INSERT INTO automation_events "
                "(incident_id, created_at, event_type, details_json) VALUES (?, ?, ?, ?)",
                (incident_id, _now_iso(), event_type, encoded),
            )
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True


class Remediator:
    """Fixed actions whose parameters come only from operator configuration."""

    def reconcile_information_job(self) -> dict[str, Any]:
        consumer.register_consumer_job()
        return {"action": "reconcile_information_job", "ok": True}

    def restart_health_xapp(self) -> dict[str, Any]:
        backend = config.RAPP_AUTOMATION_XAPP_RESTART_BACKEND
        target = config.RAPP_AUTOMATION_XAPP_RESTART_TARGET
        if backend == "disabled":
            return {
                "action": "restart_health_xapp",
                "ok": False,
                "skipped": True,
                "reason": "xApp restart backend is disabled",
            }
        if not _SAFE_TARGET.fullmatch(target):
            return {
                "action": "restart_health_xapp",
                "ok": False,
                "skipped": True,
                "reason": "configured xApp restart target is invalid",
            }
        commands = {
            "docker": ["/usr/bin/docker", "restart", target],
            "systemd": ["/usr/bin/systemctl", "restart", target],
        }
        command = commands.get(backend)
        if command is None:
            return {
                "action": "restart_health_xapp",
                "ok": False,
                "skipped": True,
                "reason": "configured xApp restart backend is unsupported",
            }
        try:
            completed = subprocess.run(
                command,
                shell=False,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {
                "action": "restart_health_xapp",
                "ok": False,
                "error": type(exc).__name__,
            }
        return {
            "action": "restart_health_xapp",
            "ok": completed.returncode == 0,
            "returncode": completed.returncode,
        }


@dataclass
class ActiveIncident:
    incident_id: str
    signature: tuple[str, ...]
    opened_monotonic: float
    baseline_source_instance: Optional[str]
    baseline_sequence: Optional[int]
    remediation_plan: tuple[str, ...]
    next_action_index: int = 0
    verify_deadline: Optional[float] = None
    diagnosis_requested: bool = True
    diagnosis_completed: bool = False
    diagnosis_source: Optional[str] = None


class IncidentAutomationWorker:
    """Detect, diagnose, remediate, verify, and audit without a user prompt."""

    def __init__(
        self,
        diagnose: Callable[[str, Optional[str]], dict[str, Any]],
        *,
        audit: Optional[AutomationAudit] = None,
        remediator: Optional[Remediator] = None,
        notify: Callable[[str], None] = print,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        validate_automation_configuration()
        self._diagnose = diagnose
        self._audit = audit or AutomationAudit(config.RAPP_AUTOMATION_AUDIT_DB_PATH)
        self._remediator = remediator or Remediator()
        self._notify = notify
        self._monotonic = monotonic
        self._stop = threading.Event()
        self._diagnosis_wakeup = threading.Event()
        self._state_lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._diagnosis_thread: Optional[threading.Thread] = None
        self._started_at = monotonic()
        self._next_diagnosis_at = (
            self._started_at + config.RAPP_AUTOMATION_STARTUP_GRACE_S
        )
        self._failure_signature: tuple[str, ...] = ()
        self._failure_count = 0
        self._incident: Optional[ActiveIncident] = None
        self._cooldown_until = 0.0
        self._audit_error_reported = False

    def _notify_safely(self, message: str) -> None:
        try:
            self._notify(message)
        except Exception:
            pass

    def _record(
        self, incident_id: str, event_type: str, details: dict[str, Any]
    ) -> bool:
        try:
            self._audit.record(incident_id, event_type, details)
            return True
        except Exception as exc:
            if not self._audit_error_reported:
                self._audit_error_reported = True
                self._notify_safely(
                    "\n[AUTOMATION] Audit persistence failed; write remediation "
                    f"is suspended until restart ({type(exc).__name__}).\n"
                )
            return False

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="rapp-incident-automation", daemon=True
        )
        self._diagnosis_thread = threading.Thread(
            target=self._run_diagnosis_loop,
            name="rapp-scheduled-diagnosis",
            daemon=True,
        )
        self._thread.start()
        self._diagnosis_thread.start()

    def stop(self, timeout: float = 10.0) -> bool:
        self._stop.set()
        self._diagnosis_wakeup.set()
        deadline = time.monotonic() + max(0.0, timeout)
        for thread in (self._thread, self._diagnosis_thread):
            if thread is not None:
                thread.join(max(0.0, deadline - time.monotonic()))
        stopped = all(
            thread is None or not thread.is_alive()
            for thread in (self._thread, self._diagnosis_thread)
        )
        if stopped:
            self._audit.close()
        return stopped

    @staticmethod
    def _failures(report: dict[str, Any]) -> tuple[str, ...]:
        return tuple(sorted(
            check["name"] for check in report.get("checks", [])
            if check.get("status") == "fail" and check.get("severity") == "critical"
        ))

    def _perform_diagnosis(self) -> None:
        with self._state_lock:
            incident = self._incident
            incident_id = incident.incident_id if incident is not None else "scheduled"
        try:
            answer = self._diagnose(AUTOMATIC_QUERY, config.RAPP_AUTOMATION_THREAD_ID)
            status = answer.get("summary", {}).get("status", "unknown")
            source = answer.get("explanation_source", "unknown")
            self._record(
                incident_id,
                "automatic_diagnosis",
                {
                    "status": status,
                    "explanation_source": source,
                    "explanation": answer.get("explanation"),
                    "memory": answer.get("memory"),
                    "read_tools": answer.get("read_tools"),
                },
            )
            # Print every scheduled result, including healthy assessments. This
            # makes the minute cadence observable while an operator is also
            # using the interactive CLI; the durable audit remains authoritative.
            self._notify_safely(
                f"\n[AUTOMATION] Automatic diagnosis: {status}. "
                f"{answer.get('explanation', '')}\n"
            )
        except Exception as exc:
            source = "unavailable"
            self._record(
                incident_id,
                "diagnosis_failed",
                {"error": type(exc).__name__, "message": str(exc)[:300]},
            )
            self._notify_safely(
                f"\n[AUTOMATION] Automatic diagnosis failed safely: "
                f"{type(exc).__name__}\n"
            )
        finally:
            with self._state_lock:
                current = self._incident
                if current is not None and current.incident_id == incident_id:
                    current.diagnosis_completed = True
                    current.diagnosis_source = source
            self._diagnosis_wakeup.set()

    def _run_diagnosis_loop(self) -> None:
        while not self._stop.is_set():
            if self.diagnosis_tick():
                continue
            now = self._monotonic()
            wait_s = max(0.0, self._next_diagnosis_at - now)
            self._diagnosis_wakeup.wait(wait_s)
            self._diagnosis_wakeup.clear()

    def diagnosis_tick(self) -> bool:
        """Run one due diagnosis synchronously; exposed for deterministic tests."""
        now = self._monotonic()
        with self._state_lock:
            incident_pending = (
                self._incident is not None
                and self._incident.diagnosis_requested
                and not self._incident.diagnosis_completed
            )
            scheduled_due = now >= self._next_diagnosis_at
        if not (incident_pending or scheduled_due):
            return False
        self._perform_diagnosis()
        with self._state_lock:
            self._next_diagnosis_at = (
                self._monotonic() + config.RAPP_AUTOMATION_LLM_INTERVAL_S
            )
        return True

    @staticmethod
    def _remediation_plan(signature: tuple[str, ...]) -> tuple[str, ...]:
        failures = set(signature)
        actions: list[str] = []
        if failures & _DELIVERY_FAILURES:
            actions.append("reconcile_information_job")
        if failures & (_XAPP_FAILURES | {"telemetry_received"}):
            actions.append("restart_health_xapp")
        return tuple(actions)

    def _open_incident(
        self,
        signature: tuple[str, ...],
        report: dict[str, Any],
        now: float,
    ) -> None:
        incident = ActiveIncident(
            incident_id=str(uuid.uuid4()),
            signature=signature,
            opened_monotonic=now,
            baseline_source_instance=report.get("source_instance_id"),
            baseline_sequence=report.get("sequence_number"),
            remediation_plan=self._remediation_plan(signature),
        )
        with self._state_lock:
            self._incident = incident
        self._record(
            incident.incident_id,
            "incident_opened",
            {"failed_checks": list(signature), "health_report": report},
        )
        self._notify_safely(
            f"\n[INCIDENT {incident.incident_id[:8]}] Opened after "
            f"{self._failure_count} consecutive failures: {', '.join(signature)}. "
            "Waiting for the incident-specific LangGraph diagnosis.\n"
        )
        self._diagnosis_wakeup.set()

    def _remediate(self, incident: ActiveIncident, now: float) -> None:
        if self._stop.is_set() or incident.next_action_index >= len(
            incident.remediation_plan
        ):
            return
        action = incident.remediation_plan[incident.next_action_index]
        if not self._record(
            incident.incident_id,
            "remediation_intent",
            {
                "action": action,
                "policy": "fixed_allowlist_v1",
                "diagnosis_source": incident.diagnosis_source,
            },
        ):
            self._escalate(
                incident,
                {},
                now,
                reason="audit_unavailable_before_write_action",
            )
            return
        if self._stop.is_set():
            self._record(
                incident.incident_id,
                "remediation_cancelled",
                {"action": action, "reason": "service_stopping"},
            )
            return
        try:
            if action == "reconcile_information_job":
                result = self._remediator.reconcile_information_job()
            else:
                result = self._remediator.restart_health_xapp()
        except Exception as exc:
            result = {
                "action": action,
                "ok": False,
                "error": type(exc).__name__,
                "message": str(exc)[:300],
            }
        incident.next_action_index += 1
        incident.verify_deadline = (
            self._monotonic() + config.RAPP_AUTOMATION_VERIFY_TIMEOUT_S
        )
        self._record(incident.incident_id, "remediation_result", result)
        self._notify_safely(
            f"[INCIDENT {incident.incident_id[:8]}] Remediation: "
            f"{result.get('action', 'unknown')} -> "
            f"{'submitted' if result.get('ok') else 'not completed'}; verifying.\n"
        )

    def _is_verified(self, report: dict[str, Any], incident: ActiveIncident) -> bool:
        if report.get("overall_status") not in {"healthy", "degraded"}:
            return False
        source_instance = report.get("source_instance_id")
        sequence = report.get("sequence_number")
        if (
            not isinstance(source_instance, str)
            or not source_instance
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence < 0
        ):
            return False
        if incident.baseline_source_instance is None:
            return True
        if source_instance != incident.baseline_source_instance:
            return True
        baseline = incident.baseline_sequence
        return (
            isinstance(baseline, int)
            and not isinstance(baseline, bool)
            and sequence > baseline
        )

    def _close_incident(self, report: dict[str, Any], now: float) -> None:
        assert self._incident is not None
        incident = self._incident
        self._record(
            incident.incident_id,
            "recovery_verified",
            {
                "elapsed_seconds": round(now - incident.opened_monotonic, 3),
                "health_report": report,
            },
        )
        self._notify_safely(
            f"\n[INCIDENT {incident.incident_id[:8]}] Recovery verified in "
            f"{now - incident.opened_monotonic:.1f}s.\n"
        )
        self._incident = None
        self._failure_count = 0
        self._failure_signature = ()
        self._cooldown_until = now + config.RAPP_AUTOMATION_COOLDOWN_S

    def _escalate(
        self,
        incident: ActiveIncident,
        report: dict[str, Any],
        now: float,
        *,
        reason: str,
    ) -> None:
        self._record(
            incident.incident_id,
            "incident_escalated",
            {
                "reason": reason,
                "failed_checks": list(self._failures(report)),
                "health_report": report,
            },
        )
        self._notify_safely(
            f"\n[INCIDENT {incident.incident_id[:8]}] Automatic recovery was "
            f"not completed ({reason}); operator escalation required.\n"
        )
        self._cooldown_until = now + config.RAPP_AUTOMATION_COOLDOWN_S
        self._incident = None
        self._failure_count = 0
        self._failure_signature = ()

    def tick(self) -> None:
        now = self._monotonic()
        snapshot = consumer.get_health_snapshot()
        report = evaluate_health(snapshot)
        signature = self._failures(report)

        if self._incident is not None:
            incident = self._incident
            if not incident.diagnosis_completed:
                return
            if self._is_verified(report, incident):
                self._close_incident(report, now)
            elif incident.verify_deadline is None:
                if incident.next_action_index < len(incident.remediation_plan):
                    self._remediate(incident, now)
                else:
                    self._escalate(
                        incident, report, now, reason="no_safe_remediation_for_failure"
                    )
            elif now >= incident.verify_deadline:
                if incident.next_action_index < len(incident.remediation_plan):
                    self._remediate(incident, now)
                else:
                    self._escalate(
                        incident, report, now, reason="verification_timeout"
                    )
            return

        if (
            now - self._started_at < config.RAPP_AUTOMATION_STARTUP_GRACE_S
            or now < self._cooldown_until
        ):
            return
        if not signature:
            self._failure_signature = ()
            self._failure_count = 0
            return
        if signature == self._failure_signature:
            self._failure_count += 1
        else:
            self._failure_signature = signature
            self._failure_count = 1
        if self._failure_count >= config.RAPP_AUTOMATION_FAILURES_REQUIRED:
            self._open_incident(signature, report, now)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:
                self._record(
                    "worker",
                    "loop_error",
                    {"error": type(exc).__name__, "message": str(exc)[:300]},
                )
            self._stop.wait(config.RAPP_AUTOMATION_POLL_S)
