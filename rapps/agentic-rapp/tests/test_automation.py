from __future__ import annotations

import sqlite3
import threading
import time

import pytest

import automation


class Clock:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakeRemediator:
    def __init__(self) -> None:
        self.actions: list[str] = []

    def reconcile_information_job(self):
        self.actions.append("reconcile_information_job")
        return {"action": self.actions[-1], "ok": True}

    def restart_health_xapp(self):
        self.actions.append("restart_health_xapp")
        return {"action": self.actions[-1], "ok": True}


class BrokenAudit:
    def record(self, *_args, **_kwargs):
        raise sqlite3.OperationalError("disk unavailable")

    def close(self):
        pass


def report(
    status: str,
    sequence: int | None = None,
    *,
    instance: str | None = "xapp-a",
    failed_check: str = "kpm_data_fresh",
):
    failed = status == "unhealthy"
    return {
        "overall_status": status,
        "source_instance_id": instance,
        "sequence_number": sequence,
        "checks": [
            {
                "name": failed_check,
                "status": "fail" if failed else "pass",
                "severity": "critical",
            }
        ],
    }


def make_worker(
    tmp_path,
    monkeypatch,
    reports,
    clock,
    diagnoses,
    remediator=None,
    audit=None,
    diagnose_error=None,
):
    current = iter(reports)
    monkeypatch.setattr(automation.consumer, "get_health_snapshot", lambda: {})
    monkeypatch.setattr(automation, "evaluate_health", lambda _snapshot: next(current))
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_STARTUP_GRACE_S", 0)
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_FAILURES_REQUIRED", 3)
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_LLM_INTERVAL_S", 60)
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_VERIFY_TIMEOUT_S", 10)
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_COOLDOWN_S", 20)
    audit = audit or automation.AutomationAudit(str(tmp_path / "audit.sqlite3"))

    def diagnose(_query, _thread_id):
        diagnoses.append(clock())
        if diagnose_error is not None:
            raise diagnose_error
        return {
            "summary": {"status": "unhealthy"},
            "explanation_source": "deepseek",
            "explanation": "Scheduled diagnosis.",
            "memory": {"thread_id": "health-agent-automation"},
            "read_tools": {"tools_used": ["get_dme_job_status"]},
        }

    return automation.IncidentAutomationWorker(
        diagnose,
        audit=audit,
        remediator=remediator or FakeRemediator(),
        notify=lambda _message: None,
        monotonic=clock,
    )


def event_types(database_path):
    connection = sqlite3.connect(str(database_path))
    values = [row[0] for row in connection.execute(
        "SELECT event_type FROM automation_events ORDER BY id"
    )]
    connection.close()
    return values


def test_scheduled_diagnosis_runs_at_most_once_per_interval(tmp_path, monkeypatch):
    clock = Clock()
    diagnoses = []
    worker = make_worker(tmp_path, monkeypatch, [], clock, diagnoses)

    assert worker.diagnosis_tick() is True
    clock.advance(30)
    assert worker.diagnosis_tick() is False
    clock.advance(29)
    assert worker.diagnosis_tick() is False
    clock.advance(1)
    assert worker.diagnosis_tick() is True

    assert diagnoses == [1000.0, 1060.0]
    worker.stop()


def test_healthy_scheduled_diagnosis_is_visible(tmp_path, monkeypatch):
    clock = Clock()
    messages = []
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_STARTUP_GRACE_S", 0)
    audit = automation.AutomationAudit(str(tmp_path / "audit.sqlite3"))
    worker = automation.IncidentAutomationWorker(
        lambda _query, _thread_id: {
            "summary": {"status": "healthy"},
            "explanation_source": "deepseek",
            "explanation": "Live KPM evidence is healthy.",
        },
        audit=audit,
        notify=messages.append,
        monotonic=clock,
    )

    assert worker.diagnosis_tick() is True

    assert any("Automatic diagnosis: healthy" in message for message in messages)
    worker.stop()


def test_slow_diagnosis_does_not_block_health_tick(tmp_path, monkeypatch):
    clock = Clock()
    entered = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_FAILURES_REQUIRED", 1)
    worker = make_worker(
        tmp_path,
        monkeypatch,
        [report("unhealthy", 10)],
        clock,
        [],
    )
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_FAILURES_REQUIRED", 1)

    def blocking_diagnosis(_query, _thread_id):
        entered.set()
        release.wait(1)
        return {
            "summary": {"status": "unhealthy"},
            "explanation_source": "deepseek",
            "explanation": "Diagnosis finished.",
        }

    worker._diagnose = blocking_diagnosis
    diagnosis_thread = threading.Thread(target=worker._perform_diagnosis)
    diagnosis_thread.start()
    assert entered.wait(1)

    started = time.monotonic()
    worker.tick()
    elapsed = time.monotonic() - started

    assert elapsed < 0.1
    assert worker._incident is not None
    release.set()
    diagnosis_thread.join(1)
    worker.stop()


def test_incident_diagnosis_precedes_remediation_and_recovery_verifies(
    tmp_path, monkeypatch
):
    clock = Clock()
    diagnoses = []
    remediator = FakeRemediator()
    worker = make_worker(
        tmp_path,
        monkeypatch,
        [
            report("unhealthy", 10),
            report("unhealthy", 10),
            report("unhealthy", 10),
            report("unhealthy", 10),
            report("healthy", 11),
        ],
        clock,
        diagnoses,
        remediator,
    )

    worker.tick()
    clock.advance(5)
    worker.tick()
    clock.advance(5)
    worker.tick()
    assert worker._incident is not None
    assert remediator.actions == []

    assert worker.diagnosis_tick() is True
    worker.tick()
    assert remediator.actions == ["reconcile_information_job"]

    clock.advance(5)
    worker.tick()
    assert worker._incident is None

    events = event_types(tmp_path / "audit.sqlite3")
    assert events.index("incident_opened") < events.index("automatic_diagnosis")
    assert events.index("automatic_diagnosis") < events.index("remediation_intent")
    assert "remediation_result" in events
    assert "recovery_verified" in events
    worker.stop()


def test_new_xapp_instance_with_reset_sequence_verifies_recovery(
    tmp_path, monkeypatch
):
    clock = Clock()
    remediator = FakeRemediator()
    worker = make_worker(
        tmp_path,
        monkeypatch,
        [
            report("unhealthy", 100, instance="old-xapp"),
            report("unhealthy", 100, instance="old-xapp"),
            report("unhealthy", 100, instance="old-xapp"),
            report("unhealthy", 100, instance="old-xapp"),
            report("healthy", 1, instance="new-xapp"),
        ],
        clock,
        [],
        remediator,
    )

    worker.tick()
    worker.tick()
    worker.tick()
    worker.diagnosis_tick()
    worker.tick()
    worker.tick()

    assert worker._incident is None
    assert remediator.actions == ["reconcile_information_job"]
    assert "recovery_verified" in event_types(tmp_path / "audit.sqlite3")
    worker.stop()


def test_second_stage_then_escalation_resets_failure_debounce(
    tmp_path, monkeypatch
):
    clock = Clock()
    remediator = FakeRemediator()
    worker = make_worker(
        tmp_path,
        monkeypatch,
        [report("unhealthy", 10)] * 7,
        clock,
        [],
        remediator,
    )

    worker.tick()
    worker.tick()
    worker.tick()
    worker.diagnosis_tick()
    worker.tick()
    clock.advance(10)
    worker.tick()
    clock.advance(10)
    worker.tick()

    assert remediator.actions == [
        "reconcile_information_job",
        "restart_health_xapp",
    ]
    assert worker._incident is None
    assert worker._failure_count == 0

    clock.advance(20)
    worker.tick()
    assert worker._failure_count == 1
    assert worker._incident is None
    worker.stop()


def test_unsupported_e2_only_failure_escalates_without_write(
    tmp_path, monkeypatch
):
    clock = Clock()
    remediator = FakeRemediator()
    worker = make_worker(
        tmp_path,
        monkeypatch,
        [report("unhealthy", 5, failed_check="e2_nodes_connected")] * 4,
        clock,
        [],
        remediator,
    )

    worker.tick()
    worker.tick()
    worker.tick()
    worker.diagnosis_tick()
    worker.tick()

    assert remediator.actions == []
    assert worker._incident is None
    assert "incident_escalated" in event_types(tmp_path / "audit.sqlite3")
    worker.stop()


def test_audit_failure_fails_closed_without_killing_tick(tmp_path, monkeypatch):
    clock = Clock()
    remediator = FakeRemediator()
    worker = make_worker(
        tmp_path,
        monkeypatch,
        [report("unhealthy", 8)] * 4,
        clock,
        [],
        remediator,
        audit=BrokenAudit(),
    )

    worker.tick()
    worker.tick()
    worker.tick()
    worker.diagnosis_tick()
    worker.tick()

    assert remediator.actions == []
    assert worker._incident is None
    worker.stop()


def test_diagnosis_failure_is_recorded_before_deterministic_recovery(
    tmp_path, monkeypatch
):
    clock = Clock()
    remediator = FakeRemediator()
    worker = make_worker(
        tmp_path,
        monkeypatch,
        [report("unhealthy", 8)] * 4,
        clock,
        [],
        remediator,
        diagnose_error=RuntimeError("provider unavailable"),
    )

    worker.tick()
    worker.tick()
    worker.tick()
    worker.diagnosis_tick()
    worker.tick()

    assert remediator.actions == ["reconcile_information_job"]
    events = event_types(tmp_path / "audit.sqlite3")
    assert events.index("diagnosis_failed") < events.index("remediation_intent")
    worker.stop()


def test_xapp_restart_rejects_option_like_target(monkeypatch):
    monkeypatch.setattr(
        automation.config, "RAPP_AUTOMATION_XAPP_RESTART_BACKEND", "docker"
    )
    monkeypatch.setattr(
        automation.config, "RAPP_AUTOMATION_XAPP_RESTART_TARGET", "--help"
    )

    result = automation.Remediator().restart_health_xapp()

    assert result["ok"] is False
    assert result["skipped"] is True
    assert "invalid" in result["reason"]


@pytest.mark.parametrize("bad_value", [0, -1, float("nan"), float("inf")])
def test_invalid_poll_interval_fails_fast(tmp_path, monkeypatch, bad_value):
    monkeypatch.setattr(automation.config, "RAPP_AUTOMATION_POLL_S", bad_value)

    with pytest.raises(ValueError, match="RAPP_AUTOMATION_POLL_S"):
        automation.IncidentAutomationWorker(
            lambda *_args: {},
            audit=BrokenAudit(),
        )
