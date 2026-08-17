from __future__ import annotations

import signal

import pytest

import main as rapp_main


class FakeStdin:
    def __init__(self, is_tty: bool) -> None:
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


def test_non_tty_uses_headless_shutdown_wait(monkeypatch):
    calls = []
    monkeypatch.setattr(rapp_main.sys, "stdin", FakeStdin(False))
    monkeypatch.setattr(
        rapp_main, "_wait_for_headless_shutdown", lambda: calls.append("wait")
    )
    monkeypatch.setattr(
        "builtins.input",
        lambda _prompt: pytest.fail("headless mode must not read stdin"),
    )

    rapp_main._run_user_interface()

    assert calls == ["wait"]


def test_tty_keeps_existing_interactive_exit_behavior(monkeypatch):
    prompts = []
    monkeypatch.setattr(rapp_main.sys, "stdin", FakeStdin(True))
    monkeypatch.setattr(
        rapp_main,
        "_wait_for_headless_shutdown",
        lambda: pytest.fail("TTY mode must not enter the headless wait"),
    )
    monkeypatch.setattr(
        "builtins.input", lambda prompt: prompts.append(prompt) or "exit"
    )

    rapp_main._run_user_interface()

    assert prompts == ["you> "]


def test_automation_uses_a_dedicated_compiled_graph(monkeypatch):
    memory_store = object()
    compiled_graph = object()
    calls = []
    monkeypatch.setattr(
        rapp_main,
        "build_graph",
        lambda *, memory_store: calls.append(("build", memory_store))
        or compiled_graph,
    )

    def ask_structured(query, *, thread_id, compiled_graph):
        calls.append(("ask", query, thread_id, compiled_graph))
        return {"summary": {"status": "healthy"}}

    monkeypatch.setattr(rapp_main, "ask_structured", ask_structured)

    diagnose = rapp_main._build_automation_diagnoser(memory_store)
    result = diagnose("scheduled question", "health-agent-automation")

    assert result == {"summary": {"status": "healthy"}}
    assert calls == [
        ("build", memory_store),
        (
            "ask",
            "scheduled question",
            "health-agent-automation",
            compiled_graph,
        ),
    ]


def test_headless_sigterm_sets_event_and_restores_handler(monkeypatch):
    installed_handler = None
    signal_calls = []
    previous_handler = object()

    class Event:
        def __init__(self) -> None:
            self.is_set = False

        def set(self) -> None:
            self.is_set = True

        def wait(self, _timeout: float) -> bool:
            assert installed_handler is not None
            installed_handler(signal.SIGTERM, None)
            return self.is_set

    def install(sig, handler):
        nonlocal installed_handler
        signal_calls.append((sig, handler))
        if handler is not previous_handler:
            installed_handler = handler

    monkeypatch.setattr(rapp_main.signal, "getsignal", lambda _sig: previous_handler)
    monkeypatch.setattr(rapp_main.signal, "signal", install)

    event = Event()
    rapp_main._wait_for_headless_shutdown(event)

    assert event.is_set is True
    assert signal_calls[0][0] == signal.SIGTERM
    assert signal_calls[-1] == (signal.SIGTERM, previous_handler)


def test_registration_failure_continues_when_automation_enabled(
    monkeypatch, capsys
):
    monkeypatch.setattr(rapp_main.config, "RAPP_AUTOMATION_ENABLED", True)
    monkeypatch.setattr(
        rapp_main.consumer,
        "register_consumer_job",
        lambda: (_ for _ in ()).throw(RuntimeError("DME unavailable")),
    )

    assert rapp_main._register_information_job() is False
    assert "Automatic incident recovery will keep running" in capsys.readouterr().out


def test_registration_failure_remains_fatal_when_automation_disabled(monkeypatch):
    monkeypatch.setattr(rapp_main.config, "RAPP_AUTOMATION_ENABLED", False)
    monkeypatch.setattr(
        rapp_main.consumer,
        "register_consumer_job",
        lambda: (_ for _ in ()).throw(RuntimeError("DME unavailable")),
    )

    with pytest.raises(SystemExit, match="Could not register"):
        rapp_main._register_information_job()


@pytest.mark.parametrize(
    ("registered", "automation_enabled", "expected_calls"),
    [
        (True, False, 1),
        (False, True, 1),
        (False, False, 0),
    ],
)
def test_shutdown_deregisters_jobs_that_automation_may_have_created(
    monkeypatch, registered, automation_enabled, expected_calls
):
    calls = []
    monkeypatch.setattr(
        rapp_main.config, "RAPP_AUTOMATION_ENABLED", automation_enabled
    )
    monkeypatch.setattr(
        rapp_main.consumer,
        "deregister_consumer_job",
        lambda: calls.append("deregister"),
    )

    rapp_main._deregister_information_job(registered)

    assert len(calls) == expected_calls
