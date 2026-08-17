"""Interactive or headless entry point for the automated Health Agent rApp."""
from __future__ import annotations

import signal
import sys
import threading
import time

import config
import consumer
from automation import IncidentAutomationWorker
from deepseek_client import DeepSeekExplainer
from graph import (
    DEFAULT_QUERY,
    ask,
    ask_structured,
    build_graph,
    reset_runtime_graph,
)
from memory import (
    RAppMemory,
    NullRAppMemory,
    SnapshotMemoryWriter,
    TelemetryMemoryWorker,
    close_runtime_memory,
    get_runtime_memory,
)


def _register_information_job() -> bool:
    """Register once at startup, deferring transient recovery to automation."""
    try:
        consumer.register_consumer_job()
    except Exception as exc:
        message = (
            f"Could not register the rApp Information Job with "
            f"{config.DME_BASE_URL}: {exc}"
        )
        if not config.RAPP_AUTOMATION_ENABLED:
            raise SystemExit(message) from exc
        print(
            f"Warning: {message}. Automatic incident recovery will keep "
            "running and retry reconciliation."
        )
        return False

    print(f"Registered R1 Information Job {config.JOB_ID!r}.")
    return True


def _deregister_information_job(registered: bool) -> None:
    """Best-effort cleanup, including jobs created later by automation."""
    if registered or config.RAPP_AUTOMATION_ENABLED:
        consumer.deregister_consumer_job()


def _wait_for_headless_shutdown(
    shutdown_event: threading.Event | None = None,
) -> None:
    """Keep a non-interactive service alive until SIGTERM or Ctrl-C."""
    shutdown_event = shutdown_event or threading.Event()
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def request_shutdown(_signum, _frame) -> None:
        shutdown_event.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    print("Headless automation is running; send SIGTERM or Ctrl-C to stop.")
    try:
        while not shutdown_event.wait(1.0):
            pass
    except KeyboardInterrupt:
        print()
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)


def _run_user_interface() -> None:
    """Run the existing CLI on a TTY, otherwise host the headless service."""
    if not sys.stdin.isatty():
        _wait_for_headless_shutdown()
        return

    while True:
        try:
            query = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if query.lower() in {"exit", "quit"}:
            break
        if not query:
            continue
        print(f"agent> {ask(query)}\n")


def _build_automation_diagnoser(memory_store):
    """Create an automation-only graph so CLI calls cannot block its cadence."""
    automation_graph = build_graph(memory_store=memory_store)

    def diagnose(query: str, thread_id: str | None):
        return ask_structured(
            query,
            thread_id=thread_id,
            compiled_graph=automation_graph,
        )

    return diagnose


def main() -> None:
    memory_store = get_runtime_memory()
    memory_worker = None
    memory_writer = None
    if isinstance(memory_store, RAppMemory):
        memory_writer = SnapshotMemoryWriter(memory_store)
        memory_writer.start()
        consumer.set_snapshot_sink(memory_writer.submit)
        memory_worker = TelemetryMemoryWorker(
            memory_store,
            DeepSeekExplainer(),
        )
        memory_worker.start()
    elif (
        isinstance(memory_store, NullRAppMemory)
        and memory_store.initialization_error
    ):
        print(
            "Warning: durable rApp memory is unavailable; continuing with the "
            f"original stateless behavior ({memory_store.initialization_error})."
        )

    receiver_thread = threading.Thread(target=consumer.run_receiver, daemon=True)
    receiver_thread.start()
    time.sleep(0.5)
    automation_worker = None

    print(
        f"Receiver listening at "
        f"{config.CONSUMER_BASE_URL}{config.CONSUMER_CALLBACK_PATH}"
    )
    registered = False
    try:
        registered = _register_information_job()
        if config.RAPP_AUTOMATION_ENABLED:
            automation_worker = IncidentAutomationWorker(
                _build_automation_diagnoser(memory_store)
            )
            automation_worker.start()
            print(
                "Automatic incident detection, diagnosis, remediation, and "
                f"verification enabled; scheduled AI assessment every "
                f"{config.RAPP_AUTOMATION_LLM_INTERVAL_S:g} seconds."
            )
        if isinstance(memory_store, RAppMemory):
            print(
                f"Durable memory enabled for thread "
                f"{config.RAPP_DEFAULT_THREAD_ID!r}; telemetry rolls up every "
                f"{config.TELEMETRY_MEMORY_WINDOW_S:g} seconds."
            )
        print(f"Ask {DEFAULT_QUERY!r} or type 'exit'.\n")
        _run_user_interface()
    finally:
        automation_stopped = True
        if automation_worker is not None:
            automation_stopped = automation_worker.stop()
            if not automation_stopped:
                print("Automation worker is still finishing a bounded operation.")
        _deregister_information_job(registered)
        consumer.set_snapshot_sink(None)
        writer_stopped = True
        if memory_writer is not None:
            writer_stopped = memory_writer.stop()
            writer_stats = memory_writer.stats()
            if (
                not writer_stopped
                or writer_stats["queued"]
                or writer_stats["dropped"]
                or writer_stats["failures"]
            ):
                print(f"Telemetry memory writer stopped with {writer_stats}.")
        worker_stopped = True
        if memory_worker is not None:
            worker_stopped = memory_worker.stop()
            if not worker_stopped:
                print(
                    "Telemetry memory rollup is still finishing; leaving the "
                    "database connection open for safe process shutdown."
                )
        if writer_stopped and worker_stopped and automation_stopped:
            reset_runtime_graph()
            close_runtime_memory()


if __name__ == "__main__":
    main()
