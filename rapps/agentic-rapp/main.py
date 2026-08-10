"""Interactive entry point for the read-only Health Agent rApp."""
from __future__ import annotations

import threading
import time

import config
import consumer
from deepseek_client import DeepSeekExplainer
from graph import DEFAULT_QUERY, ask, reset_runtime_graph
from memory import (
    RAppMemory,
    NullRAppMemory,
    SnapshotMemoryWriter,
    TelemetryMemoryWorker,
    close_runtime_memory,
    get_runtime_memory,
)


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

    print(
        f"Receiver listening at "
        f"{config.CONSUMER_BASE_URL}{config.CONSUMER_CALLBACK_PATH}"
    )
    registered = False
    try:
        try:
            consumer.register_consumer_job()
            registered = True
        except Exception as exc:
            raise SystemExit(
                f"Could not register the rApp Information Job with "
                f"{config.DME_BASE_URL}: {exc}"
            ) from exc

        print(f"Registered R1 Information Job {config.JOB_ID!r}.")
        if isinstance(memory_store, RAppMemory):
            print(
                f"Durable memory enabled for thread "
                f"{config.RAPP_DEFAULT_THREAD_ID!r}; telemetry rolls up every "
                f"{config.TELEMETRY_MEMORY_WINDOW_S:g} seconds."
            )
        print(f"Ask {DEFAULT_QUERY!r} or type 'exit'.\n")

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
    finally:
        if registered:
            consumer.deregister_consumer_job()
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
        if writer_stopped and worker_stopped:
            reset_runtime_graph()
            close_runtime_memory()


if __name__ == "__main__":
    main()
