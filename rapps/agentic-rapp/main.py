"""Interactive entry point for the read-only Health Agent rApp."""
from __future__ import annotations

import threading
import time

import config
import consumer
from graph import ask


def main() -> None:
    receiver_thread = threading.Thread(target=consumer.run_receiver, daemon=True)
    receiver_thread.start()
    time.sleep(0.5)

    print(
        f"Receiver listening at "
        f"{config.CONSUMER_BASE_URL}{config.CONSUMER_CALLBACK_PATH}"
    )
    try:
        consumer.register_consumer_job()
    except Exception as exc:
        raise SystemExit(
            f"Could not register the rApp Information Job with {config.DME_BASE_URL}: {exc}"
        ) from exc

    print(f"Registered R1 Information Job {config.JOB_ID!r}.")
    print("Ask 'Is the system healthy?' or type 'exit'.\n")

    try:
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
        consumer.deregister_consumer_job()


if __name__ == "__main__":
    main()
