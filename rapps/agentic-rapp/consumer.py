"""R1/DME consumer and telemetry receiver for the read-only health rApp."""
from __future__ import annotations

import copy
import logging
import threading
from datetime import datetime, timezone
from typing import Callable, Optional, TypedDict

import requests
from flask import Flask, jsonify, request

import config
from telemetry import isoformat_utc, validate_telemetry_payload


class HealthSnapshot(TypedDict):
    received: bool
    telemetry: Optional[dict]
    received_at: Optional[str]
    producer_pushed_at: Optional[str]
    info_job_identity: Optional[str]


_lock = threading.RLock()
_latest_snapshot: Optional[HealthSnapshot] = None
_snapshot_sink: Optional[Callable[[HealthSnapshot], object]] = None

receiver_app = Flask(__name__)
receiver_app.config["MAX_CONTENT_LENGTH"] = config.CONSUMER_MAX_BODY_BYTES


@receiver_app.route(config.CONSUMER_CALLBACK_PATH, methods=["POST"])
def receive_health_data() -> tuple:
    global _latest_snapshot
    envelope = request.get_json(silent=True)
    if not isinstance(envelope, dict):
        return jsonify({"status": "rejected", "errors": ["body must be an object"]}), 400

    telemetry = envelope.get("telemetry")
    errors = validate_telemetry_payload(telemetry)
    if errors:
        return jsonify({"status": "rejected", "errors": errors}), 400

    job_id = envelope.get("info_job_identity")
    if job_id != config.JOB_ID:
        return (
            jsonify(
                {
                    "status": "rejected",
                    "errors": [f"unexpected info_job_identity: {job_id!r}"],
                }
            ),
            409,
        )

    snapshot = HealthSnapshot(
        received=True,
        telemetry=copy.deepcopy(telemetry),
        received_at=isoformat_utc(datetime.now(timezone.utc)),
        producer_pushed_at=envelope.get("producer_pushed_at"),
        info_job_identity=job_id,
    )
    with _lock:
        _latest_snapshot = snapshot
        sink = _snapshot_sink
    if sink is not None:
        try:
            # The runtime attaches a bounded non-blocking queue here. Keep the
            # generic hook outside the latest-snapshot lock so readers are never
            # held behind persistence work.
            sink(copy.deepcopy(snapshot))
        except Exception:
            logging.getLogger(__name__).exception(
                "Could not append accepted telemetry to rApp memory"
            )
    return jsonify({"status": "accepted"}), 200


def get_health_snapshot() -> HealthSnapshot:
    with _lock:
        if _latest_snapshot is not None:
            return copy.deepcopy(_latest_snapshot)
    return HealthSnapshot(
        received=False,
        telemetry=None,
        received_at=None,
        producer_pushed_at=None,
        info_job_identity=None,
    )


def register_consumer_job() -> None:
    """Create or update this rApp's idempotent R1 Information Job."""
    callback_url = f"{config.CONSUMER_BASE_URL}{config.CONSUMER_CALLBACK_PATH}"
    job = {
        "info_type_id": config.INFO_TYPE_ID,
        "job_result_uri": callback_url,
        "job_owner": config.JOB_OWNER,
        "job_definition": {"callback_url": callback_url},
    }
    response = requests.put(
        f"{config.DME_BASE_URL}/data-consumer/v1/info-jobs/{config.JOB_ID}",
        json=job,
        timeout=config.HTTP_TIMEOUT_S,
    )
    response.raise_for_status()


def deregister_consumer_job() -> None:
    try:
        response = requests.delete(
            f"{config.DME_BASE_URL}/data-consumer/v1/info-jobs/{config.JOB_ID}",
            timeout=config.HTTP_TIMEOUT_S,
        )
        if response.status_code not in (204, 404):
            response.raise_for_status()
    except requests.RequestException:
        # Shutdown cleanup is best effort. DME persistence/reconciliation owns
        # recovery in a production deployment.
        pass


def reset_state() -> None:
    global _latest_snapshot, _snapshot_sink
    with _lock:
        _latest_snapshot = None
        _snapshot_sink = None


def set_snapshot_sink(
    sink: Optional[Callable[[HealthSnapshot], object]],
) -> None:
    """Attach durable history storage without changing the R1 callback API."""
    global _snapshot_sink
    with _lock:
        _snapshot_sink = sink


def run_receiver() -> None:
    # Keep receiver errors visible without flooding the interactive rApp prompt
    # with one Werkzeug access-log line per telemetry delivery.
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    receiver_app.run(
        host=config.CONSUMER_BIND_HOST,
        port=config.CONSUMER_PORT,
        use_reloader=False,
    )
