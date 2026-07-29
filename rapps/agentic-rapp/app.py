"""
R1/DME-lite: a lightweight implementation of the O-RAN R1 interface's Data
Management & Exposure (DME) services, built directly against O-RAN SC's
published Information Coordination Service (ICS) OpenAPI contract
(ics-api.json, "Data management and exposure" v1.0).

This is NOT the O-RAN SC Java/Spring ICS - it's a from-scratch
implementation of the same REST contract, sized for a lab rApp/xApp
evaluation rather than a multi-vendor production SMO. What's implemented:

  Data producer (registration)  - PUT/GET/DELETE info-types, info-producers
  Data producer (callbacks)     - DME calls the PRODUCER's callback URLs
                                   (implemented by the producer, not here)
  Data consumer                 - PUT/GET/DELETE info-jobs (subscriptions)
  Service status                - GET /status

Deliberately out of scope for this lab implementation (noted here for
transparency in write-ups): A1-EI (Near-RT RIC as consumer), the OPA-based
authorization hook, info-type-subscription change notifications, and
Spring Actuator endpoints. None of these affect the producer/consumer data
flow being evaluated.

Delivery mechanism: per the ICS design principle "ICS provides APIs for
control of data subscriptions, but is not involved in the delivery of
data... any delivery protocol can be used" - this lab setup uses plain
REST push (the producer POSTs JSON to the consumer's job_result_uri),
which is one of the protocols the real DMaaP Adapter/Mediator also
support alongside Kafka.
"""
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request

import config
from store import Store

app = Flask(__name__)
store = Store()

REQUEST_TIMEOUT_S = config.HTTP_TIMEOUT_S


def _notify_producer_job_put(producer_reg: dict, job_id: str, job: dict):
    """Mirrors the real ICS calling a producer's info_job_callback_url
    when a subscription is created/updated for a type it supports."""
    payload = {
        "info_job_identity": job_id,
        "info_type_identity": job.get("info_type_id"),
        "owner": job.get("job_owner"),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "info_job_data": job.get("job_definition", {}),
        # deprecated per spec, but still populated for producers that
        # haven't migrated to reading target from info_job_data
        "target_uri": job.get("job_result_uri"),
    }
    try:
        requests.post(producer_reg["info_job_callback_url"], json=payload, timeout=REQUEST_TIMEOUT_S)
    except requests.RequestException as e:
        app.logger.warning("Producer job-created callback failed for job %s: %s", job_id, e)


def _notify_producer_job_deleted(producer_reg: dict, job_id: str):
    try:
        url = producer_reg["info_job_callback_url"].rstrip("/") + f"/{job_id}"
        requests.delete(url, timeout=REQUEST_TIMEOUT_S)
    except requests.RequestException as e:
        app.logger.warning("Producer job-deleted callback failed for job %s: %s", job_id, e)


# ============================= Data producer (registration) =============================

@app.route("/data-producer/v1/info-types/<type_id>", methods=["PUT"])
def put_info_type(type_id):
    body = request.get_json(force=True)
    if "info_job_data_schema" not in body:
        return jsonify({"status": 400, "detail": "info_job_data_schema is required"}), 400
    existed = store.get_info_type(type_id) is not None
    store.put_info_type(type_id, body)
    return jsonify({}), 200 if existed else 201


@app.route("/data-producer/v1/info-types/<type_id>", methods=["GET"])
def get_info_type_producer(type_id):
    t = store.get_info_type(type_id)
    if t is None:
        return jsonify({"status": 404, "detail": "Information type is not found"}), 404
    return jsonify(t), 200


@app.route("/data-producer/v1/info-types/<type_id>", methods=["DELETE"])
def delete_info_type(type_id):
    producers = store.producers_for_type(type_id)
    if producers:
        return jsonify({"status": 409, "detail": "The Information type has one or several active producers"}), 409
    store.delete_info_type(type_id)
    return "", 204


@app.route("/data-producer/v1/info-types", methods=["GET"])
def list_info_types_producer():
    return jsonify(store.list_info_type_ids()), 200


@app.route("/data-producer/v1/info-producers/<producer_id>", methods=["PUT"])
def put_producer(producer_id):
    body = request.get_json(force=True)
    for field in ("info_producer_supervision_callback_url", "info_job_callback_url", "supported_info_types"):
        if field not in body:
            return jsonify({"status": 400, "detail": f"{field} is required"}), 400
    for type_id in body["supported_info_types"]:
        if store.get_info_type(type_id) is None:
            return jsonify({"status": 404, "detail": f"Producer type not found: {type_id}"}), 404

    existed = store.get_producer(producer_id) is not None
    store.put_producer(producer_id, body)

    # Real ICS behavior: a newly (re)started producer gets notified of all
    # existing jobs of the types it supports, so it can start producing
    # without the consumer having to do anything.
    for type_id in body["supported_info_types"]:
        for job_id, job in store.jobs_for_type(type_id).items():
            _notify_producer_job_put(body, job_id, job)

    return jsonify({}), 200 if existed else 201


@app.route("/data-producer/v1/info-producers/<producer_id>", methods=["GET"])
def get_producer(producer_id):
    p = store.get_producer(producer_id)
    if p is None:
        return jsonify({"status": 404, "detail": "Information producer is not found"}), 404
    return jsonify(p), 200


@app.route("/data-producer/v1/info-producers/<producer_id>", methods=["DELETE"])
def delete_producer(producer_id):
    if store.get_producer(producer_id) is None:
        return jsonify({"status": 404, "detail": "Producer is not found"}), 404
    store.delete_producer(producer_id)
    return "", 204


@app.route("/data-producer/v1/info-producers", methods=["GET"])
def list_producers():
    type_filter = request.args.get("infoTypeId")
    return jsonify(store.list_producer_ids(type_filter)), 200


@app.route("/data-producer/v1/info-producers/<producer_id>/status", methods=["GET"])
def producer_status(producer_id):
    p = store.get_producer(producer_id)
    if p is None:
        return jsonify({"status": 404, "detail": "Information producer is not found"}), 404
    try:
        r = requests.get(p["info_producer_supervision_callback_url"], timeout=REQUEST_TIMEOUT_S)
        state = "ENABLED" if r.status_code == 200 else "DISABLED"
    except requests.RequestException:
        state = "DISABLED"
    return jsonify({"operational_state": state}), 200


# ============================= Data consumer =============================

@app.route("/data-consumer/v1/info-types", methods=["GET"])
def list_info_types_consumer():
    return jsonify(store.list_info_type_ids()), 200


@app.route("/data-consumer/v1/info-types/<type_id>", methods=["GET"])
def get_info_type_consumer(type_id):
    t = store.get_info_type(type_id)
    if t is None:
        return jsonify({"status": 404, "detail": "Information type is not found"}), 404
    producers = store.producers_for_type(type_id)
    return jsonify({
        "no_of_producers": len(producers),
        "type_status": "ENABLED" if producers else "DISABLED",
        "job_data_schema": t.get("info_job_data_schema", {}),
    }), 200


@app.route("/data-consumer/v1/info-jobs/<job_id>", methods=["PUT"])
def put_job(job_id):
    body = request.get_json(force=True)
    for field in ("info_type_id", "job_result_uri", "job_owner"):
        if field not in body:
            return jsonify({"status": 400, "detail": f"{field} is required"}), 400
    if store.get_info_type(body["info_type_id"]) is None:
        return jsonify({"status": 404, "detail": "Information type is not found"}), 404

    existed = store.get_job(job_id) is not None
    store.put_job(job_id, body)

    # Notify every producer currently registered for this type - this is
    # what actually starts the data flow toward the consumer.
    for producer_reg in store.producers_for_type(body["info_type_id"]).values():
        _notify_producer_job_put(producer_reg, job_id, body)

    return jsonify({}), 200 if existed else 201


@app.route("/data-consumer/v1/info-jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    j = store.get_job(job_id)
    if j is None:
        return jsonify({"status": 404, "detail": "Information subscription job is not found"}), 404
    return jsonify(j), 200


@app.route("/data-consumer/v1/info-jobs/<job_id>", methods=["DELETE"])
def delete_job(job_id):
    job = store.get_job(job_id)
    if job is None:
        return jsonify({"status": 404, "detail": "Information subscription job is not found"}), 404
    for producer_reg in store.producers_for_type(job["info_type_id"]).values():
        _notify_producer_job_deleted(producer_reg, job_id)
    store.delete_job(job_id)
    return "", 204


@app.route("/data-consumer/v1/info-jobs", methods=["GET"])
def list_jobs():
    type_filter = request.args.get("infoTypeId")
    owner_filter = request.args.get("owner")
    return jsonify([jid for jid, _ in store.list_jobs(type_filter, owner_filter)]), 200


@app.route("/data-consumer/v1/info-jobs/<job_id>/status", methods=["GET"])
def job_status(job_id):
    job = store.get_job(job_id)
    if job is None:
        return jsonify({"status": 404, "detail": "Information subscription job is not found"}), 404
    producers = store.producers_for_type(job["info_type_id"])
    return jsonify({
        "info_job_status": "ENABLED" if producers else "DISABLED",
        "producers": list(producers.keys()),
    }), 200


# ============================= Service status =============================

@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "status": "living",
        "no_of_types": len(store.info_types),
        "no_of_producers": len(store.producers),
        "no_of_jobs": len(store.jobs),
    }), 200


def reset_state():
    """Reset the in-memory lab broker for isolated integration tests."""
    global store
    store = Store()


if __name__ == "__main__":
    # Lab/dev server only. For anything longer-running than a demo, put
    # this behind gunicorn/waitress - see README.
    app.run(
        host=config.DME_BIND_HOST,
        port=config.DME_PORT,
        use_reloader=False,
    )
