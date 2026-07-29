"""
In-memory data store backing the R1/DME-lite broker.

Mirrors the entities defined by O-RAN SC's Information Coordination
Service (ICS) API (the reference implementation of the R1 Data
Management & Exposure services): Information Types, Information
Producers, and Information Jobs (subscriptions). A production ICS
persists these (S3 or filesystem); an in-memory store is fine for a
lab-scale rApp/xApp evaluation, and swapping in real persistence later
doesn't change the API surface at all.
"""
import threading
from datetime import datetime, timezone


class Store:
    def __init__(self):
        self._lock = threading.RLock()
        # info_type_id -> producer_info_type_info dict
        self.info_types = {}
        # producer_id -> producer_registration_info dict
        self.producers = {}
        # job_id -> consumer_job dict (as submitted by the consumer)
        self.jobs = {}

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    # ---- Info Types (registered by producers) ----
    def put_info_type(self, type_id: str, type_info: dict):
        with self._lock:
            self.info_types[type_id] = type_info

    def get_info_type(self, type_id: str):
        with self._lock:
            return self.info_types.get(type_id)

    def delete_info_type(self, type_id: str):
        with self._lock:
            self.info_types.pop(type_id, None)

    def list_info_type_ids(self):
        with self._lock:
            return list(self.info_types.keys())

    # ---- Producers ----
    def put_producer(self, producer_id: str, reg_info: dict):
        with self._lock:
            self.producers[producer_id] = reg_info

    def get_producer(self, producer_id: str):
        with self._lock:
            return self.producers.get(producer_id)

    def delete_producer(self, producer_id: str):
        with self._lock:
            self.producers.pop(producer_id, None)

    def list_producer_ids(self, info_type_id: str = None):
        with self._lock:
            if info_type_id is None:
                return list(self.producers.keys())
            return [
                pid
                for pid, reg in self.producers.items()
                if info_type_id in reg.get("supported_info_types", [])
            ]

    def producers_for_type(self, info_type_id: str):
        with self._lock:
            return {
                pid: reg
                for pid, reg in self.producers.items()
                if info_type_id in reg.get("supported_info_types", [])
            }

    # ---- Jobs (subscriptions) ----
    def put_job(self, job_id: str, job: dict):
        with self._lock:
            self.jobs[job_id] = job

    def get_job(self, job_id: str):
        with self._lock:
            return self.jobs.get(job_id)

    def delete_job(self, job_id: str):
        with self._lock:
            return self.jobs.pop(job_id, None)

    def list_jobs(self, info_type_id: str = None, owner: str = None):
        with self._lock:
            items = list(self.jobs.items())
        if info_type_id:
            items = [(jid, j) for jid, j in items if j.get("info_type_id") == info_type_id]
        if owner:
            items = [(jid, j) for jid, j in items if j.get("job_owner") == owner]
        return items

    def jobs_for_type(self, info_type_id: str):
        with self._lock:
            return {jid: j for jid, j in self.jobs.items() if j.get("info_type_id") == info_type_id}