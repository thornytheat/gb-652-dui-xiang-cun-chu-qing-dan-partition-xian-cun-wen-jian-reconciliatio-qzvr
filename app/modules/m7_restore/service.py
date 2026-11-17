"""M7 归档恢复对账服务：恢复申请、存储服务查询与回执对账。

约定：
- 恢复申请以 (key, version_id) 的幂等键登记；重试复用同一 request_id，不重复计费。
- 存储服务以本地替身复现“受理、查询、回执丢失”，不会连接真实存储。
- 受理后中断可续接：重跑时先查询存储服务已有的受理记录，不重复申请。
- 到期仍无法确认的申请转“待裁决”，既不冒充缺失也不冒充核验成功。
- 迟到回执按 request_id 归回原申请，不按当前最新版本重新归属。
- 旧对象版本的恢复结果不覆盖新版本；恢复完成后再比大小与摘要。
- 路径归集沿用 M2 的规范化规则，不修改路径契约；不做补数领取。
"""
from __future__ import annotations

from datetime import timedelta

from . import core
from .storage_stub import StorageStub, selected, utc

DEFAULT_NOW = "2026-11-17T10:00:00+08:00"
DEFAULT_POLICY = {"fee_per_application": 1, "receipt_timeout_seconds": 3600, "deliver_receipt": True}
DEFAULT_SIMULATION = {
    "lost_receipt": ["dt=2026-11-15/hour=01/b.parquet@v1", "dt=2026-11-15/hour=01/b2.parquet@v1"],
    "late_receipt": ["dt=2026-11-15/hour=02/c.parquet@v2"],
    "failed": ["dt=2026-11-15/hour=03/e.parquet@v1"],
    "reject": ["dt=2026-11-15/hour=03/f.parquet@v1"],
    "accepted_offset_seconds": {
        "dt=2026-11-15/hour=01/b.parquet@v1": -100,
        "dt=2026-11-15/hour=01/b2.parquet@v1": -3700,
        "dt=2026-11-15/hour=02/c.parquet@v2": -3700,
    },
    "late_grace_seconds": 60,
    "results": {
        "dt=2026-11-15/hour=00/a.parquet@v2": {"size": 21, "digest": "sha-a2"},
        "dt=2026-11-15/hour=03/e.parquet@v1": {"size": 50, "digest": "sha-e1"},
        "dt=2026-11-15/hour=03/f.parquet@v1": {"size": 60, "digest": "sha-f1"},
        "dt=2026-11-15/hour=04/i.parquet@v2": {"size": 110, "digest": "sha-i1", "version_id": "v1"},
    },
}
SAMPLE = {"now": DEFAULT_NOW, "policy": DEFAULT_POLICY, "simulation": DEFAULT_SIMULATION}


def _now(payload):
    return core.parse_time(payload.get("now", DEFAULT_NOW))


def _policy(payload) -> dict:
    raw = payload.get("policy") or {}
    return {
        "fee_per_application": int(raw.get("fee_per_application", DEFAULT_POLICY["fee_per_application"])),
        "receipt_timeout_seconds": int(raw.get("receipt_timeout_seconds", DEFAULT_POLICY["receipt_timeout_seconds"])),
        "deliver_receipt": bool(raw.get("deliver_receipt", DEFAULT_POLICY["deliver_receipt"])),
    }


def _simulation(payload) -> dict:
    supplied = payload.get("simulation")
    return DEFAULT_SIMULATION if supplied is None else (supplied or {})


def _offset(simulation: dict, key: str, version: str) -> int:
    for selector, seconds in (simulation.get("accepted_offset_seconds") or {}).items():
        if selected(selector, key, version):
            return int(seconds)
    return 0


def _record(row, status, evidence, request_id=None, receipt=None, restore_required=False):
    return {
        "key": row["key"],
        "version_id": row["version_id"],
        "partition": core.partition_of(row["key"]),
        "source_line": row["source_line"],
        "size": row["size"],
        "digest": row["digest"],
        "status": status,
        "restore_required": restore_required,
        "request_id": request_id,
        "receipt_late": bool(receipt["late"]) if receipt else False,
        "evidence": evidence,
    }


def _fees(store, policy) -> dict:
    applications = store.applications()
    return {
        "application_count": len(applications),
        "billed_count": sum(1 for app in applications if int(app["fee_charged"] or 0) > 0),
        "fee_total": sum(int(app["fee_charged"] or 0) for app in applications),
        "rate": policy["fee_per_application"],
        "unit": "次",
    }


def _storage(store) -> dict:
    jobs = store.jobs()
    return {
        "accepted_count": len(jobs),
        "rejected_count": sum(1 for job in jobs if job["rejected"]),
        "completed_count": sum(1 for job in jobs if job["outcome"] == "completed"),
        "receipt_count": len(store.receipts()),
        "delivery_count": sum(int(job["delivery_count"] or 0) for job in jobs),
    }


def _readback_applications(store) -> list[dict]:
    out = []
    for app in store.applications():
        receipt = store.get_receipt(app["request_id"]) if app["request_id"] else None
        out.append({
            "request_id": app["request_id"],
            "key": app["key"],
            "version_id": app["version_id"],
            "state": app["state"],
            "manifest_size": app["manifest_size"],
            "manifest_digest": app["manifest_digest"],
            "accepted_at": app["accepted_at"],
            "deadline_at": app["deadline_at"],
            "receipt_lost": bool(app["receipt_lost"]),
            "fee_charged": int(app["fee_charged"] or 0),
            "has_receipt": receipt is not None,
            "receipt_late": bool(receipt["late"]) if receipt else False,
        })
    return out


class Restore:
    def __init__(self, store, policy, simulation, now):
        self.store = store
        self.policy = policy
        self.simulation = simulation
        self.now = now
        self.stub = StorageStub(store, simulation, policy["deliver_receipt"], now)

    def _adopt(self, application, job, persist):
        """把存储服务的受理记录归并回申请，用于续接或中断后的恢复。"""
        changes = {}
        if application["request_id"] != job["request_id"]:
            changes["request_id"] = job["request_id"]
        lost = bool(job["policy"].get("lost_receipt", False))
        if bool(application["receipt_lost"]) != lost:
            changes["receipt_lost"] = int(lost)
        fee = 0 if job["rejected"] else self.policy["fee_per_application"]
        if fee > int(application["fee_charged"] or 0):
            changes["fee_charged"] = fee
        if application["state"] == "SUBMITTED":
            changes["state"] = "ACCEPTED"
        if changes:
            application = {**application, **changes}
            if persist:
                application = self.store.save_application(application)
        return application

    def apply(self, row, advance: bool):
        """提交或续接恢复申请，返回 (状态, 证据, 申请, 回执, 本次是否新受理)。"""
        key, version = row["key"], row["version_id"]
        idempotency_key = core.request_key(key, version)
        application = self.store.get_application(idempotency_key)

        if application is None and not advance:
            return "AWAITING_REQUEST", "no-application", None, None, False

        if application is None:
            accepted = self.now + timedelta(seconds=_offset(self.simulation, key, version))
            application = self.store.save_application({
                "idempotency_key": idempotency_key,
                "request_id": None,
                "key": key,
                "version_id": version,
                "manifest_size": row["size"],
                "manifest_digest": row["digest"],
                "state": "SUBMITTED",
                "receipt_lost": 0,
                "fee_charged": 0,
                "accepted_at": core.iso(accepted),
                "deadline_at": core.iso(accepted + timedelta(seconds=self.policy["receipt_timeout_seconds"])),
                "created_at": core.iso(self.now),
            })

        job = self.stub.query(idempotency_key)
        accepted_now = False
        if job is None and advance:
            job = self.stub.accept(application)
            accepted_now = True
        if job is not None:
            application = self._adopt(application, job, persist=advance)
        if job is None:
            return "AWAITING_REQUEST", "not-accepted", application, None, False

        deadline = utc(application["deadline_at"])
        receipt = self.stub.deliver(job, deadline) if advance else self.store.get_receipt(job["request_id"])
        return self._classify(application, job, receipt, deadline, accepted_now, advance)

    def _classify(self, application, job, receipt, deadline, accepted_now, persist=True):
        def closed(state, status, evidence, record=None):
            if application["state"] != state:
                if persist:
                    self.store.save_application({**application, "state": state})
                application["state"] = state
            return status, evidence, application, record, accepted_now

        if job["rejected"]:
            return closed("REJECTED", "REJECTED", "storage-rejected")
        if job["outcome"] == "failed":
            return closed("RESTORE_FAILED", "RESTORE_FAILED", "storage-failed", receipt)
        if receipt is not None and receipt["version_id"] != application["version_id"]:
            return closed("STALE_VERSION", "STALE_VERSION",
                          f"receipt-version={receipt['version_id']}", receipt)
        if receipt is None or receipt["result_size"] is None or receipt["result_digest"] is None:
            unconfirmed = job["outcome"] is not None
            if self.now > deadline:
                evidence = "restored-unconfirmed-past-deadline" if unconfirmed else "receipt-not-received-past-deadline"
                return closed("PENDING_ADJUDICATION", "PENDING_ADJUDICATION", evidence, receipt)
            evidence = "restored-unconfirmed" if unconfirmed else "receipt-pending"
            return closed("ACCEPTED", "RESTORING", evidence, receipt)
        status = core.compare(application["manifest_size"], application["manifest_digest"],
                              receipt["result_size"], receipt["result_digest"])
        if receipt["late"]:
            return closed("COMPLETED_LATE", status, "restore-receipt-late", receipt)
        return closed("COMPLETED", status, "restore-receipt", receipt)


def reconcile(payload: dict, store, advance: bool = True) -> dict:
    settings = {**SAMPLE, **payload}
    now = _now(settings)
    policy = _policy(settings)
    simulation = _simulation(settings)
    manifest = settings.get("manifest")
    manifest = core.normalize_manifest(manifest) if manifest else core.load_manifest(settings.get("manifest_path"))
    registry = settings.get("objects")
    registry = core.normalize_objects(registry) if registry is not None else core.load_objects(settings.get("objects_path"))

    current = core.current_versions(registry)
    by_version = core.index_versions(registry)
    restore = Restore(store, policy, simulation, now)
    interrupt_after = simulation.get("interrupt_after_accepts")
    if interrupt_after is None and simulation.get("interrupt"):
        interrupt_after = 1

    records = []
    seen = set()
    accepted_here = 0
    interrupted = False
    for row in manifest:
        identity = (row["key"], row["version_id"])
        if identity in seen:
            records.append(_record(row, "DUPLICATE", "duplicate-manifest-row"))
            continue
        seen.add(identity)
        current_entry = current.get(row["key"])
        if current_entry is None:
            records.append(_record(row, "MISSING", "no-registry-entry"))
            continue
        if row["version_id"] != current_entry["version_id"]:
            records.append(_record(
                row, "STALE_VERSION",
                f"manifest-version={row['version_id']} current={current_entry['version_id']}"))
            continue
        entry = by_version[(row["key"], row["version_id"])]
        if entry["storage_class"] not in core.ARCHIVE_CLASSES:
            status = core.compare(row["size"], row["digest"], entry["size"], entry["digest"])
            records.append(_record(row, status, "direct-verify"))
            continue

        status, evidence, application, receipt, accepted_now = restore.apply(row, advance)
        request_id = application["request_id"] if application else None
        records.append(_record(row, status, evidence, request_id=request_id,
                               receipt=receipt, restore_required=True))
        if accepted_now:
            accepted_here += 1
            if interrupt_after and accepted_here >= int(interrupt_after):
                interrupted = True
                break

    valid = sum(record["status"] == "MATCH" for record in records)
    quarantine = len(records) - valid
    partitions = core.aggregate_partitions(records)
    statuses = {record["status"] for record in records}
    if interrupted:
        status, reason = "INTERRUPTED", "INTERRUPTED_AFTER_ACCEPT"
    elif "PENDING_ADJUDICATION" in statuses:
        status = reason = "PENDING_ADJUDICATION"
    elif "RESTORING" in statuses:
        status = reason = "RESTORING"
    elif "AWAITING_REQUEST" in statuses:
        status, reason = "READY", "AWAITING_REQUEST"
    elif statuses and statuses <= {"MATCH"}:
        status, reason = "READY", "OK"
    else:
        status, reason = "READY", "DIFF"

    return {
        "status": status,
        "reason": reason,
        "mode": "advance" if advance else "readback",
        "input_count": len(manifest),
        "processed_count": len(records),
        "remaining_count": len(manifest) - len(records),
        "valid_count": valid,
        "quarantine_count": quarantine,
        "balanced": len(records) == valid + quarantine,
        "restore_required_count": sum(record["restore_required"] for record in records),
        "records": records,
        "partitions": partitions,
        "applications": _readback_applications(store),
        "receipts": store.receipts(),
        "fees": _fees(store, policy),
        "storage": _storage(store),
        "policy": policy,
        "now": core.iso(now),
    }
