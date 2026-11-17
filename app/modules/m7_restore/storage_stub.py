"""本地存储服务替身：复现恢复申请的受理、查询、完成与回执丢失。

不连接真实存储；状态全部落在同一 SQLite 中，便于测试中断、重试和迟到回执。
受理以幂等键为准：重复提交返回原受理记录，不重复计费、不重复计数。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.manifest import canonical_digest


def request_id_for(idempotency_key: str) -> str:
    return "req-" + canonical_digest({"idempotency": idempotency_key})[:16]


def selected(selector: str, key: str, version_id: str) -> bool:
    return selector in (key, f"{key}@{version_id}")


def _any_selected(selectors, key: str, version_id: str) -> bool:
    return any(selected(item, key, version_id) for item in selectors or [])


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


class StorageStub:
    def __init__(self, store, simulation: dict | None, deliver_receipt: bool, now: datetime):
        self.store = store
        self.simulation = simulation or {}
        self.deliver_receipt = deliver_receipt
        self.now = now

    def _result(self, application: dict) -> dict:
        key, version = application["key"], application["version_id"]
        result = {"size": application["manifest_size"], "digest": application["manifest_digest"],
                  "version_id": version}
        for selector, override in (self.simulation.get("results") or {}).items():
            if selected(selector, key, version):
                result.update(override or {})
                break
        return result

    def accept(self, application: dict) -> dict:
        """受理恢复申请；幂等键已存在时直接返回原受理记录，不重复计费。"""
        existing = self.store.get_job(application["idempotency_key"])
        if existing:
            return existing
        key, version = application["key"], application["version_id"]
        rejected = _any_selected(self.simulation.get("reject"), key, version)
        pending = _any_selected(self.simulation.get("pending"), key, version)
        failed = _any_selected(self.simulation.get("failed"), key, version)
        late = _any_selected(self.simulation.get("late_receipt"), key, version)
        lost = _any_selected(self.simulation.get("lost_receipt"), key, version)
        accepted_at = utc(application["accepted_at"])
        deadline_at = utc(application["deadline_at"])
        result = self._result(application)

        outcome = None
        complete_at = None
        if rejected:
            outcome, complete_at = "rejected", accepted_at
        elif pending:
            outcome = None
        elif failed:
            outcome, complete_at = "failed", accepted_at
        elif late:
            outcome = "completed"
            complete_at = deadline_at + timedelta(seconds=int(self.simulation.get("late_grace_seconds", 60)))
        else:
            outcome, complete_at = "completed", accepted_at

        done = outcome in ("rejected", "failed") or outcome is None
        return self.store.create_job({
            "idempotency_key": application["idempotency_key"],
            "request_id": request_id_for(application["idempotency_key"]),
            "key": key,
            "version_id": version,
            "policy": {
                "deliver_receipt": self.deliver_receipt,
                "lost_receipt": lost,
                "late_receipt": late,
            },
            "accepted_at": application["accepted_at"],
            "rejected": rejected,
            "outcome": outcome,
            "result_size": None if done else result["size"],
            "result_digest": None if done else result["digest"],
            "result_version": None if done else result["version_id"],
            "complete_at": complete_at.isoformat() if complete_at else None,
            "delivery_count": 0,
            "created_at": self.now.isoformat(),
        })
    def query(self, idempotency_key: str) -> dict | None:
        return self.store.get_job(idempotency_key)

    def deliver(self, job: dict, deadline_at: datetime) -> dict | None:
        """尝试投递回执。丢失、未完成或尚未到完成时间时不产生回执。"""
        existing = self.store.get_receipt(job["request_id"])
        if existing:
            return existing
        policy = job["policy"]
        if job["rejected"] or job["outcome"] is None:
            return None
        if job["outcome"] == "completed" and job["result_size"] is None:
            return None
        if not policy.get("deliver_receipt", True) or policy.get("lost_receipt"):
            return None
        complete_at = utc(job["complete_at"]) if job["complete_at"] else None
        if complete_at is None or complete_at > self.now:
            return None
        receipt = self.store.save_receipt({
            "request_id": job["request_id"],
            "key": job["key"],
            "version_id": job["result_version"] or job["version_id"],
            "outcome": job["outcome"],
            "result_size": job["result_size"],
            "result_digest": job["result_digest"],
            "completed_at": complete_at.isoformat(),
            "received_at": self.now.isoformat(),
            "late": self.now > deadline_at,
            "source": "delivery",
        })
        self.store.bump_delivery(job["idempotency_key"])
        return receipt
