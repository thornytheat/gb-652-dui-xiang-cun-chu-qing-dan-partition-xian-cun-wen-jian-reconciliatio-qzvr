from __future__ import annotations
import json
from pathlib import Path
import sqlite3

SCHEMA = (
    "CREATE TABLE IF NOT EXISTS runs (digest TEXT PRIMARY KEY, run_id TEXT NOT NULL, result TEXT NOT NULL)",
    """CREATE TABLE IF NOT EXISTS storage_jobs (
        idempotency_key TEXT PRIMARY KEY,
        request_id TEXT NOT NULL UNIQUE,
        key TEXT NOT NULL,
        version_id TEXT NOT NULL,
        policy TEXT NOT NULL,
        accepted_at TEXT NOT NULL,
        rejected INTEGER NOT NULL DEFAULT 0,
        outcome TEXT,
        result_size INTEGER,
        result_digest TEXT,
        result_version TEXT,
        complete_at TEXT,
        delivery_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS restore_applications (
        idempotency_key TEXT PRIMARY KEY,
        request_id TEXT,
        key TEXT NOT NULL,
        version_id TEXT NOT NULL,
        manifest_size INTEGER NOT NULL,
        manifest_digest TEXT NOT NULL,
        state TEXT NOT NULL,
        receipt_lost INTEGER NOT NULL DEFAULT 0,
        fee_charged INTEGER NOT NULL DEFAULT 0,
        accepted_at TEXT,
        deadline_at TEXT,
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS restore_receipts (
        request_id TEXT PRIMARY KEY,
        key TEXT NOT NULL,
        version_id TEXT NOT NULL,
        outcome TEXT NOT NULL,
        result_size INTEGER,
        result_digest TEXT,
        completed_at TEXT NOT NULL,
        received_at TEXT NOT NULL,
        late INTEGER NOT NULL DEFAULT 0,
        source TEXT NOT NULL
    )""",
)

APP_FIELDS = ("idempotency_key", "request_id", "key", "version_id", "manifest_size",
              "manifest_digest", "state", "receipt_lost", "fee_charged", "accepted_at",
              "deadline_at", "created_at")
JOB_FIELDS = ("idempotency_key", "request_id", "key", "version_id", "policy", "accepted_at",
              "rejected", "outcome", "result_size", "result_digest", "result_version",
              "complete_at", "delivery_count", "created_at")
RECEIPT_FIELDS = ("request_id", "key", "version_id", "outcome", "result_size", "result_digest",
                  "completed_at", "received_at", "late", "source")


def _row_to_dict(fields, row):
    return dict(zip(fields, row))


class Store:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            for statement in SCHEMA:
                db.execute(statement)

    def connect(self):
        return sqlite3.connect(self.path)

    def record(self, digest: str, run_id: str, result: dict) -> dict:
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO runs VALUES (?, ?, ?)",
                       (digest, run_id, json.dumps(result, ensure_ascii=False, sort_keys=True)))
            row = db.execute("SELECT run_id, result FROM runs WHERE digest=?", (digest,)).fetchone()
        return {"run_id": row[0], "result": json.loads(row[1])}

    def count(self) -> int:
        with self.connect() as db:
            return db.execute("SELECT COUNT(*) FROM runs").fetchone()[0]

    def all(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT run_id, digest, result FROM runs ORDER BY run_id").fetchall()
        return [{"run_id": r[0], "digest": r[1], "result": json.loads(r[2])} for r in rows]

    # --- 本地存储服务替身：受理、查询与回执 ---------------------------------

    def create_job(self, job: dict) -> dict:
        with self.connect() as db:
            db.execute(
                "INSERT INTO storage_jobs (idempotency_key, request_id, key, version_id, policy, "
                "accepted_at, rejected, outcome, result_size, result_digest, result_version, "
                "complete_at, delivery_count, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (job["idempotency_key"], job["request_id"], job["key"], job["version_id"],
                 json.dumps(job.get("policy") or {}, ensure_ascii=False, sort_keys=True),
                 job["accepted_at"], int(job.get("rejected", False)), job.get("outcome"),
                 job.get("result_size"), job.get("result_digest"), job.get("result_version"),
                 job.get("complete_at"), int(job.get("delivery_count", 0)), job["created_at"]))
        return self.get_job(job["idempotency_key"])

    def get_job(self, idempotency_key: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT %s FROM storage_jobs WHERE idempotency_key=?"
                             % ",".join(JOB_FIELDS), (idempotency_key,)).fetchone()
        if not row:
            return None
        job = _row_to_dict(JOB_FIELDS, row)
        job["policy"] = json.loads(job["policy"])
        return job

    def get_job_by_request(self, request_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT %s FROM storage_jobs WHERE request_id=?"
                             % ",".join(JOB_FIELDS), (request_id,)).fetchone()
        if not row:
            return None
        job = _row_to_dict(JOB_FIELDS, row)
        job["policy"] = json.loads(job["policy"])
        return job

    def bump_delivery(self, idempotency_key: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE storage_jobs SET delivery_count = delivery_count + 1 "
                       "WHERE idempotency_key=?", (idempotency_key,))

    def jobs(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT %s FROM storage_jobs ORDER BY accepted_at, idempotency_key"
                              % ",".join(JOB_FIELDS)).fetchall()
        out = []
        for row in rows:
            job = _row_to_dict(JOB_FIELDS, row)
            job["policy"] = json.loads(job["policy"])
            out.append(job)
        return out

    # --- 恢复申请 -----------------------------------------------------------

    def save_application(self, app: dict) -> dict:
        with self.connect() as db:
            existing = db.execute("SELECT idempotency_key FROM restore_applications WHERE idempotency_key=?",
                                  (app["idempotency_key"],)).fetchone()
            if existing:
                db.execute("UPDATE restore_applications SET request_id=?, state=?, receipt_lost=?, "
                           "fee_charged=?, accepted_at=?, deadline_at=? WHERE idempotency_key=?",
                           (app.get("request_id"), app["state"], int(app.get("receipt_lost", False)),
                            int(app.get("fee_charged", 0)), app.get("accepted_at"),
                            app.get("deadline_at"), app["idempotency_key"]))
            else:
                db.execute(
                    "INSERT INTO restore_applications (%s) VALUES (%s)"
                    % (",".join(APP_FIELDS), ",".join("?" * len(APP_FIELDS))),
                    tuple(int(app[f]) if f == "receipt_lost" or f == "fee_charged"
                          else app.get(f) for f in APP_FIELDS))
        return self.get_application(app["idempotency_key"])

    def get_application(self, idempotency_key: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT %s FROM restore_applications WHERE idempotency_key=?"
                             % ",".join(APP_FIELDS), (idempotency_key,)).fetchone()
        return _row_to_dict(APP_FIELDS, row) if row else None

    def get_application_by_request(self, request_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT %s FROM restore_applications WHERE request_id=?"
                             % ",".join(APP_FIELDS), (request_id,)).fetchone()
        return _row_to_dict(APP_FIELDS, row) if row else None

    def applications(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT %s FROM restore_applications ORDER BY created_at, idempotency_key"
                              % ",".join(APP_FIELDS)).fetchall()
        return [_row_to_dict(APP_FIELDS, row) for row in rows]

    def fee_total(self) -> int:
        with self.connect() as db:
            return db.execute("SELECT COALESCE(SUM(fee_charged), 0) FROM restore_applications").fetchone()[0]

    # --- 回执 ---------------------------------------------------------------

    def save_receipt(self, receipt: dict) -> dict:
        with self.connect() as db:
            db.execute(
                "INSERT INTO restore_receipts (%s) VALUES (%s) ON CONFLICT(request_id) DO UPDATE SET "
                "outcome=excluded.outcome, result_size=excluded.result_size, "
                "result_digest=excluded.result_digest, completed_at=excluded.completed_at, "
                "received_at=excluded.received_at, late=excluded.late, source=excluded.source"
                % (",".join(RECEIPT_FIELDS), ",".join("?" * len(RECEIPT_FIELDS))),
                tuple(int(receipt[f]) if f == "late" else receipt.get(f) for f in RECEIPT_FIELDS))
        return self.get_receipt(receipt["request_id"])

    def get_receipt(self, request_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT %s FROM restore_receipts WHERE request_id=?"
                             % ",".join(RECEIPT_FIELDS), (request_id,)).fetchone()
        return _row_to_dict(RECEIPT_FIELDS, row) if row else None

    def receipts(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT %s FROM restore_receipts ORDER BY received_at, request_id"
                              % ",".join(RECEIPT_FIELDS)).fetchall()
        return [_row_to_dict(RECEIPT_FIELDS, row) for row in rows]
