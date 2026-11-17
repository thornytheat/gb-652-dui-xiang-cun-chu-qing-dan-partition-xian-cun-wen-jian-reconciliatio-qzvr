"""归档恢复对账的纯逻辑：清单/登记读取、版本归集、分区统计与大小摘要比较。"""
from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from app.errors import DomainError
from app.manifest import canonical_digest
from app.modules.m2_paths.core import normalize

SAMPLE_DIR = Path(__file__).resolve().parents[3] / "sample_data"
ARCHIVE_CLASSES = {"ARCHIVE", "DEEP_ARCHIVE", "COLD_ARCHIVE"}

STATUSES = (
    "MATCH", "SIZE_MISMATCH", "DIGEST_MISMATCH", "MISSING", "RESTORING",
    "PENDING_ADJUDICATION", "STALE_VERSION", "REJECTED", "RESTORE_FAILED",
    "AWAITING_REQUEST", "DUPLICATE",
)


def load_manifest(path: str | Path | None = None) -> list[dict]:
    source = Path(path) if path else SAMPLE_DIR / "restore_manifest.csv"
    rows = []
    for line_no, row in enumerate(csv.DictReader(io.StringIO(source.read_text(encoding="utf-8"))), 2):
        rows.append({
            "key": (row.get("key") or "").strip(),
            "size": int(row["size"]),
            "digest": (row.get("digest") or "").strip(),
            "version_id": (row.get("version_id") or "").strip(),
            "source_line": line_no,
        })
    return rows


def load_objects(path: str | Path | None = None) -> list[dict]:
    source = Path(path) if path else SAMPLE_DIR / "restore_objects.jsonl"
    rows = []
    for line_no, line in enumerate(source.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        rows.append({
            "key": value["key"],
            "version_id": str(value["version_id"]),
            "size": int(value["size"]),
            "digest": value["digest"],
            "storage_class": str(value.get("storage_class", "STANDARD")).upper(),
            "is_current": bool(value.get("is_current", False)),
            "source_line": line_no,
        })
    return rows


def parse_time(value) -> datetime:
    """时间值必须带偏移；无值按当前时刻，供测试显式推进。"""
    if value is None or value == "":
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    try:
        result = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainError("INVALID_TIME", f"时间值无法解析：{value}") from exc
    if result.tzinfo is None:
        raise DomainError("INVALID_TIME", "时间值必须带时区偏移")
    return result.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_manifest(rows: list[dict]) -> list[dict]:
    """接受调用方直接给出的清单行，补上稳定 source_line 并校验必要字段。"""
    out = []
    for index, row in enumerate(rows, 2):
        try:
            size = int(row["size"])
        except (KeyError, TypeError, ValueError) as exc:
            raise DomainError("INVALID_MANIFEST", f"第 {index} 行 size 无法解析") from exc
        key = str(row.get("key") or "").strip()
        if not key or not row.get("digest") or not row.get("version_id"):
            raise DomainError("INVALID_MANIFEST", f"第 {index} 行缺少 key、digest 或 version_id")
        out.append({
            "key": key,
            "size": size,
            "digest": str(row["digest"]),
            "version_id": str(row["version_id"]),
            "source_line": int(row.get("source_line", index)),
        })
    return out


def normalize_objects(rows: list[dict]) -> list[dict]:
    out = []
    for index, row in enumerate(rows, 1):
        out.append({
            "key": str(row["key"]),
            "version_id": str(row["version_id"]),
            "size": int(row["size"]),
            "digest": str(row["digest"]),
            "storage_class": str(row.get("storage_class", "STANDARD")).upper(),
            "is_current": bool(row.get("is_current", False)),
            "source_line": int(row.get("source_line", index)),
        })
    return out


def partition_of(key: str) -> str:
    """沿用 M2 的规范化结果归集分区，不修改路径契约。"""
    parts = [part for part in normalize(key).split("/") if "=" in part]
    return "/".join(parts) if parts else "_root"


def request_key(key: str, version_id: str) -> str:
    """恢复申请的幂等键：同一对象版本只登记一次，重试沿用。"""
    return "rst-" + canonical_digest({"key": key, "version_id": version_id})[:16]


def _version_rank(value: str):
    match = re.search(r"(\d+)$", str(value))
    return (int(match.group(1)) if match else -1, str(value))


def current_versions(registry: list[dict]) -> dict[str, dict]:
    """每个 key 的当前版本：优先显式 is_current，否则按版本号取最大。"""
    grouped: dict[str, list[dict]] = {}
    for entry in registry:
        grouped.setdefault(entry["key"], []).append(entry)
    current = {}
    for key, entries in grouped.items():
        flagged = [entry for entry in entries if entry.get("is_current")]
        current[key] = flagged[-1] if flagged else max(entries, key=lambda item: _version_rank(item["version_id"]))
    return current


def index_versions(registry: list[dict]) -> dict[tuple[str, str], dict]:
    return {(entry["key"], entry["version_id"]): entry for entry in registry}


def compare(expected_size, expected_digest, actual_size, actual_digest) -> str:
    """恢复完成后先比大小、再比摘要，两类差异不合并。"""
    if int(expected_size) != int(actual_size):
        return "SIZE_MISMATCH"
    if str(expected_digest) != str(actual_digest):
        return "DIGEST_MISMATCH"
    return "MATCH"


def aggregate_partitions(records: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for record in records:
        grouped.setdefault(record["partition"], []).append(record)
    out = []
    for partition in sorted(grouped):
        rows = grouped[partition]
        counts = {status: 0 for status in STATUSES}
        for record in rows:
            counts[record["status"]] = counts.get(record["status"], 0) + 1
        out.append({
            "partition": partition,
            "input_count": len(rows),
            "verified_count": counts["MATCH"],
            "size_mismatch_count": counts["SIZE_MISMATCH"],
            "digest_mismatch_count": counts["DIGEST_MISMATCH"],
            "missing_count": counts["MISSING"],
            "restoring_count": counts["RESTORING"],
            "pending_adjudication_count": counts["PENDING_ADJUDICATION"],
            "stale_count": counts["STALE_VERSION"],
            "rejected_count": counts["REJECTED"],
            "restore_failed_count": counts["RESTORE_FAILED"],
            "duplicate_count": counts["DUPLICATE"],
            "balanced": sum(counts.values()) == len(rows),
        })
    return out
