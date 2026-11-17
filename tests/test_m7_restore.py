import tempfile

from app.modules.m7_restore import core, service
from app.store import Store


def new_store():
    return Store(str(tempfile.mktemp(suffix=".db")))


def by_key(result, suffix):
    return next(record for record in result["records"] if record["key"].endswith(suffix))


def test_sample_accounting_balanced():
    result = service.reconcile({}, new_store())
    assert result["input_count"] == result["valid_count"] + result["quarantine_count"]
    assert result["balanced"] is True
    assert result["processed_count"] == result["input_count"]
    assert result["remaining_count"] == 0


def test_partitions_sum_to_records():
    result = service.reconcile({}, new_store())
    assert sum(part["input_count"] for part in result["partitions"]) == result["processed_count"]
    assert all(part["balanced"] for part in result["partitions"])
    assert result["valid_count"] == sum(part["verified_count"] for part in result["partitions"])


def test_sample_covers_every_status():
    result = service.reconcile({}, new_store())
    statuses = {record["status"] for record in result["records"]}
    assert {"MATCH", "SIZE_MISMATCH", "DIGEST_MISMATCH", "MISSING", "RESTORING",
            "PENDING_ADJUDICATION", "STALE_VERSION", "REJECTED", "RESTORE_FAILED",
            "DUPLICATE"} <= statuses


def test_size_checked_before_digest():
    assert core.compare(10, "a", 11, "b") == "SIZE_MISMATCH"
    assert core.compare(10, "a", 10, "b") == "DIGEST_MISMATCH"
    assert core.compare(10, "a", 10, "a") == "MATCH"


def test_missing_object_is_not_pending_adjudication():
    result = service.reconcile({}, new_store())
    missing = by_key(result, "hour=03/g.parquet")
    assert missing["status"] == "MISSING"
    assert missing["request_id"] is None


def test_pending_adjudication_does_not_masquerade():
    result = service.reconcile({}, new_store())
    pending = by_key(result, "hour=01/b2.parquet")
    assert pending["status"] == "PENDING_ADJUDICATION"
    assert pending["status"] not in {"MATCH", "MISSING"}


def test_stale_manifest_version_is_flagged():
    result = service.reconcile({}, new_store())
    stale_rows = [r for r in result["records"]
                  if r["key"].endswith("hour=00/a.parquet") and r["version_id"] == "v1"]
    assert stale_rows[0]["status"] == "STALE_VERSION"


def test_old_version_receipt_does_not_overwrite_new():
    result = service.reconcile({}, new_store())
    row = by_key(result, "hour=04/i.parquet")
    assert row["version_id"] == "v2"
    assert row["status"] == "STALE_VERSION"
    assert "receipt-version=v1" in row["evidence"]


def test_restore_result_compares_after_completion():
    result = service.reconcile({}, new_store())
    assert by_key(result, "hour=00/a.parquet")["status"] == "SIZE_MISMATCH"


def test_late_receipt_attaches_to_original_request():
    result = service.reconcile({}, new_store())
    late = by_key(result, "hour=02/c.parquet")
    assert late["status"] == "MATCH"
    assert late["receipt_late"] is True
    app = next(a for a in result["applications"] if a["key"] == late["key"])
    assert app["request_id"] == late["request_id"]
    assert app["state"] == "COMPLETED_LATE"


def test_retry_reuses_request_and_does_not_double_bill():
    store = new_store()
    first = service.reconcile({}, store)
    second = service.reconcile({}, store)
    assert first["fees"] == second["fees"]
    assert [a["request_id"] for a in first["applications"]] == [a["request_id"] for a in second["applications"]]
    assert first["fees"]["application_count"] == len(first["applications"])


def test_billing_counts_only_accepted_requests():
    result = service.reconcile({}, new_store())
    rejected = [a for a in result["applications"] if a["state"] == "REJECTED"]
    assert rejected and all(a["fee_charged"] == 0 for a in rejected)
    assert result["fees"]["billed_count"] == result["fees"]["application_count"] - len(rejected)


def test_interrupt_after_accept_resumes_without_duplicate():
    store = new_store()
    first = service.reconcile({"simulation": {"interrupt_after_accepts": 2}}, store)
    assert first["status"] == "INTERRUPTED"
    assert first["remaining_count"] > 0
    assert first["fees"]["application_count"] == 2
    second = service.reconcile({"simulation": {"interrupt_after_accepts": 2}}, store)
    assert second["processed_count"] > first["processed_count"]
    # 已受理的两条被续接而非重提：本轮只新增 2 次申请
    assert second["fees"]["application_count"] == 4
    final = service.reconcile({}, store)
    assert final["processed_count"] == final["input_count"]
    assert final["fees"]["application_count"] == len(final["applications"]) == 9
    assert final["fees"]["billed_count"] == 8


def test_readback_does_not_submit_new_requests():
    store = new_store()
    service.reconcile({}, store)
    before = service.reconcile({}, store, advance=False)
    after = service.reconcile({}, store, advance=False)
    assert before["fees"] == after["fees"]
    assert len(before["applications"]) == len(after["applications"])


def test_readback_before_any_application_shows_awaiting():
    result = service.reconcile({}, new_store(), advance=False)
    statuses = {record["status"] for record in result["records"]}
    assert "AWAITING_REQUEST" in statuses
    assert result["fees"]["application_count"] == 0


def test_duplicate_manifest_row_is_isolated():
    result = service.reconcile({}, new_store())
    duplicates = [r for r in result["records"] if r["status"] == "DUPLICATE"]
    assert len(duplicates) == 1
    assert duplicates[0]["evidence"] == "duplicate-manifest-row"


def test_receipt_lost_stays_restoring_before_deadline():
    result = service.reconcile({}, new_store())
    lost = by_key(result, "hour=01/b.parquet")
    assert lost["status"] == "RESTORING"
    assert lost["receipt_late"] is False


def test_advancing_clock_turns_lost_receipt_into_pending():
    store = new_store()
    service.reconcile({}, store)
    later = service.reconcile({"now": "2026-11-17T12:00:00+08:00"}, store)
    assert by_key(later, "hour=01/b.parquet")["status"] == "PENDING_ADJUDICATION"
    assert by_key(later, "hour=03/e.parquet")["status"] == "RESTORE_FAILED"


def test_partition_rules_follow_m2():
    assert core.partition_of("dt=2026-11-15/hour=00/a.parquet") == "dt=2026-11-15/hour=00"
    assert core.partition_of("lake/_SUCCESS") == "_root"


def test_version_selection_prefers_current_flag():
    registry = [
        {"key": "k", "version_id": "v1", "is_current": False},
        {"key": "k", "version_id": "v2", "is_current": True},
    ]
    assert core.current_versions(registry)["k"]["version_id"] == "v2"


def test_receipts_keep_version_and_late_flag():
    result = service.reconcile({}, new_store())
    assert all(receipt["version_id"] for receipt in result["receipts"])
    assert any(receipt["late"] for receipt in result["receipts"])


def test_fee_total_matches_rate_times_billed():
    result = service.reconcile({}, new_store())
    fees = result["fees"]
    assert fees["fee_total"] == fees["rate"] * fees["billed_count"]


def test_store_accept_is_idempotent_by_key():
    store = new_store()
    service.reconcile({}, store)
    jobs = store.jobs()
    assert len({job["idempotency_key"] for job in jobs}) == len(jobs)
    assert all(job["delivery_count"] <= 1 for job in jobs)


def test_crash_after_storage_accept_is_adopted_not_resubmitted():
    """存储侧已受理、申请侧尚未记下 request_id 时重跑，应查询归并而非重提。"""
    store = new_store()
    manifest = [{"key": "dt=2026-11-15/hour=01/b.parquet", "size": 30,
                 "digest": "sha-b1", "version_id": "v1"}]
    registry = core.load_objects()
    key, version = manifest[0]["key"], manifest[0]["version_id"]

    # 模拟中断点：申请已登记为 SUBMITTED，存储侧已受理，但 request_id 未落库。
    from app.modules.m7_restore.storage_stub import StorageStub, utc
    application = store.save_application({
        "idempotency_key": core.request_key(key, version), "request_id": None,
        "key": key, "version_id": version, "manifest_size": 30, "manifest_digest": "sha-b1",
        "state": "SUBMITTED", "receipt_lost": 0, "fee_charged": 0,
        "accepted_at": "2026-11-17T01:58:20Z", "deadline_at": "2026-11-17T02:58:20Z",
        "created_at": "2026-11-17T01:58:20Z",
    })
    stub = StorageStub(store, {}, True, utc("2026-11-17T01:58:20Z"))
    accepted = stub.accept(application)

    result = service.reconcile({"manifest": manifest, "objects": registry}, store)
    adopted = next(a for a in result["applications"] if a["request_id"] == accepted["request_id"])
    assert adopted["fee_charged"] == 1
    assert result["fees"]["application_count"] == 1
    assert result["storage"]["accepted_count"] == 1


# --- TRAP 回归 ---------------------------------------------------------------

def test_trap_1_lost_receipt_retry_does_not_double_bill():
    # TRAP-27：受理后回执丢失，重发不得重复计费
    store = new_store()
    first = service.reconcile({}, store)
    second = service.reconcile({}, store)
    assert by_key(first, "hour=01/b.parquet")["status"] == "RESTORING"
    assert by_key(second, "hour=01/b.parquet")["request_id"] == by_key(first, "hour=01/b.parquet")["request_id"]
    assert second["fees"] == first["fees"]


def test_trap_2_unconfirmed_past_deadline_is_adjudicated():
    # TRAP-28：到期仍无法确认转待裁决，不冒充缺失或核验成功
    result = service.reconcile({}, new_store())
    pending = by_key(result, "hour=01/b2.parquet")
    assert pending["status"] == "PENDING_ADJUDICATION"
    assert pending["status"] not in {"MISSING", "MATCH"}


def test_trap_3_late_receipt_keeps_original_owner():
    # TRAP-29：迟到回执按原申请归属，不按当前最新版本改挂
    result = service.reconcile({}, new_store())
    late = by_key(result, "hour=02/c.parquet")
    receipt = next(r for r in result["receipts"] if r["request_id"] == late["request_id"])
    assert receipt["late"] == 1
    assert receipt["version_id"] == "v2"


def test_trap_4_old_version_result_does_not_override_new():
    # TRAP-30：旧对象版本的恢复结果不覆盖新版本
    result = service.reconcile({}, new_store())
    row = by_key(result, "hour=04/i.parquet")
    assert row["status"] == "STALE_VERSION"
    assert row["status"] != "MATCH"


def test_trap_5_compare_size_before_digest_after_restore():
    # TRAP-31：恢复完成后再比大小和摘要，且先比大小
    result = service.reconcile({}, new_store())
    size = by_key(result, "hour=00/a.parquet")
    assert size["status"] == "SIZE_MISMATCH"
    assert size["evidence"] == "restore-receipt"


def test_trap_6_interrupt_after_accept_does_not_resubmit():
    # TRAP-32：受理后中断可恢复，续跑不重复申请
    store = new_store()
    first = service.reconcile({"simulation": {"interrupt_after_accepts": 3}}, store)
    accepted = {a["request_id"] for a in first["applications"]}
    second = service.reconcile({"simulation": {"interrupt_after_accepts": 3}}, store)
    assert accepted <= {a["request_id"] for a in second["applications"]}
    assert second["fees"]["application_count"] == len(first["applications"]) + 3


def test_late_receipt_recovers_from_pending_adjudication():
    """待裁决不是终态：迟到回执到达后按原申请归回并复核。"""
    store = new_store()
    manifest = [{"key": "dt=2026-11-15/hour=02/c.parquet", "size": 20,
                 "digest": "sha-c2", "version_id": "v2"}]
    objects = [{"key": manifest[0]["key"], "version_id": "v2", "size": 20, "digest": "sha-c2",
                "storage_class": "COLD_ARCHIVE", "is_current": True}]
    simulation = {
        "late_receipt": [f"{manifest[0]['key']}@v2"],
        "accepted_offset_seconds": {f"{manifest[0]['key']}@v2": -3700},
        "late_grace_seconds": 600,
    }

    pending = service.reconcile({"manifest": manifest, "objects": objects, "simulation": simulation}, store)
    assert pending["records"][0]["status"] == "PENDING_ADJUDICATION"
    request_id = pending["records"][0]["request_id"]

    recovered = service.reconcile({
        "manifest": manifest, "objects": objects, "simulation": simulation,
        "now": "2026-11-17T10:10:00+08:00",
    }, store)
    record = recovered["records"][0]
    assert record["status"] == "MATCH"
    assert record["request_id"] == request_id
    assert record["receipt_late"] is True
    assert recovered["fees"] == pending["fees"]


def test_new_current_version_gets_fresh_request_not_old_receipt():
    """登记切换到新版本后，旧申请与旧回执留在原版本，不被新版本沿用。"""
    store = new_store()
    key = "dt=2026-11-15/hour=04/i.parquet"
    manifest = [{"key": key, "size": 100, "digest": "sha-i2", "version_id": "v1"}]
    old_objects = [{"key": key, "version_id": "v1", "size": 100, "digest": "sha-i2",
                    "storage_class": "ARCHIVE", "is_current": True}]
    new_objects = [{"key": key, "version_id": "v2", "size": 200, "digest": "sha-i2b",
                    "storage_class": "ARCHIVE", "is_current": True}]

    first = service.reconcile({"manifest": manifest, "objects": old_objects}, store)
    old_request = first["applications"][0]["request_id"]
    assert first["records"][0]["status"] == "MATCH"

    second = service.reconcile({"manifest": manifest, "objects": new_objects}, store)
    assert second["records"][0]["status"] == "STALE_VERSION"
    # 只有首个 v1 申请被计费，旧回执未挂到新版本
    assert second["fees"]["application_count"] == 1
    assert second["applications"][0]["request_id"] == old_request
    assert second["applications"][0]["version_id"] == "v1"
