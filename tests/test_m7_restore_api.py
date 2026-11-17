from app.modules.m7_restore import core

ARCHIVE_UNAVAILABLE = "DEEP_ARCHIVE"


def test_loaders_read_actual_manifest_and_registry():
    manifest = core.load_manifest()
    registry = core.load_objects()
    assert len(manifest) == 14
    assert {row["source_line"] for row in manifest} >= {2, 15}
    assert any(entry["version_id"] == "v2" for entry in registry)


def test_manifest_and_registry_drive_reconcile(client):
    manifest = core.load_manifest()
    manifest[0]["size"] = 999
    digest = core.canonical_digest(manifest)
    response = client.post("/api/restore/reconcile", json={"manifest": manifest})
    assert response.status_code == 200
    records = response.json()["records"]
    assert records[0]["key"] == manifest[0]["key"]
    assert core.canonical_digest(manifest) == digest


def test_page_renders_restore_panel(client):
    response = client.get("/restore")
    assert response.status_code == 200
    assert "恢复对账记录" in response.text


def test_readback_api_is_stable_and_does_not_submit(client):
    empty = client.get("/api/restore/reconcile").json()
    assert empty["mode"] == "readback"
    assert empty["fees"]["application_count"] == 0
    first = client.post("/api/restore/reconcile", json={}).json()
    second = client.get("/api/restore/reconcile").json()
    third = client.get("/api/restore/reconcile").json()
    assert first["fees"] == second["fees"] == third["fees"]
    assert second["applications"] == third["applications"]


def test_api_reports_fees_partitions_and_applications(client):
    body = client.post("/api/restore/reconcile", json={}).json()
    assert body["fees"]["fee_total"] == body["fees"]["rate"] * body["fees"]["billed_count"]
    assert sum(p["input_count"] for p in body["partitions"]) == body["processed_count"]
    assert body["applications"]
    assert all("state" in app and "request_id" in app for app in body["applications"])


def test_api_retry_does_not_duplicate_applications(client):
    first = client.post("/api/restore/reconcile", json={}).json()
    second = client.post("/api/restore/reconcile", json={}).json()
    assert first["fees"] == second["fees"]
    assert [a["request_id"] for a in first["applications"]] == [a["request_id"] for a in second["applications"]]


def test_manifest_without_restore_objects_marks_missing(client):
    manifest = [{"key": "dt=2026-11-15/hour=09/z.parquet", "size": 5, "digest": "sha-z", "version_id": "v1"}]
    body = client.post("/api/restore/reconcile", json={"manifest": manifest, "objects": []}).json()
    assert body["records"][0]["status"] == "MISSING"
    assert body["fees"]["application_count"] == 0


def test_standard_class_object_is_verified_directly(client):
    manifest = [{"key": "k", "size": 5, "digest": "s", "version_id": "v1"}]
    objects = [{"key": "k", "version_id": "v1", "size": 5, "digest": "s",
                "storage_class": "STANDARD", "is_current": True}]
    body = client.post("/api/restore/reconcile", json={"manifest": manifest, "objects": objects}).json()
    assert body["records"][0]["status"] == "MATCH"
    assert body["fees"]["application_count"] == 0


def test_invalid_time_offset_is_rejected(client):
    response = client.post("/api/restore/reconcile", json={"now": "2026-11-17T10:00:00"})
    assert response.status_code == 422
    assert response.json()["error"] == "INVALID_TIME"
