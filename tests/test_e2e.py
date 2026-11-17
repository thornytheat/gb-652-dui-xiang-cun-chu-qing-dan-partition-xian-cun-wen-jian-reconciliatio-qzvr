def test_all_module_pages(client):
    for path in ['/manifest', '/paths', '/objects', '/partitions', '/reconcile', '/runs', '/restore']:
        response = client.get(path)
        assert response.status_code == 200
        assert "panel" in response.text

def test_all_module_apis_and_idempotent_run(client):
    for path in ['/api/manifests/import', '/api/paths/normalize', '/api/objects/verify', '/api/partitions', '/api/reconcile']:
        response = client.post(path, json={})
        assert response.status_code == 200
    restore = client.post('/api/restore/reconcile', json={})
    assert restore.status_code == 200
    assert restore.json()["balanced"] is True
    first = client.get("/api/runs").json()
    second = client.get("/api/runs").json()
    assert len(first["runs"]) == len(second["runs"]) == 1
    assert first == second
