"""수신 앱(ingest_app) — 수신 규약(재전송 갱신·중복·manifest 필수·원자적 저장)과 운영 엔드포인트.

러너는 가짜(트리거 기록만). 변환·적재는 돌지 않는다. fastapi 가 없는 환경(운영 적재기 venv)에선 건너뛴다.
"""
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from law_indexer import ingest_app as I  # noqa: E402
from law_indexer.auto_ingest import IngestPaths, write_json  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "inbox"


class FakeRunner:
    """스케줄러 인터페이스(start/stop/halted/trigger/status/runs/resume/requeue)의 가짜 — 트리거만 기록."""

    mode = "fake"

    def __init__(self):
        self.triggers, self.halt, self.started = [], None, False
        self.requeued = []

    async def start(self):
        self.started = True
        self.triggers.append("startup")

    async def stop(self):
        pass

    async def halted(self):
        return self.halt is not None

    async def trigger(self, reason):
        if self.halt:
            return False
        self.triggers.append(reason)
        return True

    async def status(self):
        return {"mode": "fake", "state": "halted" if self.halt else "idle", "halt": self.halt, "last_run": None}

    async def runs(self, limit):
        return [{"id": "b", "verdict": "ok"}, {"id": "a", "verdict": "halt"}][:limit]

    async def resume(self):
        if self.halt is None:
            return False
        self.halt = None
        self.triggers.append("resume")
        return True

    async def requeue(self, name, stage):
        if name == "missing.zip":
            raise FileNotFoundError(name)
        self.requeued.append((name, stage))
        return [name]


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("LAW_OPS_TOKEN", "secret")
    paths = IngestPaths(tmp_path / "root").ensure()
    runner = FakeRunner()
    with TestClient(I.create_app(paths, runner)) as client:
        yield client, paths, runner


def _zip_bytes(members):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in members.items():
            z.writestr(name, data)
    return buf.getvalue()


def _post(client, name, data, manifest=None):
    files = {"file": (name, data, "application/zip")}
    if manifest is not None:
        files["manifest"] = (name.replace(".zip", ".manifest.json"),
                             json.dumps(manifest).encode("utf-8"), "application/json")
    return client.post("/api/v1/law/ingest", files=files)


def _manifest(data, **over):
    mf = {"file": "batch.zip", "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
          "documents": 1, "docs": [{"id": "001", "mst": "1", "dir": "law/x"}]}
    mf.update(over)
    return mf


# ── 수신 규약 ───────────────────────────────────────────────────────────

def test_startup_triggers_catch_up_sweep(env):
    client, paths, runner = env
    assert runner.started and runner.triggers == ["startup"]
    assert client.get("/health").json() == {"status": "ok"}


def test_accept_stores_pair_with_same_stamp_and_triggers(env):
    client, paths, runner = env
    data = _zip_bytes({"law/x/x.json": "{}", "law/x/별표/[별표 1] 가.hwp": "h"})
    r = _post(client, "batch-20261101-001.zip", data, _manifest(data))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["received"] and body["stored"] and not body["duplicate"]
    assert body["sha256"] == hashlib.sha256(data).hexdigest() and body["size_bytes"] == len(data)
    assert body["zip"]["json"] == 1 and body["zip"]["attachments"] == 1 and body["zip"]["top_level_sample"] == ["law"]
    stored = body["stored_as"]
    assert stored.endswith("_batch-20261101-001.zip")
    stem = stored[:-4]
    assert (paths.inbox / stored).read_bytes() == data
    assert json.loads((paths.inbox / f"{stem}.manifest.json").read_text(encoding="utf-8"))["docs"][0]["mst"] == "1"
    assert not list(paths.inbox.glob(".*"))                       # .part·.tmp 잔존 없음
    assert runner.triggers == ["startup", "ingest"] and body["triggered"] is True
    assert body["pipeline"] == {"mode": "fake", "halted": False}


def test_same_bytes_again_is_duplicate_not_stored(env):
    """같은 ZIP 재전송은 안전하다 — 200 duplicate, 저장·트리거 없음."""
    client, paths, runner = env
    data = _zip_bytes({"law/x/x.json": "{}"})
    first = _post(client, "batch.zip", data, _manifest(data)).json()
    again = _post(client, "batch.zip", data, _manifest(data))
    assert again.status_code == 200
    body = again.json()
    assert body["duplicate"] and not body["stored"] and body["first_stored_as"] == first["stored_as"]
    assert len(list(paths.inbox.glob("*.zip"))) == 1 and runner.triggers.count("ingest") == 1


def test_modified_resend_with_same_name_is_a_new_delivery(env):
    """내용을 고쳐 같은 이름으로 다시 보내면 새 수신 — 접두가 달라 뒤에 처리된다(나중 것이 최종)."""
    client, paths, runner = env
    a = _zip_bytes({"law/x/x.json": '{"v": 1}'})
    b = _zip_bytes({"law/x/x.json": '{"v": 2}'})
    first = _post(client, "batch-001.zip", a, _manifest(a)).json()
    second = _post(client, "batch-001.zip", b, _manifest(b)).json()
    assert second["stored"] and not second["duplicate"]
    assert first["stored_as"] != second["stored_as"] and first["stored_as"] < second["stored_as"]
    assert sorted(p.name for p in paths.inbox.glob("*.zip")) == [first["stored_as"], second["stored_as"]]


def test_missing_manifest_is_rejected_but_bytes_kept(env):
    client, paths, runner = env
    data = _zip_bytes({"law/x/x.json": "{}"})
    r = _post(client, "batch.zip", data)
    assert r.status_code == 422 and "manifest" in r.json()["error"]
    assert list(paths.inbox.glob("*.zip")) == [] and len(list(paths.rejected.glob("*.zip"))) == 1
    assert "ingest" not in runner.triggers


def test_sha_mismatch_and_bad_zip_are_rejected(env):
    client, paths, runner = env
    data = _zip_bytes({"law/x/x.json": "{}"})
    r = _post(client, "batch.zip", data, _manifest(data, sha256="0" * 64))
    assert r.status_code == 422 and "sha256" in r.json()["error"]
    r = _post(client, "broken.zip", b"not a zip", _manifest(b"not a zip"))
    assert r.status_code == 422 and r.json()["zip"]["valid"] is False
    assert len(list(paths.rejected.glob("*.zip"))) == 2 and list(paths.inbox.glob("*.zip")) == []


def test_rejected_bytes_do_not_count_as_seen(env):
    """거부된 전송의 바이트를 고쳐서(manifest 를 맞춰) 다시 보내면 정상 수신된다."""
    client, paths, runner = env
    data = _zip_bytes({"law/x/x.json": "{}"})
    assert _post(client, "batch.zip", data).status_code == 422          # manifest 없음
    assert _post(client, "batch.zip", data, _manifest(data)).json()["stored"] is True


def test_ops_upload_goes_to_ops_folder_untouched(env):
    """배포 번들·lib 전달 통로 — ZIP 검사·manifest·트리거 없이 ops/ 에 그대로 둔다."""
    client, paths, runner = env
    r = client.post("/api/v1/law/ingest?kind=ops", files={"file": ("lib.tar.gz", b"\x1f\x8bnot-really", "application/gzip")})
    assert r.status_code == 200 and r.json()["kind"] == "ops"
    stored = paths.ops / r.json()["stored_as"]
    assert stored.read_bytes() == b"\x1f\x8bnot-really" and stored.name.endswith("_lib.tar.gz")
    assert list(paths.inbox.glob("*")) == [p for p in paths.inbox.glob("*") if p.is_dir()]   # inbox 에 파일 없음
    assert "ingest" not in runner.triggers


# ── 운영 엔드포인트 ─────────────────────────────────────────────────────

def test_ops_endpoints_require_token(env):
    client, paths, runner = env
    assert client.post("/api/v1/pipeline/run").status_code == 403
    assert client.post("/api/v1/pipeline/run", headers={"X-Law-Ops-Token": "wrong"}).status_code == 403
    r = client.post("/api/v1/pipeline/run", headers={"X-Law-Ops-Token": "secret"})
    assert r.status_code == 202 and runner.triggers[-1] == "manual"


def test_status_runs_and_resume(env):
    client, paths, runner = env
    runner.halt = {"at": "x", "reasons": ["zip_failed:1"]}
    assert client.get("/api/v1/pipeline/status").json()["state"] == "halted"
    data = _zip_bytes({"law/x/x.json": "{}"})
    body = _post(client, "while-halted.zip", data, _manifest(data)).json()
    assert body["stored"] and body["triggered"] is False and body["pipeline"]["halted"] is True   # 받되 돌리지 않는다
    runs = client.get("/api/v1/pipeline/runs?limit=5").json()["runs"]
    assert [r["id"] for r in runs] == ["b", "a"]
    r = client.post("/api/v1/pipeline/resume", headers={"X-Law-Ops-Token": "secret"})
    assert r.json()["cleared"] is True and runner.triggers[-1] == "resume"
    assert client.get("/api/v1/pipeline/status").json()["state"] == "idle"


def test_local_scheduler_lists_run_records(tmp_path):
    """Temporal 없는 모드 — runs/ 파일을 최근 것부터, latest.json 은 제외."""
    import asyncio
    paths = IngestPaths(tmp_path / "root").ensure()
    write_json(paths.runs / "20261101T000000Z_ingest.json", {"id": "a", "verdict": "halt"})
    write_json(paths.runs / "20261101T010000Z_resume.json", {"id": "b", "verdict": "ok"})
    write_json(paths.latest, {"id": "b", "verdict": "ok"})

    class _R:
        def runs(self):
            return None

    sched = I.LocalScheduler(paths, _R())
    assert [r["id"] for r in asyncio.run(sched.runs(5))] == ["b", "a"]


def test_env_file_fills_missing_keys_only(tmp_path, monkeypatch):
    """NFS 의 .env 로 나머지 설정을 채우되, UI 가 준 환경변수는 덮지 않는다."""
    env_file = tmp_path / "ops.env"
    env_file.write_text("WEAVIATE_HTTP_HOST=from-file\nLAW_COLLECTION=C1\n# 주석\nEMPTY=\n", encoding="utf-8")
    monkeypatch.setenv("WEAVIATE_HTTP_HOST", "from-ui")
    monkeypatch.delenv("LAW_COLLECTION", raising=False)
    assert I.load_env_file(str(env_file)) == 2                       # LAW_COLLECTION, EMPTY
    import os
    assert os.environ["WEAVIATE_HTTP_HOST"] == "from-ui" and os.environ["LAW_COLLECTION"] == "C1"
    assert I.load_env_file(None) == 0


def test_requeue_endpoint(env):
    client, paths, runner = env
    h = {"X-Law-Ops-Token": "secret"}
    assert client.post("/api/v1/pipeline/requeue?zip=b1.zip").status_code == 403
    r = client.post("/api/v1/pipeline/requeue?zip=b1.zip&stage=consume", headers=h)
    assert r.status_code == 200 and r.json()["moved"] == ["b1.zip"] and runner.requeued == [("b1.zip", "consume")]
    assert client.post("/api/v1/pipeline/requeue?zip=missing.zip", headers=h).status_code == 404


def test_real_delivery_fixture_round_trips(env):
    """투라인 실제 배달분 — manifest 의 sha256·bytes 가 받은 바이트와 맞아 그대로 수신된다."""
    client, paths, runner = env
    data = (FIX / "batch-20261021-001.zip").read_bytes()
    mf = json.loads((FIX / "batch-20261021-001.manifest.json").read_text(encoding="utf-8"))
    r = _post(client, "batch-20261021-001.zip", data, mf)
    assert r.status_code == 200 and r.json()["manifest"] == {"documents": 1, "docs": 1}
