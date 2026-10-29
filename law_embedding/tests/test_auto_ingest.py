"""러너(auto_ingest) — 판정·HALT·lease·실행 기록·재개. 변환·소비는 가짜 함수로 주입한다(외부 접속 없음)."""
import json
from pathlib import Path

from law_indexer import auto_ingest as A


def _paths(tmp_path):
    return A.IngestPaths(tmp_path / "root").ensure()


def _summary(**over):
    base = {"total": 0, "done": 0, "partial": 0, "failed": 0, "duplicate": 0, "waiting": 0, "deferred": 0,
            "version_skipped": 0, "quality_guard": [], "zips": []}
    base.update(over)
    return base


# ── decide ──────────────────────────────────────────────────────────────

def test_decide_ok_when_nothing_failed():
    assert A.decide([{"zip": "a.zip", "status": "converted"}], _summary(done=1)) == ("ok", [])


def test_decide_waiting_for_retry_or_manifest():
    assert A.decide([], _summary(waiting=1))[0] == "waiting"
    assert A.decide([{"zip": "a.zip", "status": "pending_manifest"}], _summary())[0] == "waiting"


def test_decide_halts_on_every_failure_kind():
    verdict, reasons = A.decide(
        [{"zip": "bad.zip", "status": "failed"}],
        _summary(failed=1, partial=1, skipped="x", quality_guard=[{"zip": "q.zip", "reason": "chunks_dropped_ratio"}]))
    assert verdict == "halt"
    assert reasons == ["convert_failed:bad.zip", "lock_conflict", "zip_failed:1", "zip_partial:1",
                       "quality_guard:q.zip:chunks_dropped_ratio"]


def test_decide_partial_can_be_tolerated_by_policy():
    assert A.decide([], _summary(partial=1), halt_on_partial=False) == ("ok", [])


def test_version_skipped_is_not_a_halt():
    """옛 버전 건너뜀은 보호가 작동한 것 — 멈추지 않는다."""
    assert A.decide([], _summary(done=1, version_skipped=2)) == ("ok", [])


# ── run_once ────────────────────────────────────────────────────────────

def test_run_once_records_both_stages_and_strips_payloads(tmp_path):
    paths = _paths(tmp_path)
    seen = {}

    def convert(inbox, outbox, *, done_dir, failed_dir):
        seen["convert"] = (inbox, outbox, done_dir, failed_dir)
        return [{"zip": "a.zip", "status": "converted", "payloads": [{"big": "x" * 100}]}]

    def consume(folder):
        seen["consume"] = folder
        return _summary(done=1)

    record = A.run_once(paths, convert, consume, trigger="test")
    assert seen["convert"] == (paths.inbox, paths.converted, paths.done, paths.failed)
    assert seen["consume"] == paths.converted
    assert record["verdict"] == "ok" and record["trigger"] == "test"
    assert "payloads" not in record["convert"][0]


def test_run_once_turns_exception_into_halt(tmp_path):
    paths = _paths(tmp_path)

    def convert(*a, **k):
        raise RuntimeError("admin-api 죽음")

    record = A.run_once(paths, convert, lambda folder: _summary())
    assert record["verdict"] == "halt" and record["reasons"] == ["exception:RuntimeError: admin-api 죽음"]
    assert "Traceback" in record["traceback"]


# ── Runner ──────────────────────────────────────────────────────────────

def _runner(tmp_path, verdicts, **kw):
    """verdicts 를 순서대로 돌려주는 run_fn 을 가진 러너(스레드는 띄우지 않고 sweep 을 직접 부른다)."""
    paths = _paths(tmp_path)
    calls = []

    def run_fn(reason):
        calls.append(reason)
        verdict = verdicts[min(len(calls) - 1, len(verdicts) - 1)]
        return {"trigger": reason, "started_at": A._now(), "finished_at": A._now(),
                "verdict": verdict, "reasons": ["zip_failed:1"] if verdict == "halt" else []}

    return A.Runner(paths, run_fn, interval_sec=600, instance="pod-a", **kw), paths, calls


def test_sweep_writes_run_record_and_latest(tmp_path):
    runner, paths, calls = _runner(tmp_path, ["ok"])
    record = runner.sweep("ingest")
    assert calls == ["ingest"] and record["instance"] == "pod-a"
    assert json.loads(paths.latest.read_text(encoding="utf-8"))["id"] == record["id"]
    assert (paths.runs / f"{record['id']}.json").exists()
    assert not paths.lease.exists()                         # 끝나면 lease 를 놓는다
    assert runner.status()["state"] == "idle"


def test_halt_stops_further_sweeps_until_resumed(tmp_path):
    runner, paths, calls = _runner(tmp_path, ["halt", "ok"])
    assert runner.sweep("ingest")["verdict"] == "halt"
    halt = json.loads(paths.halt.read_text(encoding="utf-8"))
    assert halt["reasons"] == ["zip_failed:1"] and halt["run_id"]
    assert runner.status()["state"] == "halted"
    assert runner.sweep("interval") is None and calls == ["ingest"]   # 멈춤 — 돌지 않는다
    assert runner.resume(by="tester") is True
    assert not paths.halt.exists()
    assert runner.sweep("resume")["verdict"] == "ok" and calls == ["ingest", "resume"]
    assert runner.resume() is False                          # 풀 것이 없다


def test_requeue_moves_failed_pair_back_and_triggers(tmp_path):
    runner, paths, calls = _runner(tmp_path, ["ok"])
    for suffix in (".zip", ".manifest.json", ".convert-report.json"):
        (paths.failed / f"b1{suffix}").write_bytes(b"x")
    assert runner.requeue("b1.zip", "convert") == ["b1.zip", "b1.manifest.json"]
    assert (paths.inbox / "b1.zip").exists() and (paths.inbox / "b1.manifest.json").exists()
    assert not (paths.failed / "b1.convert-report.json").exists()
    (paths.converted / "failed").mkdir(parents=True)
    (paths.converted / "failed" / "b2.zip").write_bytes(b"x")
    assert runner.requeue("b2.zip", "consume") == ["b2.zip"]
    assert (paths.converted / "b2.zip").exists()
    import pytest
    with pytest.raises(FileNotFoundError):
        runner.requeue("nope.zip", "convert")
    with pytest.raises(ValueError):
        runner.requeue("b1.zip", "elsewhere")
    assert runner._reason == "requeue"                                   # 다음 sweep 사유


def test_lease_of_another_live_instance_blocks_sweep(tmp_path):
    """구 리비전 파드 같은 다른 인스턴스가 돌고 있으면 같은 폴더를 건드리지 않는다. 오래된 lease 는 넘겨받는다."""
    runner, paths, calls = _runner(tmp_path, ["ok"])
    A.write_json(paths.lease, {"instance": "pod-old", "at": A._now()})
    assert runner.sweep("ingest") is None and calls == []
    A.write_json(paths.lease, {"instance": "pod-old", "at": "2020-01-01T00:00:00+00:00"})
    assert runner.sweep("ingest")["verdict"] == "ok" and calls == ["ingest"]


def test_status_counts_pending_inbox_and_failed(tmp_path):
    runner, paths, _ = _runner(tmp_path, ["ok"])
    (paths.inbox / "b1.zip").write_bytes(b"x")
    (paths.inbox / "b1.manifest.json").write_text("{}", encoding="utf-8")
    (paths.inbox / "b2.zip").write_bytes(b"x")                       # manifest 없음
    (paths.inbox / ".b3.zip.part").write_bytes(b"x")                 # 쓰는 중 — 세지 않는다
    (paths.failed / "old.zip").write_bytes(b"x")
    status = runner.status()
    assert status["inbox"] == {"pending": 2, "pending_manifest": ["b2.zip"]}
    assert status["failed"] == {"convert": 1, "consume": 0}
    assert status["attention"] == [] and status["last_received_at"] is None and "disk" in status


def test_runner_thread_wakes_on_trigger(tmp_path):
    """실제 스레드 — trigger() 로 깨어나 sweep 하고, stop() 으로 끝난다."""
    import threading
    paths = _paths(tmp_path)
    ran = threading.Event()

    def run_fn(reason):
        ran.set()
        return {"trigger": reason, "started_at": A._now(), "finished_at": A._now(), "verdict": "ok", "reasons": []}

    runner = A.Runner(paths, run_fn, interval_sec=3600, instance="pod-a")
    runner.start()
    runner.trigger("ingest")
    assert ran.wait(5), "트리거 후 5초 안에 sweep 이 돌지 않았다"
    runner.stop()
    runner._thread.join(5)
    assert not runner._thread.is_alive()
    assert A.read_json(paths.latest)["trigger"] == "ingest"


# ── 보존 · 방치된 ZIP · 결손 감지 ────────────────────────────────────────

def _old(path, days):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    import os, time
    stamp = time.time() - days * 86400
    os.utime(path, (stamp, stamp))


def test_retire_removes_only_archives_older_than_retention(tmp_path):
    paths = _paths(tmp_path)
    _old(paths.done / "old.zip", 40)
    _old(paths.converted / "done" / "old.zip", 40)
    _old(paths.converted / "packages" / "processed" / "old.jsonl", 40)
    _old(paths.rejected / "old.zip", 40)
    _old(paths.runs / "20260101T000000Z_x.json", 40)
    _old(paths.done / "new.zip", 3)
    _old(paths.failed / "old.zip", 40)                               # 실패분은 사람이 봐야 한다 — 지우지 않는다
    _old(paths.converted / "state" / "abc.json", 40)                 # 이력도 지우지 않는다
    A.write_json(paths.latest, {"id": "keep"})
    removed = A.retire(paths, 30)
    assert removed["files"] == 5
    assert not (paths.done / "old.zip").exists() and (paths.done / "new.zip").exists()
    assert (paths.failed / "old.zip").exists() and (paths.converted / "state" / "abc.json").exists()
    assert paths.latest.exists()
    assert A.retire(paths, 0) == {"files": 0, "bytes": 0}             # 0 = 영구 보존


def test_zip_without_manifest_beyond_grace_halts(tmp_path):
    """manifest 없이 유예를 넘긴 ZIP 은 조용히 대기하지 않고 멈춤 사유가 된다(옛 수신기 잔존물·수동 투입)."""
    runner, paths, calls = _runner(tmp_path, ["ok"], manifest_grace_sec=60)
    _old(paths.inbox / "legacy.zip", 1)
    (paths.inbox / "fresh.zip").write_bytes(b"x")                     # 방금 — 유예 안
    assert runner.manifest_overdue() == ["legacy.zip"]
    record = runner.sweep("interval")
    assert record["verdict"] == "halt" and record["reasons"] == ["manifest_missing:legacy.zip"]
    assert runner.status()["state"] == "halted"


def test_attention_flags_missing_receipts(tmp_path):
    runner, paths, _ = _runner(tmp_path, ["ok"], expect_interval_hours=24)
    with open(paths.ledger, "a", encoding="utf-8") as f:
        f.write(json.dumps({"status": "rejected", "received_at": "2026-10-22T00:00:00+00:00"}) + "\n")
        f.write(json.dumps({"status": "accepted", "received_at": "2026-10-01T00:00:00+00:00"}) + "\n")
    assert runner.last_receipt() == "2026-10-01T00:00:00+00:00"
    assert runner.attention()[0].startswith("no_receipt_for_")
    assert runner.status()["hours_since_last_receipt"] > 24
    record = runner.sweep("interval")
    assert record["verdict"] == "ok" and record["attention"][0].startswith("no_receipt_for_")
