"""Temporal worker 정의 스모크 — temporal 서버·weaviate·모델 없이 import/등록만 검증.

라이브 실행(초기색인·package 소비)은 temporal + weaviate 가 있어야 하므로 여기선 워크플로/액티비티가
올바르게 정의·등록되는지(이름·데코레이터)만 본다. import 실패나 데코레이터 누락을 잡는다."""
from law_indexer import worker, worker_workflows as ww
from law_indexer.worker_workflows import CONSUME_PACKAGE_ACTIVITY, INITIAL_INDEX_ACTIVITY


def test_request_dataclasses():
    assert ww.IndexRequest().source == "law"
    assert ww.IndexRequest(source="admrul", limit=5).limit == 5
    assert ww.PackageRequest(package_path="/x/pkg.jsonl").source is None


def test_workflows_are_defined():
    assert hasattr(ww.InitialIndexWorkflow, "__temporal_workflow_definition")
    assert hasattr(ww.ConsumePackageWorkflow, "__temporal_workflow_definition")
    assert hasattr(ww.LawInboxWorkflow, "__temporal_workflow_definition")
    assert ww.LawInboxRequest(root="/x").halt_on_partial is True


def test_activities_registered_with_expected_names():
    d1 = getattr(worker.initial_index, "__temporal_activity_definition", None)
    d2 = getattr(worker.consume_package_activity, "__temporal_activity_definition", None)
    d3 = getattr(worker.convert_inbox_activity, "__temporal_activity_definition", None)
    d4 = getattr(worker.consume_zip_activity, "__temporal_activity_definition", None)
    assert d1 is not None and d1.name == INITIAL_INDEX_ACTIVITY
    assert d2 is not None and d2.name == CONSUME_PACKAGE_ACTIVITY
    assert d3 is not None and d3.name == ww.CONVERT_INBOX_ACTIVITY
    assert d4 is not None and d4.name == ww.CONSUME_ZIP_ACTIVITY
    assert ww.LawInboxWorkflow in worker.WORKFLOWS and worker.consume_zip_activity in worker.ACTIVITIES


def test_temporal_address_prefers_explicit_then_codeserving_env(monkeypatch):
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    monkeypatch.setenv("TEMPORAL_HOST", "temporal-frontend.temporal.svc.cluster.local")
    monkeypatch.setenv("TEMPORAL_PORT", "7233")
    assert worker.temporal_address() == "temporal-frontend.temporal.svc.cluster.local:7233"
    monkeypatch.setenv("TEMPORAL_ADDRESS", "localhost:7233")
    assert worker.temporal_address() == "localhost:7233"


def test_collection_selector():
    from law_indexer.config import Settings
    s = Settings.from_env()
    assert worker._collection_for(s, "law") == s.law_collection
    assert worker._collection_for(s, "admrul") == s.admrul_collection


def test_activity_attempt_reports_are_merged(tmp_path, monkeypatch):
    """재시도된 activity 는 앞선 시도의 보고를 잃지 않는다 — 시도 사이에 failed/ 로 옮겨진 ZIP 이 판정에서
    빠지면 실패가 ok 로 둔갑한다(2026-11-05 전체 검증 PS-2)."""
    from types import SimpleNamespace
    from law_indexer.auto_ingest import IngestPaths
    from law_indexer.ingest_rules import merge_reports, merge_summaries
    paths = IngestPaths(tmp_path / "root").ensure()
    info = SimpleNamespace(attempt=1, workflow_run_id="run-1", activity_id="7")
    monkeypatch.setattr(worker.activity, "info", lambda: info)
    first = worker._merge_attempts(paths, "convert", [{"zip": "a.zip", "status": "failed"}], merge_reports)
    assert first == [{"zip": "a.zip", "status": "failed"}]
    info.attempt = 2
    second = worker._merge_attempts(paths, "convert", [{"zip": "b.zip", "status": "converted"}], merge_reports)
    assert [(r["zip"], r["status"]) for r in second] == [("a.zip", "failed"), ("b.zip", "converted")]
    info.attempt = 1
    s1 = worker._merge_attempts(paths, "consume", {"total": 2, "done": 1, "failed": 1, "zips": ["x"]}, merge_summaries)
    info.attempt = 2
    s2 = worker._merge_attempts(paths, "consume", {"total": 0, "done": 0, "failed": 0, "zips": []}, merge_summaries)
    assert s1["failed"] == 1 and s2["failed"] == 1 and s2["total"] == 2 and s2["attempts"] == 2 and s2["zips"] == ["x"]
    info.attempt, info.activity_id = 1, "8"                                   # 다음 sweep(다른 activity)은 섞이지 않는다
    assert worker._merge_attempts(paths, "consume", {"total": 0, "failed": 0}, merge_summaries)["failed"] == 0

    def no_activity():
        raise RuntimeError("Not in activity context")

    monkeypatch.setattr(worker.activity, "info", no_activity)
    assert worker._merge_attempts(paths, "consume", {"failed": 3}, merge_summaries) == {"failed": 3}


def test_merge_rules():
    from law_indexer.ingest_rules import decide, merge_reports, merge_summaries
    reports = merge_reports([[{"zip": "a.zip", "status": "failed"}],
                             [{"zip": "a.zip", "status": "converted"}, {"zip": "b.zip", "status": "converted"}]])
    assert [(r["zip"], r["status"]) for r in reports] == [("a.zip", "converted"), ("b.zip", "converted")]
    summary = merge_summaries([{"total": 1, "failed": 1, "waiting": 1, "quality_guard": [{"zip": "a"}]},
                               {"total": 1, "done": 1, "waiting": 0}])
    assert summary["failed"] == 1 and summary["done"] == 1 and summary["total"] == 2 and summary["waiting"] == 0
    assert summary["quality_guard"] == [{"zip": "a"}] and summary["attempts"] == 2
    assert decide(reports, summary)[0] == "halt"
    assert merge_summaries([]) == {}
