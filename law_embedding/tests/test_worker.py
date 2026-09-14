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


def test_activities_registered_with_expected_names():
    d1 = getattr(worker.initial_index, "__temporal_activity_definition", None)
    d2 = getattr(worker.consume_package_activity, "__temporal_activity_definition", None)
    assert d1 is not None and d1.name == INITIAL_INDEX_ACTIVITY
    assert d2 is not None and d2.name == CONSUME_PACKAGE_ACTIVITY


def test_collection_selector():
    from law_indexer.config import Settings
    s = Settings.from_env()
    assert worker._collection_for(s, "law") == s.law_collection
    assert worker._collection_for(s, "admrul") == s.admrul_collection
