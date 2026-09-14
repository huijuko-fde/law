"""consume_folder(스트리밍 소비자) 검증 — 성공→processed, 실패→failed+errors.json, 멱등.

WeaviateStore 는 fake 로 monkeypatch(실제 Weaviate 없이).
"""
import json

import pytest

from law_indexer import pipeline
from law_indexer.config import Settings


class _FakeStore:
    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def delete_by_law_id(self, law_id, collection):
        return 1

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        return 0

    def upsert(self, objects, dimension, model_name, collection):
        objs = list(objects)
        return {"success": len(objs), "failed": 0, "failed_ids": []}


class _FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


def _payload(law_id):
    return {"law_id": law_id, "version_uid": f"{law_id}:1:20240101", "law_name": f"법{law_id}",
            "law_type": "법률",
            "body": {"articles": [{"article_no": "제1조", "content": "본문",
                                   "provision_id": f"law:{law_id}#JO0001"}]}}


def _write_pkg(path, *records):
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n", encoding="utf-8")


def _header(**extra):
    return dict(record_type="package_header", package_id="p", source="law", mode="delta", **extra)


def test_consume_folder_success_failure_and_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "WeaviateStore", _FakeStore)
    folder = tmp_path / "pkgs"
    folder.mkdir()
    _write_pkg(folder / "ok.jsonl", _header(),
               {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _payload("L1")},
               {"record_type": "package_footer", "record_count": 1})
    _write_pkg(folder / "bad.jsonl", _header(),
               {"record_type": "document", "op": "upsert", "law_id": "L2", "payload": _payload("L2")},
               {"record_type": "package_footer", "record_count": 999})   # 불일치 → errors

    settings = Settings.from_env()
    summary = pipeline.consume_folder(_FakeEmbedder(), settings, folder,
                                      processed_dir=folder / "processed", failed_dir=folder / "failed")

    assert summary["ok"] == 1 and summary["failed"] == 1
    assert (folder / "processed" / "ok.jsonl").exists()                  # 성공 → 아카이브
    assert (folder / "failed" / "bad.jsonl").exists()                    # 실패 → 격리
    assert (folder / "failed" / "bad.jsonl.errors.json").exists()        # 실패 사유 사이드카
    assert not (folder / "ok.jsonl").exists() and not (folder / "bad.jsonl").exists()

    # 재실행 = 멱등: 이미 옮겨진 건 top-level 스캔에 안 잡힌다 → 할 일 0
    summary2 = pipeline.consume_folder(_FakeEmbedder(), settings, folder,
                                       processed_dir=folder / "processed", failed_dir=folder / "failed")
    assert summary2["total"] == 0


def test_consume_folder_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "WeaviateStore", _FakeStore)
    folder = tmp_path / "empty"
    folder.mkdir()
    summary = pipeline.consume_folder(_FakeEmbedder(), settings=Settings.from_env(), folder=folder)
    assert summary["total"] == 0 and summary["ok"] == 0 and summary["failed"] == 0


class _FlakyStore(_FakeStore):
    """upsert 가 일시장애로 예외를 내는 store(transient 재시도 검증용)."""

    def upsert(self, objects, dimension, model_name, collection):
        raise RuntimeError("weaviate 연결 끊김(일시장애)")


def _ok_pkg(folder, name="x.jsonl"):
    _write_pkg(folder / name, _header(),
               {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _payload("L1")},
               {"record_type": "package_footer", "record_count": 1})


def test_missing_dir_raises(tmp_path):
    """없는/오타 폴더는 조용한 성공(0건)이 아니라 즉시 에러."""
    with pytest.raises(FileNotFoundError):
        pipeline.consume_folder(_FakeEmbedder(), Settings.from_env(), tmp_path / "nope")


def test_transient_error_retries_not_quarantined(tmp_path, monkeypatch):
    """일시장애(upsert 예외)는 failed/ 로 안 가고 원래 이름으로 되돌아와 재시도 대기."""
    monkeypatch.setattr(pipeline, "WeaviateStore", _FlakyStore)
    folder = tmp_path / "pkgs"; folder.mkdir()
    _ok_pkg(folder)
    s = Settings.from_env()
    summary = pipeline.consume_folder(_FakeEmbedder(), s, folder,
                                      processed_dir=folder / "processed", failed_dir=folder / "failed")
    assert summary["retry"] == 1 and summary["failed"] == 0
    assert (folder / "x.jsonl").exists()                       # 되돌아옴(재시도 대기)
    assert not (folder / "failed" / "x.jsonl").exists()
    assert (folder / "x.jsonl.attempts").read_text() == "1"


def test_transient_exhausts_to_failed(tmp_path, monkeypatch):
    """MAX 재시도 소진하면 결국 failed/ 로 격리(무한 재시도 방지)."""
    monkeypatch.setattr(pipeline, "WeaviateStore", _FlakyStore)
    folder = tmp_path / "pkgs"; folder.mkdir()
    _ok_pkg(folder)
    s = Settings.from_env()
    last = None
    for _ in range(pipeline._MAX_CONSUME_ATTEMPTS):
        last = pipeline.consume_folder(_FakeEmbedder(), s, folder,
                                       processed_dir=folder / "processed", failed_dir=folder / "failed")
    assert last["failed"] == 1 and last["retry"] == 0
    assert (folder / "failed" / "x.jsonl").exists()
    assert not (folder / "x.jsonl").exists()
    assert not (folder / "x.jsonl.attempts").exists()          # 격리 시 attempts 정리


def test_reclaims_stale_processing(tmp_path, monkeypatch):
    """크래시로 남은 <pkg>.processing 를 다음 실행이 되돌려 소비한다(orphan 방지)."""
    monkeypatch.setattr(pipeline, "WeaviateStore", _FakeStore)
    folder = tmp_path / "pkgs"; folder.mkdir()
    _write_pkg(folder / "x.jsonl.processing", _header(),
               {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _payload("L1")},
               {"record_type": "package_footer", "record_count": 1})
    summary = pipeline.consume_folder(_FakeEmbedder(), Settings.from_env(), folder,
                                      processed_dir=folder / "processed", failed_dir=folder / "failed")
    assert summary["ok"] == 1
    assert (folder / "processed" / "x.jsonl").exists()


def test_truncated_package_without_footer_retries_then_quarantines(tmp_path, monkeypatch):
    """header 는 있는데 footer 가 없는 package = 뒤가 잘린 것 — 정상 소비로 통과시키지 않는다.

    생산자(emit_package)는 footer 를 항상 쓴다. 종전엔 footer 가 없으면 대조를 건너뛰어
    잘린 package 의 뒷부분 문서들이 조용히 누락됐다. 지금은 오류로 남아 일시장애 재시도를
    거치고(비원자적 복사 중이던 파일이면 그 사이 완성본이 온다), 임계를 넘으면 격리된다."""
    monkeypatch.setattr(pipeline, "WeaviateStore", _FakeStore)
    folder = tmp_path / "pkgs"
    folder.mkdir()
    _write_pkg(folder / "cut.jsonl", _header(),
               {"record_type": "document", "op": "upsert", "law_id": "L9", "payload": _payload("L9")})
    settings = Settings.from_env()
    for _ in range(pipeline._MAX_CONSUME_ATTEMPTS):
        summary = pipeline.consume_folder(_FakeEmbedder(), settings, folder,
                                          processed_dir=folder / "processed", failed_dir=folder / "failed")
        assert summary["ok"] == 0                          # 어떤 판에서도 정상 통과는 없다
    assert (folder / "failed" / "cut.jsonl").exists()      # 임계 도달 → 격리
    assert not (folder / "cut.jsonl").exists()
