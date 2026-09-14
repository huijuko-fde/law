"""MED 견고화 검증 — cli exit 코드 · iso_date 관대화 · mapper 비정상원소 가드.

한 필드/원소가 이상해도 '문서 전체 드롭'이나 '조용한 exit 0' 이 되지 않는지 고정한다.
"""
from pathlib import Path

from law_indexer.cli import _failure_code
from law_indexer.mapper import iso_date, map_law_data


# ── cli exit 코드 ────────────────────────────────────────────────────────

def test_failure_code_clean_is_zero():
    assert _failure_code({"ok": 3, "failed": 0, "errors": []}) == 0
    assert _failure_code({"by_source": {"law": {"files_failed": 0, "errors": []}}}) == 0
    assert _failure_code("not a dict") == 0


def test_failure_code_partial_is_two():
    assert _failure_code({"failed": 2}) == 2                          # consume-folder
    assert _failure_code({"files_failed": 1}) == 2
    assert _failure_code({"objects_failed": 5}) == 2
    assert _failure_code({"errors": [{"e": "x"}]}) == 2
    assert _failure_code({"by_source": {"law": {"objects_failed": 1}}}) == 2   # index 중첩


# ── iso_date 관대화 ──────────────────────────────────────────────────────

def test_iso_date_bad_returns_none_not_raise():
    assert iso_date("쓰레기날짜") is None
    assert iso_date("not-a-date") is None
    assert iso_date(None) is None and iso_date("") is None
    assert iso_date("20240101") == "2024-01-01T00:00:00Z"            # 정상은 그대로


# ── mapper 비정상원소 가드 ───────────────────────────────────────────────

def test_map_law_survives_malformed_elements():
    """비-dict article/addendum/appendix, 이상한 날짜가 섞여도 문서가 드롭되지 않고 정상 조문은 매핑."""
    data = {
        "law_id": "L1", "version_uid": "L1:1:20240101", "law_name": "법", "law_type": "법률",
        "promulgation_date": "쓰레기날짜",                            # None 으로 흡수(드롭 안 함)
        "body": {"articles": [
            "문자열-비정상",                                          # 비-dict → skip
            {"article_no": "제1조", "content": "본문", "provision_id": "law:L1#JO0001"},
        ]},
        "addenda": ["비정상", {"content": "부칙내용", "provision_id": "law:L1#ADD1",
                              "promulgation_date": "20240101"}],
        "appendices": [None, "비정상"],                               # 전부 비-dict → skip
    }
    objs = map_law_data(data, Path("changeset:L1"))
    provision_ids = [o.provision_id for o in objs]
    assert "law:L1#JO0001" in provision_ids                          # 정상 조문 살아남음


# ── upsert partial 실패 시 옛 청크 정리를 건너뛰는지 ────────────────────────
import json                                                            # noqa: E402

from law_indexer.config import Settings                               # noqa: E402
from law_indexer.package import consume_package                       # noqa: E402


class _StaleTrackStore:
    """upsert 의 실패 개수를 주입하고, delete_stale_law_chunks 가 불렸는지 기록한다."""

    def __init__(self, upsert_failed: int):
        self.upsert_failed = upsert_failed
        self.stale_called = False

    def upsert(self, objects, dimension, model_name, collection):
        n = len(list(objects))
        f = min(self.upsert_failed, n)
        return {"success": n - f, "failed": f, "failed_ids": ["x"] * f}

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        self.stale_called = True
        return 0

    def delete_by_law_id(self, law_id, collection):
        return 0


class _FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


def _doc_package(tmp_path):
    """새 버전(version_uid) 문서 1건 package — 옛 청크 정리 대상이 생기게."""
    payload = {"law_id": "L1", "version_uid": "L1:2:20250101", "law_name": "관세법", "law_type": "법률",
               "body": {"articles": [{"article_no": "제1조", "content": "본문",
                                      "provision_id": "law:L1#JO0001"}]}}
    p = tmp_path / "pkg.jsonl"
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in [
        {"record_type": "package_header", "package_id": "p", "source": "law", "mode": "delta"},
        {"record_type": "document", "op": "upsert", "law_id": "L1",
         "git_path": "관세법/법률/관세법.json", "payload": payload},
    ]) + "\n", encoding="utf-8")
    return p


def test_stale_cleanup_skipped_on_partial_upsert_failure(tmp_path):
    """청크 하나라도 upsert 실패(예외 아님)면 옛 버전 청크를 지우지 않는다 — 검색 구멍 방지."""
    store = _StaleTrackStore(upsert_failed=1)
    totals = consume_package(store, _FakeEmbedder(), Settings.from_env(), _doc_package(tmp_path))
    assert totals["objects_failed"] == 1
    assert store.stale_called is False                                # 실패 → 옛 청크 보존


def test_stale_cleanup_runs_on_full_success(tmp_path):
    """전부 성공(failed==0)하면 옛 버전 청크를 정리한다."""
    store = _StaleTrackStore(upsert_failed=0)
    consume_package(store, _FakeEmbedder(), Settings.from_env(), _doc_package(tmp_path))
    assert store.stale_called is True


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
