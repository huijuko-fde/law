"""컬렉션 스키마 정의(SCHEMA)와 생성 규칙.

MCP 도구 `legal_search_provisions` 는 `query_properties=["search_text"]` 로 하이브리드
검색을 한다. 예전 스키마는 **모든 text 속성에 indexSearchable=False** 였고, 그 상태로
hybrid 를 부르면 Weaviate 가 "Searching by property ..." 로 **에러를 던진다**(빈 결과가
아니라 호출 자체 실패). 그래서 search_text 의 searchable 은 이 시스템의 계약이다.
"""
import pytest

from law_indexer.weaviate_store import SCHEMA


def _by_name():
    return {p["name"]: p for p in SCHEMA}


def test_schema_property_counts():
    """속성 35 = 필터 18 + 키워드검색 1 + 표시·부가 16. (git_path 필터 승격 — MCP 요청)"""
    assert len(SCHEMA) == 35
    assert len([p for p in SCHEMA if p["filterable"]]) == 18
    assert len([p for p in SCHEMA if p["searchable"]]) == 1


def test_search_text_is_the_only_bm25_property():
    """키워드 검색은 search_text 하나가 전담한다(본문·제목은 이미 합성돼 있다)."""
    searchable = [p["name"] for p in SCHEMA if p["searchable"]]
    assert searchable == ["search_text"]
    assert _by_name()["search_text"]["tokenization"] == "kagome_kr"
    assert _by_name()["content"]["searchable"] is False


def test_filter_text_properties_use_field_tokenizer():
    """필터 텍스트는 정확일치용이라 field 토크나이저를 쓴다(BM25 걸지 않는다)."""
    for prop in SCHEMA:
        if prop["filterable"] and prop["type"] in ("text", "text[]"):
            assert prop["tokenization"] == "field", prop["name"]
            assert prop["searchable"] is False, prop["name"]


def test_enforcement_date_supports_range_filters():
    """'2024년 기준' 같은 시점 질의는 시행일 범위 필터로 처리한다."""
    assert _by_name()["enforcement_date"]["range_filters"] is True
    assert [p["name"] for p in SCHEMA if p["range_filters"]] == ["enforcement_date"]


@pytest.mark.parametrize("name", [
    "chunk_id", "provision_id", "law_id", "version_uid", "unit_type",
    "is_current", "is_future", "enforcement_date", "reference_ids",
    "domain", "law_name", "law_type", "source_type", "revision_type", "git_path",
])
def test_required_filters_present(name):
    """MCP 도구와 증분 삭제가 where 조건으로 쓰는 속성들."""
    assert _by_name()[name]["filterable"] is True


def test_meta_is_not_filterable():
    """meta 는 JSON 문자열 1개라 where 필터 대상이 아니다."""
    meta = _by_name()["meta"]
    assert meta["filterable"] is False and meta["searchable"] is False


def test_tokenizer_fallback_order(monkeypatch):
    """kagome_kr 모듈이 없는 Weaviate 에서도 trigram 으로 컬렉션이 만들어져야 한다."""
    from law_indexer import weaviate_store as ws

    attempted = []

    class _FakeCollections:
        def exists(self, name):
            return False

        def delete(self, name):
            pass

    class _Store:
        settings = type("S", (), {"search_text_tokenization": "kagome_kr"})()
        client = type("C", (), {"collections": _FakeCollections()})()

        def _create_with_tokenization(self, name, tok):
            attempted.append(tok)
            if tok == "kagome_kr":
                raise RuntimeError("module not enabled")

    store = _Store()
    assert ws.WeaviateStore.create_collection(store, "X") is True
    assert attempted == ["kagome_kr", "trigram"]
