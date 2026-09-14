"""컬렉션 3개(law/admrul/schlpub = 레포당 하나) 라우팅 검증.

수집기 package/change-set 은 source 가 law/admrul 둘뿐이고, 학칙공단 문서는 admrul 에
doc_target(school/pi/public)으로 섞여 온다 — 소비자가 문서 단위로 schlpub 컬렉션에
갈라 넣어야 한다(delete 는 law_id 접두로 판별).
"""
import json

import pytest

from law_indexer.config import Settings
from law_indexer.package import consume_package
from law_indexer.pipeline import index_changeset


class FakeStore:
    """upsert/delete 가 어느 컬렉션으로 갔는지 기록하는 store."""

    def __init__(self, label="primary"):
        self.label = label
        self.calls = []            # (동작, law_id, collection)

    def delete_by_law_id(self, law_id, collection):
        self.calls.append(("delete", law_id, collection))
        return 1

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        self.calls.append(("stale", law_id, collection))
        return 0

    def upsert(self, objects, dimension, model_name, collection):
        self.calls.append(("upsert", objects[0].law_id if objects else None, collection))
        return {"success": len(objects), "failed": 0, "failed_ids": []}

    # _RoutedStores.close() 가 새로 연 연결을 닫을 때 부른다
    class _C:
        def close(self):
            pass
    client = _C()


class FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


@pytest.fixture
def routed_stores(monkeypatch):
    """_RoutedStores 가 lazily 여는 WeaviateStore 를 가짜로 바꾼다 — 열린 source 별로 기록."""
    opened = {}

    def _fake_store(settings, source=None):
        store = FakeStore(label=source)
        opened[source] = store
        return store

    monkeypatch.setattr("law_indexer.weaviate_store.WeaviateStore", _fake_store)
    return opened


def _admrul_payload(law_id, doc_target="admrul"):
    return {
        "law_id": law_id, "adm_uid": law_id, "law_name": f"규정{law_id}",
        "law_type": "고시", "doc_target": doc_target, "revision_date": "20240101",
        "body": {"articles": [
            {"article_no": "제1조", "article_title": "목적", "content": "목적 조문",
             "provision_id": f"admrul:규정{law_id}#JO0001"}]},
    }


def test_source_for_doc_matrix():
    s = Settings.from_env()
    assert s.source_for_doc("school") == "schlpub"
    assert s.source_for_doc("pi") == "schlpub"
    assert s.source_for_doc("public") == "schlpub"
    assert s.source_for_doc("admrul") == "admrul"
    assert s.source_for_doc("eflaw") == "law"
    # doc_target 없는 delete: law_id 접두로 판별, 못 가르면 default
    assert s.source_for_doc(None, "school:123", default="admrul") == "schlpub"
    assert s.source_for_doc(None, "admrul:9", default="admrul") == "admrul"
    assert s.source_for_doc(None, None, default="admrul") == "admrul"


def test_package_routes_schlpub_docs_to_schlpub_collection(tmp_path, routed_stores):
    """admrul package 에 섞여 온 학칙 문서는 schlpub 컬렉션으로, 고시는 admrul 로."""
    settings = Settings.from_env()
    lines = [
        {"record_type": "package_header", "package_id": "p1", "source": "admrul"},
        {"record_type": "document", "op": "upsert", "law_id": "admrul:1",
         "payload": _admrul_payload("admrul:1", "admrul")},
        {"record_type": "document", "op": "upsert", "law_id": "school:2",
         "payload": _admrul_payload("school:2", "school")},
        {"record_type": "delete", "law_id": "pi:3"},
        {"record_type": "package_footer", "record_count": 3},
    ]
    pkg = tmp_path / "p1.jsonl"
    pkg.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in lines), encoding="utf-8")

    primary = FakeStore(label="admrul-primary")
    totals = consume_package(primary, FakeEmbedder(), settings, pkg, source_override="admrul")

    assert not totals["errors"]
    # 고시는 호출자가 준 store(admrul 키) + admrul 컬렉션
    assert ("upsert", "admrul:1", settings.admrul_collection) in primary.calls
    # 학칙 upsert 와 공단정관 delete 는 schlpub 연결이 새로 열려 그 컬렉션으로
    sch = routed_stores.get("schlpub")
    assert sch is not None
    assert ("upsert", "school:2", settings.schlpub_collection) in sch.calls
    assert ("delete", "pi:3", settings.schlpub_collection) in sch.calls
    # 반대로 새 연결에 admrul 문서가 새지 않았고, primary 에 schlpub 문서가 없다
    assert all(c[1] != "admrul:1" for c in sch.calls)
    assert all(not str(c[1]).startswith(("school:", "pi:")) for c in primary.calls)


def test_changeset_routes_by_doc_target_and_id_prefix(tmp_path, routed_stores):
    """1세대 change-set 도 같은 규칙 — upsert 는 doc_target, delete 는 law_id 접두."""
    settings = Settings.from_env()
    cs = tmp_path / "cs.jsonl"
    cs.write_text(
        json.dumps({"op": "upsert", "source": "admrul", "law_id": "school:2",
                    "payload": _admrul_payload("school:2", "school")}, ensure_ascii=False) + "\n"
        + json.dumps({"op": "delete", "source": "admrul", "law_id": "public:7"},
                     ensure_ascii=False) + "\n",
        encoding="utf-8")

    primary = FakeStore(label="admrul-primary")
    totals = index_changeset(primary, FakeEmbedder(), settings, cs, "admrul")

    assert totals["files_success"] == 1 and totals["deleted_docs"] == 1
    sch = routed_stores.get("schlpub")
    assert sch is not None
    assert ("upsert", "school:2", settings.schlpub_collection) in sch.calls
    assert ("delete", "public:7", settings.schlpub_collection) in sch.calls
    assert primary.calls == []                    # 전부 schlpub 으로 갈라졌다
