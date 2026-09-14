"""index_changeset(증분 소비자) 로직 검증 — Weaviate/임베딩 모델 없이 fake 로.

change-set(JSONL) 계약: upsert 는 payload(또는 git_path)로 재적재하되 law_id 단위로 옛 청크를
먼저 지운다, delete 는 law_id 단위로 지운다. (temporal_law/pipeline/common/changeset.py 와 대응)
"""
import json

from law_indexer.config import Settings
from law_indexer.pipeline import index_changeset


class FakeStore:
    """delete_by_law_id/upsert 만 흉내 내는 store — 컬렉션 인자를 받는 merged 시그니처."""

    def __init__(self):
        self.deleted = []
        self.stale = []
        self.upserted = []

    def delete_by_law_id(self, law_id, collection):
        self.deleted.append(law_id)
        return 1

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        self.stale.append((law_id, set(keep_version_uids)))
        return 0

    def upsert(self, objects, dimension, model_name, collection):
        self.upserted.append([o.law_id for o in objects])
        return {"success": len(objects), "failed": 0, "failed_ids": []}


class FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


def _law_payload(law_id):
    return {
        "law_id": law_id, "version_uid": f"{law_id}:1:20240101", "law_name": f"법{law_id}",
        "law_type": "법률",
        "body": {"articles": [
            {"article_no": "제1조", "article_title": "목적", "content": "목적 조문",
             "provision_id": f"law:{law_id}#JO0001"}]},
    }


def test_changeset_upsert_and_delete(tmp_path):
    settings = Settings.from_env()
    cs = tmp_path / "cs.jsonl"
    cs.write_text(
        json.dumps({"op": "upsert", "source": "law", "law_id": "L1",
                    "git_path": "x.json", "payload": _law_payload("L1")}, ensure_ascii=False) + "\n"
        + json.dumps({"op": "delete", "source": "law", "law_id": "L2"}, ensure_ascii=False) + "\n",
        encoding="utf-8")

    store, embedder = FakeStore(), FakeEmbedder()
    totals = index_changeset(store, embedder, settings, cs, "law", data_root=None)

    assert totals["files_success"] == 1 and totals["objects_success"] == 1
    assert totals["deleted_docs"] == 1
    # upsert(L1)=재적재 먼저 → 옛 버전 stale 정리 / delete(L2)=전량삭제. L1 만 재적재.
    assert store.deleted == ["L2"]
    assert [s[0] for s in store.stale] == ["L1"]
    assert store.upserted == [["L1"]]


def test_changeset_reads_payload_from_git_path(tmp_path):
    settings = Settings.from_env()
    root = tmp_path / "law_data"
    (root / "법L3" / "법률").mkdir(parents=True)
    rel = "법L3/법률/법L3.json"
    (root / rel).write_text(json.dumps(_law_payload("L3"), ensure_ascii=False), encoding="utf-8")
    cs = tmp_path / "cs.jsonl"
    cs.write_text(json.dumps({"op": "upsert", "source": "law", "law_id": "L3", "git_path": rel},
                             ensure_ascii=False) + "\n", encoding="utf-8")

    store, embedder = FakeStore(), FakeEmbedder()
    totals = index_changeset(store, embedder, settings, cs, "law", data_root=root)
    assert totals["files_success"] == 1 and store.upserted == [["L3"]]
