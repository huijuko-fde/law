"""rename record(개명 통지) 소비 검증 — Weaviate/임베딩 모델 없이 fake 로.

수집기가 문서 개명을 감지하면 package 에 rename record 를 실어 보낸다. 소비자는
① 개명 문서를 참조하던 청크들의 reference_ids/meta 를 새 이름 기준으로 고치고(벡터 보존)
② 미러(RepoMirror)의 옛 문서 폴더를 정리한다. 개명 문서 자체는 같은 package 의 document
record 로 재적재된다(여기서는 그 부분을 document 1건으로 흉내).
"""
import json

from law_indexer.config import Settings
from law_indexer.package import RepoMirror, _rename_prefix, consume_package
from law_indexer.weaviate_store import _patch_renamed_meta, _swap_renamed_head


# ── 참조 id 머리 교체: 정확 경계 판정 ──────────────────────────────

def test_swap_renamed_head_boundaries():
    old, new = "law:법인세법", "law:기업소득세법"
    assert _swap_renamed_head("law:법인세법#JO0001", old, new) == "law:기업소득세법#JO0001"
    assert _swap_renamed_head("law:법인세법:suffix#DOC", old, new) == "law:기업소득세법:suffix#DOC"
    assert _swap_renamed_head("law:법인세법", old, new) == "law:기업소득세법"
    # 이름이 겹치기만 하는 다른 문서는 건드리지 않는다
    assert _swap_renamed_head("law:법인세법시행령#JO0001", old, new) == "law:법인세법시행령#JO0001"
    assert _swap_renamed_head(None, old, new) is None


def test_rename_prefix():
    assert _rename_prefix("eflaw") == "law" and _rename_prefix(None) == "law"
    assert _rename_prefix("admrul") == "admrul"
    for t in ("school", "pi", "public"):
        assert _rename_prefix(t) == "SchlPubRul"


# ── meta(JSON) 패치: 참조 id·경로만, 인용 원문은 보존 ─────────────

def test_patch_renamed_meta_targets_only_renamed_refs():
    def swap(rid):
        return _swap_renamed_head(rid, "law:갑법", "law:을법")

    meta = json.dumps({"relation_refs": [
        {"reference_id": "law:갑법#JO0001", "line_text": "「갑법」 제1조에 따라",
         "target_git_path": "갑법/법률/갑법.json",
         "target_candidate_paths": [{"git_path": "갑법/법률/갑법.json", "id": "law:갑법#DOC"}]},
        {"reference_id": "law:갑법시행령#JO0002",                    # 다른 문서 — 그대로
         "target_git_path": "갑법/시행령/갑법 시행령.json"},
    ]}, ensure_ascii=False)
    out = _patch_renamed_meta(meta, swap, "갑법", "을법")
    assert out is not None
    parsed = json.loads(out)
    r0, r1 = parsed["relation_refs"]
    assert r0["reference_id"] == "law:을법#JO0001"
    assert r0["target_git_path"] == "을법/법률/을법.json"
    assert r0["target_candidate_paths"][0]["git_path"] == "을법/법률/을법.json"
    assert r0["line_text"] == "「갑법」 제1조에 따라"               # 인용 원문은 안 바꾼다
    assert r1["reference_id"] == "law:갑법시행령#JO0002"            # 개명 대상 아님 — 그대로
    assert r1["target_git_path"] == "갑법/시행령/갑법 시행령.json"
    # 바꿀 것이 없으면 None(멱등 — 두 번째 적용은 no-op)
    assert _patch_renamed_meta(out, swap, "갑법", "을법") is None


# ── 미러 옛 폴더 정리: 새 폴더 있으면 삭제, 없으면 이동 ───────────

def test_repo_mirror_rename_dir(tmp_path):
    mirror = RepoMirror(tmp_path, push=False)
    old = tmp_path / "갑법" / "법률"
    old.mkdir(parents=True)
    (old / "갑법.json").write_text("{}", encoding="utf-8")

    # 새 폴더가 아직 없으면 → 이동(미러가 파일을 잃지 않게)
    assert mirror.rename_dir("갑법/법률", "을법/법률")
    assert not old.exists() and (tmp_path / "을법" / "법률" / "갑법.json").exists()

    # 새 폴더가 이미 있으면(문서가 같은 package 로 재적재됨) → 옛 폴더 삭제
    old.mkdir(parents=True)
    (old / "갑법.json").write_text("{}", encoding="utf-8")
    assert mirror.rename_dir("갑법/법률", "을법/법률")
    assert not (tmp_path / "갑법").exists() or not old.exists()

    # 옛 폴더가 없으면 no-op(멱등), 경로 탈출은 거부
    assert not mirror.rename_dir("갑법/법률", "을법/법률")
    assert not mirror.rename_dir("../밖", "을법/법률")


# ── package 소비: rename record → 세 컬렉션 참조 패치 호출 ────────

class FakeStore:
    def __init__(self):
        self.upserted, self.stale, self.patches = [], [], []

    def upsert(self, objects, dimension, model_name, collection):
        self.upserted.append([o.law_id for o in objects])
        return {"success": len(objects), "failed": 0, "failed_ids": []}

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        self.stale.append(law_id)
        return 0

    def delete_by_law_id(self, law_id, collection):
        return 0

    def patch_renamed_references(self, old_head, new_head, old_name, new_name, collection):
        self.patches.append((old_head, new_head, collection))
        return {"chunks": 1, "refs": 2}

    def delete_renamed_self_chunks(self, law_id, old_head, collection):
        return 0

    def has_provision_head(self, head, collection):
        # 동명 문서 없음(옛 이름 청크가 안 남음) → 참조 패치 진행
        return False


class FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


def test_consume_package_applies_rename(tmp_path, monkeypatch):
    from law_indexer import package as pkg
    settings = Settings.from_env()
    fake = FakeStore()
    # 다른 컬렉션 연결도 전부 fake 로(테스트에서 실제 Weaviate 를 열지 않게)
    monkeypatch.setattr(pkg._RoutedStores, "get", lambda self, source: fake)

    payload = {"law_id": "L1", "version_uid": "L1:2:20260101", "law_name": "을법",
               "law_type": "법률", "doc_target": "eflaw",
               "body": {"articles": [{"article_no": "제1조", "article_title": "목적",
                                      "content": "본문", "provision_id": "law:을법#JO0001"}]}}
    p = tmp_path / "law-20260828-000000-abc123.jsonl"
    records = [
        {"record_type": "package_header", "package_id": "p1", "source": "law", "mode": "delta"},
        {"record_type": "document", "op": "upsert", "law_id": "L1",
         "git_path": "을법/법률/을법.json", "payload": payload},
        {"record_type": "rename", "law_id": "L1", "doc_target": "eflaw",
         "old_name": "갑법", "new_name": "을법",
         "old_dir": "갑법/법률", "new_dir": "을법/법률"},
        {"record_type": "package_footer", "package_id": "p1", "record_count": 2},
    ]
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records), encoding="utf-8")

    totals = consume_package(fake, FakeEmbedder(), settings, p)

    assert totals["errors"] == []
    assert totals["documents"] == 1 and totals["renames"] == 1
    # 세 컬렉션(law/admrul/schlpub) 전부에서 참조자를 찾아 고친다
    assert len(fake.patches) == 3
    assert all(o == "law:갑법" and n == "law:을법" for o, n, _c in fake.patches)
    assert totals["rename_refs_patched"] == 6


def test_consume_package_rename_skips_patch_when_old_name_still_lives(tmp_path, monkeypatch):
    """동명 가드 — 옛 이름을 그대로 쓰는 다른 문서가 남아 있으면 참조 패치를 건너뛴다.

    옛 이름 인용은 원래도 어느 문서를 가리키는지 확정 불가(enrich 가 동명 후보 목록으로 전달)라,
    새 이름으로 돌리면 남아 있는 문서를 가리키던 참조까지 바뀐다."""
    from law_indexer import package as pkg
    settings = Settings.from_env()

    class AmbiguousStore(FakeStore):
        def has_provision_head(self, head, collection):
            return True                                    # 옛 이름 청크가 아직 살아 있다

    fake = AmbiguousStore()
    monkeypatch.setattr(pkg._RoutedStores, "get", lambda self, source: fake)
    p = tmp_path / "law-20260828-000001-abc124.jsonl"
    records = [
        {"record_type": "package_header", "package_id": "p2", "source": "law", "mode": "delta"},
        {"record_type": "rename", "law_id": "L1", "doc_target": "eflaw",
         "old_name": "갑법", "new_name": "을법"},
        {"record_type": "package_footer", "package_id": "p2", "record_count": 1},
    ]
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records), encoding="utf-8")

    totals = consume_package(fake, FakeEmbedder(), settings, p)

    assert totals["errors"] == []
    assert fake.patches == []                              # 참조 패치 안 함
    assert totals["rename_ambiguous"][0]["old_name"] == "갑법"
    assert totals["renames"] == 1
