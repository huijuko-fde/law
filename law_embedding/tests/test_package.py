"""§6 JSONL package 소비자(consume_package) 검증 — Weaviate/임베딩 모델 없이 fake 로.

모든 record_type(document/normalized_chunk/preprocessed_chunk/file/pending_attachment/delete)과
자동감지(index_changeset 이 첫 줄로 package/1세대 구분), 그리고 law_id 단위 단일 삭제를 검증한다.
"""
import dataclasses
import json
from pathlib import Path

from law_indexer.config import Settings
from law_indexer.package import consume_package, is_package_file
from law_indexer.pipeline import index_changeset


class FakeStore:
    """delete_by_law_id/delete_stale_law_chunks 호출과 upsert 객체를 기록하는 store."""

    def __init__(self):
        self.deleted = []          # delete_by_law_id (폐지 전량삭제) 로 넘어온 law_id
        self.stale = []            # delete_stale_law_chunks (upsert 후 옛 버전 정리) (law_id, keep)
        self.upserted = []         # upsert 된 LegalProvision 목록들

    def delete_by_law_id(self, law_id, collection):
        self.deleted.append(law_id)
        return 1

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        self.stale.append((law_id, set(keep_version_uids)))
        return 0

    def upsert(self, objects, dimension, model_name, collection):
        self.upserted.append(list(objects))
        return {"success": len(objects), "failed": 0, "failed_ids": []}


class FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


class FakeDocParser:
    """run() 이 Doc Parser 원시 청크(text/i_chunk_on_doc/i_page)를 돌려주는 가짜 전처리기."""

    def run(self, request_path, chunk_size, chunk_overlap, endpoint_path=None):
        return [{"text": "전처리로 뽑은 파일 텍스트", "i_chunk_on_doc": 0, "i_page": 1, "e_page": 1}]


def _law_payload(law_id):
    return {
        "law_id": law_id, "version_uid": f"{law_id}:1:20240101", "law_name": f"법{law_id}",
        "law_type": "법률",
        "body": {"articles": [
            {"article_no": "제1조", "article_title": "목적", "content": "목적 조문",
             "provision_id": f"law:{law_id}#JO0001"}]},
    }


def _header(source="law", **extra):
    return dict(record_type="package_header", package_id="pkg-1", source=source,
                mode="delta", strategy="payload_with_optional_file_chunks", **extra)


def _write(tmp_path, *records):
    path = tmp_path / "pkg.jsonl"
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n", encoding="utf-8")
    return path


def _flat(objs_lists):
    return [o for lst in objs_lists for o in lst]


# ── 자동감지 ────────────────────────────────────────────────────────────

def test_is_package_file(tmp_path):
    pkg = _write(tmp_path, _header(), {"record_type": "document", "op": "upsert", "law_id": "L1",
                                       "payload": _law_payload("L1")})
    legacy = tmp_path / "cs.jsonl"
    legacy.write_text(json.dumps({"op": "upsert", "source": "law", "law_id": "L1"}) + "\n", encoding="utf-8")
    assert is_package_file(pkg) is True
    assert is_package_file(legacy) is False
    assert is_package_file(tmp_path / "nope.jsonl") is False


def test_index_changeset_auto_routes_to_package(tmp_path):
    """index_changeset 이 첫 줄 package_header 를 보고 §6 소비자로 위임한다(source 는 header)."""
    pkg = _write(tmp_path, _header(source="law"),
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")})
    store, embedder = FakeStore(), FakeEmbedder()
    totals = index_changeset(store, embedder, Settings.from_env(), pkg, source=None)
    assert totals["source"] == "law"
    assert totals["documents"] == 1
    assert [o.law_id for o in _flat(store.upserted)] == ["L1"]
    # 개정=upsert 먼저 → delete_by_law_id 안 부르고, 옛 버전은 upsert 후 stale 정리로.
    assert store.deleted == []
    assert [s[0] for s in store.stale] == ["L1"]


# ── record_type 별 ──────────────────────────────────────────────────────

def test_document_upsert_and_delete(tmp_path):
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "git_path": "x.json",
         "payload": _law_payload("L1")},
        {"record_type": "delete", "law_id": "L2"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)
    assert totals["documents"] == 1 and totals["deleted_docs"] == 1
    # 폐지(L2)만 delete_by_law_id 로 전량삭제. 개정(L1)은 upsert 먼저 → 옛 버전 stale 정리.
    assert store.deleted == ["L2"]
    assert [s[0] for s in store.stale] == ["L1"]
    assert [o.law_id for o in _flat(store.upserted)] == ["L1"]


def test_upsert_failure_preserves_old_chunks(tmp_path):
    """upsert(임베딩/적재)가 실패하면 옛 청크를 절대 지우지 않는다 — delete-후-실패로 그 법이
    검색에서 통째로 사라지는 창을 제거했는지 검증(delete-after-upsert의 핵심 안전 속성)."""
    class FailingStore(FakeStore):
        def upsert(self, objects, dimension, model_name, collection):
            raise RuntimeError("weaviate down / OOM")

    pkg = _write(tmp_path, _header(source="law"),
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")})
    store, embedder = FailingStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)
    assert totals["laws_failed"] == 1
    assert store.deleted == []          # 폐지 전량삭제 안 함
    assert store.stale == []            # upsert 실패라 옛 버전 정리에 도달조차 안 함 → 옛 청크 보존
    assert totals["errors"]             # 에러 기록 → consume_folder 가 failed/ 로 격리 후 재시도


def _make_gif(width: int = 24, height: int = 24) -> bytes:
    from io import BytesIO

    from PIL import Image
    buf = BytesIO()
    Image.new("RGB", (width, height), "white").save(buf, format="GIF")
    return buf.getvalue()


def test_admrul_document_upsert_runs_step1_image_ocr(tmp_path):
    """증분 소비 경로(package._flush_law)에서도 admrul 조문의 [그림] 마커가 본문이미지 OCR
    텍스트로 치환되는지 — 이 STEP1은 원래 초기적재(pipeline.index_documents)에만 연결돼 있었다."""
    admrul_repo = tmp_path / "admrul_data"
    img_dir = admrul_repo / "행정규칙" / "표본 행정규칙" / "본문이미지"
    img_dir.mkdir(parents=True)
    (img_dir / "제5조_1.gif").write_bytes(_make_gif())

    payload = {
        "law_id": "A1", "adm_uid": "admrul:A1", "law_name": "표본 행정규칙", "law_type": "고시",
        "doc_target": "admrul", "doc_kind": "행정규칙", "version_uid": "A1:1:20240101",
        "body": {"articles": [{"article_no": "제5조", "article_title": "재질",
                               "provision_id": "admrul:A1#JO0005", "content": "앞부분.[그림]뒷부분."}]},
    }
    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A1", "payload": payload})
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=admrul_repo)
    store, embedder = FakeStore(), FakeEmbedder()

    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())

    assert totals["images_success"] == 1 and totals["images_failed"] == 0
    article_obj = [o for o in _flat(store.upserted) if o.provision_id == "admrul:A1#JO0005"][0]
    assert "[그림]" not in article_obj.content
    assert "전처리로 뽑은 파일 텍스트" in article_obj.content


def test_law_document_upsert_skips_step1_image_ocr(tmp_path):
    """법령류는 STEP1(이미지 OCR)을 건너뛴다 — [그림] 마커가 있어도 그대로 둔다(법령은 이미
    본문에 표·수식이 텍스트로 들어있어 OCR이 중복이라는 기존 결정, pipeline.index_documents와 동일)."""
    payload = _law_payload("L1")
    payload["body"]["articles"][0]["content"] = "앞부분.[그림]뒷부분."
    pkg = _write(tmp_path, _header(source="law"),
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": payload})
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True)
    store, embedder = FakeStore(), FakeEmbedder()

    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())

    assert totals["images_success"] == 0 and totals["images_failed"] == 0
    article_obj = _flat(store.upserted)[0]
    assert "[그림]" in article_obj.content


def test_preprocessed_chunk_indexed_as_file(tmp_path):
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "preprocessed_chunk", "law_id": "L1", "file_id": "f1", "chunk_index": 0,
         "content": "별표 전처리 텍스트", "metadata": {"file_name": "별표.pdf", "unit_type": "APPENDIX",
                                              "provision_id": "law:L1#BYL0001", "page_no": 1}},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)
    assert totals["preprocessed_chunks"] == 1
    objs = _flat(store.upserted)
    file_objs = [o for o in objs if o.source_type == "FILE"]
    assert len(file_objs) == 1 and file_objs[0].content == "별표 전처리 텍스트"
    assert file_objs[0].is_file_only is True and file_objs[0].file_name == "별표.pdf"


def test_file_record_internal_preprocess_on(tmp_path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "f1.pdf").write_text("원본 파일 바이트", encoding="utf-8")
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   package_file_inbox_dir=inbox)
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "file", "law_id": "L1", "file_id": "f1", "file_name": "원문.pdf",
         "transfer_name": "f1.pdf", "unit_type": "DOCUMENT_FILE"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())
    assert totals["files_preprocessed"] == 1 and totals["files_pending"] == 0
    file_objs = [o for o in _flat(store.upserted) if o.source_type == "FILE"]
    assert len(file_objs) == 1 and file_objs[0].content == "전처리로 뽑은 파일 텍스트"
    # 레거시 DOCUMENT_FILE 레코드도 unit_type=FILE 로 정규화되고, 본문 파일이라 file_kind=document_file
    assert file_objs[0].unit_type == "FILE"
    import json as _json
    assert _json.loads(file_objs[0].properties()["meta"])["file_kind"] == "document_file"


def test_consumed_inbox_file_deleted_when_enabled(tmp_path):
    """PACKAGE_DELETE_CONSUMED_FILES=true 면 전처리 성공한 옆채널 파일을 지운다."""
    inbox = tmp_path / "inbox"; inbox.mkdir()
    staged = inbox / "f1.pdf"; staged.write_text("원본 파일 바이트", encoding="utf-8")
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   package_file_inbox_dir=inbox, package_delete_consumed_files=True)
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "file", "law_id": "L1", "file_id": "f1", "file_name": "원문.pdf",
         "transfer_name": "f1.pdf", "unit_type": "DOCUMENT_FILE"},
    )
    totals = consume_package(FakeStore(), FakeEmbedder(), settings, pkg, doc_parser=FakeDocParser())
    assert totals["files_preprocessed"] == 1
    assert not staged.exists()                       # 전달 영역이 쌓이지 않는다


def test_consumed_inbox_file_kept_by_default(tmp_path):
    """기본(off)에서는 지우지 않는다 — 재처리·원인확인 여지를 남긴다."""
    inbox = tmp_path / "inbox"; inbox.mkdir()
    staged = inbox / "f1.pdf"; staged.write_text("원본 파일 바이트", encoding="utf-8")
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   package_file_inbox_dir=inbox, package_delete_consumed_files=False)
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "file", "law_id": "L1", "file_id": "f1", "file_name": "원문.pdf",
         "transfer_name": "f1.pdf", "unit_type": "DOCUMENT_FILE"},
    )
    consume_package(FakeStore(), FakeEmbedder(), settings, pkg, doc_parser=FakeDocParser())
    assert staged.exists()


def test_file_record_content_b64_self_contained(tmp_path):
    """content_b64(자기완결) file record — inbox 없이 base64 디코딩→임시파일→전처리→색인."""
    import base64, hashlib
    data = b"HWP FILE BYTES \x00\x01"
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   package_file_inbox_dir=None)  # inbox 없음(자기완결)
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "file", "law_id": "L1", "file_id": "f1", "file_name": "별표1.hwp",
         "content_b64": base64.b64encode(data).decode("ascii"),
         "sha256": hashlib.sha256(data).hexdigest(),
         "provision_id": "law:L1#BYL0001", "unit_type": "FILE"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())
    assert totals["files_preprocessed"] == 1 and totals["files_pending"] == 0
    file_objs = [o for o in _flat(store.upserted) if o.source_type == "FILE"]
    assert len(file_objs) == 1 and file_objs[0].content == "전처리로 뽑은 파일 텍스트"
    assert file_objs[0].provision_id == "law:L1#BYL0001"  # 별표 첨부


def test_file_record_sha256_mismatch_is_pending(tmp_path):
    """sha256 이 다르면 깨진 파일을 전처리/색인하지 않고 pending 으로 남긴다."""
    import base64
    data = b"CORRUPTED BYTES"
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   package_file_inbox_dir=None)
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "file", "law_id": "L1", "file_id": "f1", "file_name": "별표1.hwp",
         "content_b64": base64.b64encode(data).decode("ascii"),
         "sha256": "not-the-real-sha256",
         "provision_id": "law:L1#BYL0001", "unit_type": "FILE"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())
    assert totals["files_pending"] == 1 and totals["files_preprocessed"] == 0
    assert any("sha256 불일치" in item.get("error", "") for item in totals["errors"])
    assert all(o.source_type != "FILE" for o in _flat(store.upserted))


def test_file_record_pending_when_preprocess_off(tmp_path):
    """내부 전처리 off 면 file 은 보류 — 본문(document)만 색인된다(subcase 5/8)."""
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "file", "law_id": "L1", "file_id": "f1", "file_name": "원문.pdf",
         "transfer_name": "f1.pdf"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)  # 기본 off
    assert totals["files_pending"] == 1 and totals["files_preprocessed"] == 0
    assert all(o.source_type != "FILE" for o in _flat(store.upserted))   # 본문만


def test_pending_attachment_records_but_indexes_body(tmp_path):
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "pending_attachment", "law_id": "L1", "file_id": "f2",
         "reason": "file_not_transferable_and_dmz_preprocess_unavailable"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)
    assert totals["pending_attachments"] == 1 and totals["documents"] == 1
    assert [o.law_id for o in _flat(store.upserted)] == ["L1"]


def test_normalized_chunk_indexed_as_json(tmp_path):
    pkg = _write(
        tmp_path, _header(source="admrul"),
        {"record_type": "document", "op": "upsert", "law_id": "A1",
         "payload": {"law_id": "A1", "adm_uid": "admrul:A1", "revision_date": "20240101",
                     "law_name": "고시A1", "law_type": "고시", "body": {"articles": []}}},
        {"record_type": "normalized_chunk", "law_id": "A1", "chunk_index": 0,
         "content": "미리 정규화된 본문", "metadata": {"unit_type": "ARTICLE", "unit_no": "제1조"}},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)
    assert totals["normalized_chunks"] == 1
    json_objs = [o for o in _flat(store.upserted) if o.source_type == "JSON" and o.unit_no == "제1조"]
    assert len(json_objs) == 1 and json_objs[0].content == "미리 정규화된 본문"


def test_same_law_multiple_records_single_cleanup(tmp_path):
    """한 law_id 에 document+preprocessed+file 이 흩어져 와도 옛 버전 정리(stale)는 딱 1회.
    (개정 경로라 delete_by_law_id 는 안 쓴다 — upsert 먼저, 그다음 stale 1회.)"""
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "preprocessed_chunk", "law_id": "L1", "file_id": "f1", "chunk_index": 0,
         "content": "청크", "metadata": {"file_name": "a.pdf"}},
        {"record_type": "file", "law_id": "L1", "file_id": "f2", "file_name": "b.pdf",
         "transfer_name": "b.pdf"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    consume_package(store, embedder, Settings.from_env(), pkg)
    assert store.deleted.count("L1") == 0
    assert [s[0] for s in store.stale] == ["L1"]


def test_pending_only_does_not_wipe_body(tmp_path):
    """document 없이 pending_attachment 만 온 law_id 는 기존 본문을 지우지 않는다(안전장치)."""
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "pending_attachment", "law_id": "L1", "file_id": "f2", "reason": "held"},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)
    assert store.deleted == [] and store.upserted == [] and store.stale == []
    assert totals["pending_attachments"] == 1


def test_footer_count_mismatch_warns(tmp_path):
    pkg = _write(
        tmp_path, _header(),
        {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")},
        {"record_type": "package_footer", "package_id": "pkg-1", "record_count": 99},
    )
    store, embedder = FakeStore(), FakeEmbedder()
    totals = consume_package(store, embedder, Settings.from_env(), pkg)
    assert any("record_count" in str(e.get("error", "")) for e in totals["errors"])


def test_store_original_writes_document(tmp_path):
    original_dir = tmp_path / "original"
    settings = dataclasses.replace(Settings.from_env(), package_store_original=True,
                                   package_original_dir=original_dir)
    pkg = _write(tmp_path, _header(),
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")})
    store, embedder = FakeStore(), FakeEmbedder()
    consume_package(store, embedder, settings, pkg)
    saved = list((original_dir / "law" / "L1").glob("*.json"))
    assert len(saved) == 1 and json.loads(saved[0].read_text(encoding="utf-8"))["law_id"] == "L1"


def test_source_required_when_no_header_source(tmp_path):
    """header 에 source 없고 --source 도 없으면 실패(어느 컬렉션인지 못 정함)."""
    pkg = _write(tmp_path, {"record_type": "package_header", "package_id": "p"},
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _law_payload("L1")})
    store, embedder = FakeStore(), FakeEmbedder()
    try:
        consume_package(store, embedder, Settings.from_env(), pkg, source_override=None)
        assert False, "source 없으면 ValueError 여야 한다"
    except ValueError:
        pass

# ── 본문이미지를 package 로 전달하는 경로(A 방식) ───────────────────────────
# 배경: 위 test_admrul_document_upsert_runs_step1_image_ocr 는 **로컬 git 데이터 레포**에서
#   이미지를 찾는 경로다. 저장 모드가 db/manifest(무저장)면 그 레포가 없어 OCR 이 통째로
#   건너뛰어졌다(실측: images_success 0). 생산자가 이미지를 package 의 ARTICLE_IMAGE
#   file record 로 실어 보내면 레포 없이도 동작해야 한다 — 아래가 그 계약이다.

def _admrul_payload_with_marker(article_no="제93조", content="앞.[그림]뒤."):
    return {
        "law_id": "A9", "adm_uid": "admrul:A9", "law_name": "이미지 규칙", "law_type": "훈령",
        "doc_target": "admrul", "doc_kind": "행정규칙", "version_uid": "A9:1:20260101",
        "body": {"articles": [{"article_no": article_no, "article_title": "도해",
                               "provision_id": f"admrul:A9#JO0093", "content": content}]},
    }


def _image_record(article_no="제93조", seq=1, name=None, **extra):
    import base64 as _b64
    rec = {"record_type": "file", "law_id": "A9", "file_id": f"img-{article_no}-{seq}",
           "file_name": name or f"{article_no}_{seq}.gif", "unit_type": "ARTICLE_IMAGE",
           "provision_id": "admrul:A9#JO0093", "article_no": article_no, "image_seq": seq,
           "content_b64": _b64.b64encode(_make_gif()).decode("ascii")}
    rec.update(extra)
    return rec


def test_package_image_record_substitutes_marker_without_repo(tmp_path):
    """레포 경로 없이 package 의 ARTICLE_IMAGE record 만으로 [그림] 이 치환된다."""
    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A9",
                  "payload": _admrul_payload_with_marker()},
                 _image_record())
    # admrul_repo_path 를 존재하지 않는 경로로 둔다 — 레포 폴백이 끼어들 여지를 없앤다.
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=tmp_path / "no_such_repo")
    store, embedder = FakeStore(), FakeEmbedder()

    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())

    assert totals["images_success"] == 1 and totals["images_failed"] == 0
    art = [o for o in _flat(store.upserted) if o.provision_id == "admrul:A9#JO0093"][0]
    assert "[그림]" not in art.content
    assert "전처리로 뽑은 파일 텍스트" in art.content


def test_package_image_record_is_not_indexed_as_separate_chunk(tmp_path):
    """이미지는 조문 본문에 녹아들 뿐, 별개 FILE 청크로 들어가지 않는다."""
    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A9",
                  "payload": _admrul_payload_with_marker()},
                 _image_record())
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=tmp_path / "no_such_repo")
    store, embedder = FakeStore(), FakeEmbedder()

    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())

    # 첨부 전처리 카운터가 올라가면 이미지가 첨부 경로로 새어 들어간 것이다.
    assert totals["files_preprocessed"] == 0 and totals["files_pending"] == 0
    assert [o.unit_type for o in _flat(store.upserted)] == ["ARTICLE"]


def test_package_images_substituted_in_seq_order(tmp_path):
    """`[그림]` 이 여러 개면 image_seq 순서대로 채워진다(record 순서가 뒤섞여도)."""
    class SeqDocParser:
        """호출된 파일명을 텍스트로 돌려주는 전처리기 — 순서 검증용."""
        def run(self, request_path, chunk_size, chunk_overlap, endpoint_path=None):
            return [{"text": f"OCR<{Path(request_path).name}>", "i_chunk_on_doc": 0,
                     "i_page": 1, "e_page": 1}]

    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A9",
                  "payload": _admrul_payload_with_marker(content="A[그림]B[그림]C")},
                 _image_record(seq=2),                     # 일부러 역순으로 싣는다
                 _image_record(seq=1))
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=tmp_path / "no_such_repo")
    store, embedder = FakeStore(), FakeEmbedder()

    totals = consume_package(store, embedder, settings, pkg, doc_parser=SeqDocParser())

    assert totals["images_success"] == 2
    art = [o for o in _flat(store.upserted) if o.provision_id == "admrul:A9#JO0093"][0]
    # 확장자는 신경쓰지 않는다 — GIF 는 Doc Parser 가 못 읽어 PNG 로 자동 변환된다
    # (convert_image_for_doc_parser). 검증 대상은 **치환 순서**다.
    assert art.content == "AOCR<제93조_1.png>BOCR<제93조_2.png>C", art.content


def test_package_image_ocr_failure_keeps_marker(tmp_path):
    """OCR 이 빈 텍스트를 주면 그 자리는 [그림] 마커로 남고 실패로 계상된다.

    조문 전체를 실패로 치지 않는다 — 나머지 본문은 그대로 색인돼야 한다."""
    class EmptyDocParser:
        def run(self, request_path, chunk_size, chunk_overlap, endpoint_path=None):
            return [{"text": "", "i_chunk_on_doc": 0, "i_page": 1, "e_page": 1}]

    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A9",
                  "payload": _admrul_payload_with_marker()},
                 _image_record())
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=tmp_path / "no_such_repo")
    store, embedder = FakeStore(), FakeEmbedder()

    totals = consume_package(store, embedder, settings, pkg, doc_parser=EmptyDocParser())

    assert totals["images_success"] == 0 and totals["images_failed"] == 1
    art = [o for o in _flat(store.upserted) if o.provision_id == "admrul:A9#JO0093"][0]
    assert "[그림]" in art.content and "앞." in art.content


def test_package_image_uses_image_endpoint(tmp_path):
    """이미지는 **이미지 전용** 엔드포인트로 간다 — 문서용(첨부)과 모델이 다르다."""
    seen = []

    class RouteRecorder:
        def run(self, request_path, chunk_size, chunk_overlap, endpoint_path=None):
            seen.append(endpoint_path)
            return [{"text": "이미지 텍스트", "i_chunk_on_doc": 0, "i_page": 1, "e_page": 1}]

    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A9",
                  "payload": _admrul_payload_with_marker()},
                 _image_record())
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=tmp_path / "no_such_repo",
                                   doc_parser_image_endpoint_path="/preprocess_intelligent")
    consume_package(FakeStore(), FakeEmbedder(), settings, pkg, doc_parser=RouteRecorder())

    assert seen == ["/preprocess_intelligent"], seen


def test_package_image_bad_base64_counted_not_crash(tmp_path):
    """content_b64 가 깨져도 문서 전체가 죽지 않고 그 그림만 실패로 남는다."""
    bad = _image_record()
    bad["content_b64"] = "!!!not-base64!!!"
    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A9",
                  "payload": _admrul_payload_with_marker()},
                 bad)
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=tmp_path / "no_such_repo")
    store, embedder = FakeStore(), FakeEmbedder()

    totals = consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())

    assert totals["documents"] == 1 and totals["images_failed"] == 1
    art = [o for o in _flat(store.upserted) if o.provision_id == "admrul:A9#JO0093"][0]
    assert "[그림]" in art.content


def test_other_markers_untouched(tmp_path):
    """`[별표1]`·`[서식]` 같은 텍스트 참조 마커는 이미지가 아니라 절대 건드리지 않는다."""
    payload = _admrul_payload_with_marker(content="[별표1] 참조. 그림은 [그림] 이다. [서식] 도 있다.")
    pkg = _write(tmp_path, _header(source="admrul"),
                 {"record_type": "document", "op": "upsert", "law_id": "A9", "payload": payload},
                 _image_record())
    settings = dataclasses.replace(Settings.from_env(), package_preprocess_files=True,
                                   admrul_repo_path=tmp_path / "no_such_repo")
    store, embedder = FakeStore(), FakeEmbedder()

    consume_package(store, embedder, settings, pkg, doc_parser=FakeDocParser())

    art = [o for o in _flat(store.upserted) if o.provision_id == "admrul:A9#JO0093"][0]
    assert "[별표1]" in art.content and "[서식]" in art.content
    assert "[그림]" not in art.content
