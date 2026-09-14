import json
from io import BytesIO
from pathlib import Path

from law_indexer.config import Settings
from law_indexer.pipeline import index_documents, resolve_article_images
from law_indexer.preprocess import DocParserError

FIXTURES = Path(__file__).parent / "fixtures"


def _make_gif(width: int = 24, height: int = 24) -> bytes:
    """PIL 이 실제로 열 수 있는 유효 GIF 바이트. 초소형 글리프 컷(MIN_ARTICLE_IMAGE_SIDE)에
    걸리지 않도록 충분히 크게 만든다 — 이 테스트는 [그림] 대체 로직을 검증하는 것이지 컷을
    검증하는 게 아니다."""
    from PIL import Image
    buf = BytesIO()
    Image.new("RGB", (width, height), "white").save(buf, format="GIF")
    return buf.getvalue()


_MINIMAL_GIF = _make_gif()


class _FakeEmbedder:
    model_name = "fake-arctic"
    dimension = 4

    def embed_documents(self, texts):
        return [[0.1, 0.2, 0.3, 0.4] for _ in texts]


class _FakeStore:
    """WeaviateStore 대역 — 호출 로그(self.upserts)와 실제 저장 상태(self._state)를 분리해서
    관리한다. 실제 WeaviateStore.upsert 는 같은 chunk_id(=같은 UUID)를 delete_by_id 후 다시
    add 하므로(멱등 교체), 이 대역도 같은 chunk_id 는 최신 객체로 교체되게 흉내낸다 — 그래야
    §23-7 고아 청크 정리 테스트가 실제 동작과 같은 조건에서 검증된다."""

    def __init__(self):
        self.upserts = []
        self._state = {}  # collection -> {chunk_id: LegalProvision}

    def upsert(self, objects, dimension, model_name, collection):
        self.upserts.append({"objects": list(objects), "collection": collection, "model_name": model_name})
        bucket = self._state.setdefault(collection, {})
        for obj in objects:
            bucket[obj.chunk_id] = obj
        return {"success": len(objects), "failed": 0, "failed_ids": []}

    def objects_in(self, collection):
        return list(self._state.get(collection, {}).values())

    def delete_orphan_file_chunks(self, file_id, keep_chunk_ids, collection):
        """실제 WeaviateStore.delete_orphan_file_chunks 대역 — 같은 file_id 의 저장된 청크 중
        keep_chunk_ids 에 없는 것만 걷어낸다(§23-7 재처리 시 청크 수 감소 시나리오 재현용)."""
        keep = set(keep_chunk_ids)
        bucket = self._state.get(collection, {})
        to_remove = [cid for cid, obj in bucket.items() if obj.file_id == file_id and cid not in keep]
        for cid in to_remove:
            del bucket[cid]
        return len(to_remove)

    def delete_orphan_json_chunks(self, law_id, version_uid, keep_chunk_ids, collection):
        """OCR 재색인처럼 JSON 청크 수가 줄어든 경우 같은 문서 버전의 옛 JSON 청크만 정리한다."""
        keep = set(keep_chunk_ids)
        bucket = self._state.get(collection, {})
        to_remove = [
            cid for cid, obj in bucket.items()
            if obj.law_id == law_id and obj.version_uid == version_uid
            and obj.source_type == "JSON" and cid not in keep
        ]
        for cid in to_remove:
            del bucket[cid]
        return len(to_remove)


class _FakeDocParser:
    """DocParserClient 대역 — 실제 HTTP 호출 없이 성공/실패를 시뮬레이션한다."""

    def __init__(self, chunks=None, error=None):
        self.chunks = chunks if chunks is not None else [
            {"text": "전처리된 텍스트", "i_page": 1, "e_page": 1, "i_chunk_on_doc": 0}]
        self.error = error
        self.calls = []

    def run(self, file_path, chunk_size, chunk_overlap, endpoint_path=None):
        self.calls.append(file_path)
        if self.error:
            raise self.error
        return self.chunks


def _write_law_doc(repo_root: Path, *, with_file_only_appendix: bool, create_local_file: bool) -> Path:
    """temporal_law/collector/mdexport.py 의 law_dir 규칙(가족/유형)을 따르는 임시 법령 저장소를 만든다."""
    doc_dir = repo_root / "표본법" / "시행령"
    doc_dir.mkdir(parents=True)
    appendices = []
    if with_file_only_appendix:
        appendices.append({
            "no": "0001", "branch": "00", "kind": "별표", "title": "기준표", "content": "",
            "is_file_only": True, "files": [{"type": "hwp", "url": "https://x", "name": ""}],
            "provision_id": "law:표본법시행령#BYL0001",
        })
        if create_local_file:
            (doc_dir / "별표").mkdir(parents=True)
            (doc_dir / "별표" / "별표1_기준표.hwp").write_bytes(b"hwp")
    data = {
        "law_id": "l1", "mst": "m1", "version_uid": "l1:m1:20240101", "law_name": "표본법 시행령",
        "law_type": "시행령", "enforcement_date": "20240101",
        "body": {"format": "articles", "article_count": 1, "articles": [{
            "provision_id": "law:표본법시행령#JO0001", "article_no": "제1조", "article_title": "목적",
            "content": "제1조(목적) 본문.", "relations": [],
        }]},
        "addenda": [], "appendices": appendices,
    }
    path = doc_dir / "표본법 시행령.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return path


def _write_admrul_doc(repo_root: Path) -> Path:
    doc_dir = repo_root / "행정규칙" / "표본 행정규칙"
    doc_dir.mkdir(parents=True)
    data = json.loads((FIXTURES / "admrul.json").read_text(encoding="utf-8"))
    path = doc_dir / "표본 행정규칙.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return path


def _write_document_level_file_only_admrul_doc(repo_root: Path, *, create_local_file: bool) -> Path:
    """본문(body.articles)이 비어있고 최상위 attachments[] 만 있는 실제 admrul_data 패턴
    (예: "2016년 기록물 관리지침")을 재현한다."""
    doc_dir = repo_root / "행정규칙" / "표본 원문전용 행정규칙"
    doc_dir.mkdir(parents=True)
    if create_local_file:
        (doc_dir / "첨부파일").mkdir(parents=True)
        (doc_dir / "첨부파일" / "표본원문.pdf").write_bytes(b"pdf")
    data = {
        "law_id": "a1", "mst": "m1", "law_name": "표본 원문전용 행정규칙", "law_type": "고시",
        "doc_target": "admrul", "doc_kind": "행정규칙", "adm_uid": "admrul:a1",
        "enforcement_date": "20240101", "revision_date": "20240101",
        "body": {"format": "articles", "article_count": 0, "articles": []},
        "addenda": [], "appendices": [],
        "attachments": [{"name": "표본원문.pdf", "url": "https://x", "filename": "표본원문.pdf"}],
    }
    path = doc_dir / "표본 원문전용 행정규칙.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return path


def test_is_file_only_false_indexes_without_doc_parser(tmp_path):
    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=False, create_local_file=False)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    doc_parser = _FakeDocParser()

    result = index_documents(store, embedder, settings, repo_root, "law", repo_root, "abc123",
                             "https://github.com/genonai/law_data.git", recursive=True, doc_parser=doc_parser)

    assert result["files_success"] == 1 and result["files_failed"] == 0
    assert doc_parser.calls == []  # 전처리기가 아예 호출되지 않아야 한다
    objs = store.objects_in(settings.law_collection)
    assert len(objs) == 1 and objs[0].unit_type == "ARTICLE"
    assert objs[0].collection_type == "law" and objs[0].git_commit == "abc123"


def test_is_file_only_true_runs_doc_parser_and_produces_file_chunk(tmp_path):
    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=True, create_local_file=True)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    doc_parser = _FakeDocParser()

    result = index_documents(store, embedder, settings, repo_root, "law", repo_root, "abc123",
                             "https://github.com/genonai/law_data.git", recursive=True, doc_parser=doc_parser)

    assert result["files_failed"] == 0  # delete_orphan_file_chunks 호출이 문서 처리를 실패시키면 안 됨
    assert result["attachments_success"] == 1 and result["attachments_failed"] == 0
    assert len(doc_parser.calls) == 1
    file_objs = [o for o in store.objects_in(settings.law_collection) if o.source_type == "FILE"]
    assert len(file_objs) == 1
    assert file_objs[0].provision_id == "law:표본법시행령#BYL0001"
    assert file_objs[0].is_file_only is True


def test_reprocessing_with_fewer_chunks_deletes_orphan_file_chunks(tmp_path):
    """§23-7: 같은 파일을 재처리했을 때 청크 수가 줄면(예: Doc Parser 파라미터 변경) 예전
    청크가 안 지워지고 남아있으면 안 된다."""
    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=True, create_local_file=True)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()

    first_parser = _FakeDocParser(chunks=[
        {"text": "첫 페이지", "i_page": 1, "e_page": 1, "i_chunk_on_doc": 0},
        {"text": "둘째 페이지", "i_page": 2, "e_page": 2, "i_chunk_on_doc": 1},
        {"text": "셋째 페이지", "i_page": 3, "e_page": 3, "i_chunk_on_doc": 2},
    ])
    index_documents(store, embedder, settings, repo_root, "law", repo_root, "abc123",
                    "https://github.com/genonai/law_data.git", recursive=True, doc_parser=first_parser)
    file_objs = [o for o in store.objects_in(settings.law_collection) if o.source_type == "FILE"]
    assert len(file_objs) == 3

    second_parser = _FakeDocParser(chunks=[
        {"text": "합쳐진 한 페이지", "i_page": 1, "e_page": 3, "i_chunk_on_doc": 0},
    ])
    result = index_documents(store, embedder, settings, repo_root, "law", repo_root, "abc123",
                             "https://github.com/genonai/law_data.git", recursive=True, doc_parser=second_parser)

    assert result["files_failed"] == 0
    file_objs_after = [o for o in store.objects_in(settings.law_collection) if o.source_type == "FILE"]
    assert len(file_objs_after) == 1  # 3개였던 예전 청크가 고아로 남지 않고 1개로 정리됨
    assert file_objs_after[0].content == "합쳐진 한 페이지"


def test_is_file_only_true_missing_file_does_not_abort_batch(tmp_path):
    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=True, create_local_file=False)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    doc_parser = _FakeDocParser()

    result = index_documents(store, embedder, settings, repo_root, "law", repo_root, "abc123",
                             "https://github.com/genonai/law_data.git", recursive=True, doc_parser=doc_parser)

    assert result["files_failed"] == 0  # 문서 자체는 실패 처리되지 않는다
    assert result["attachments_failed"] == 1
    assert "찾을 수 없습니다" in result["errors"][0]["error"]
    assert doc_parser.calls == []  # 파일이 없어 호출조차 되지 않는다
    assert any(o.unit_type == "ARTICLE" for o in store.objects_in(settings.law_collection))


def test_doc_parser_failure_logs_clear_error_without_aborting(tmp_path):
    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=True, create_local_file=True)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    doc_parser = _FakeDocParser(error=DocParserError("TIMEOUT", "Doc Parser timeout(60s)"))

    result = index_documents(store, embedder, settings, repo_root, "law", repo_root, "abc123",
                             "https://github.com/genonai/law_data.git", recursive=True, doc_parser=doc_parser)

    assert result["attachments_failed"] == 1
    assert "[TIMEOUT]" in result["errors"][-1]["error"]
    assert result["files_success"] == 1  # 첨부 실패가 문서 처리 자체를 막지 않는다


def test_admrul_document_indexes_without_law_version_uid(tmp_path):
    repo_root = tmp_path / "admrul_data"
    _write_admrul_doc(repo_root)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()

    result = index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "def456",
                             "https://github.com/genonai/admrul_data.git", recursive=True,
                             doc_parser=_FakeDocParser())

    assert result["files_failed"] == 0
    objs = store.objects_in(settings.admrul_collection)
    assert objs and all(o.collection_type == "admrul" for o in objs)
    assert all(o.version_uid for o in objs)  # version_uid 없어도 adm_uid 로 대체돼 채워진다


def test_law_and_admrul_never_cross_collections(tmp_path):
    law_root = tmp_path / "law_data"
    _write_law_doc(law_root, with_file_only_appendix=False, create_local_file=False)
    admrul_root = tmp_path / "admrul_data"
    _write_admrul_doc(admrul_root)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()

    index_documents(store, embedder, settings, law_root, "law", law_root, "c1",
                    "https://github.com/genonai/law_data.git", recursive=True, doc_parser=_FakeDocParser())
    index_documents(store, embedder, settings, admrul_root, "admrul", admrul_root, "c2",
                    "https://github.com/genonai/admrul_data.git", recursive=True, doc_parser=_FakeDocParser())

    law_objs = store.objects_in(settings.law_collection)
    admrul_objs = store.objects_in(settings.admrul_collection)
    assert law_objs and all(o.collection_type == "law" for o in law_objs)
    assert admrul_objs and all(o.collection_type == "admrul" for o in admrul_objs)


def test_both_collections_use_same_embedding_model(tmp_path):
    law_root = tmp_path / "law_data"
    _write_law_doc(law_root, with_file_only_appendix=False, create_local_file=False)
    admrul_root = tmp_path / "admrul_data"
    _write_admrul_doc(admrul_root)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()

    index_documents(store, embedder, settings, law_root, "law", law_root, "c1",
                    "https://github.com/genonai/law_data.git", recursive=True, doc_parser=_FakeDocParser())
    index_documents(store, embedder, settings, admrul_root, "admrul", admrul_root, "c2",
                    "https://github.com/genonai/admrul_data.git", recursive=True, doc_parser=_FakeDocParser())

    assert store.upserts  # 두 컬렉션 모두 upsert가 일어났고
    assert all(u["model_name"] == embedder.model_name for u in store.upserts)  # 같은 임베딩 모델을 썼다


def test_document_level_file_only_processes_top_level_attachment(tmp_path):
    repo_root = tmp_path / "admrul_data"
    _write_document_level_file_only_admrul_doc(repo_root, create_local_file=True)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    doc_parser = _FakeDocParser()

    result = index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "c1",
                             "https://github.com/genonai/admrul_data.git", recursive=True, doc_parser=doc_parser)

    assert result["attachments_success"] == 1 and result["attachments_failed"] == 0
    assert len(doc_parser.calls) == 1
    file_objs = [o for o in store.objects_in(settings.admrul_collection) if o.source_type == "FILE"]
    assert len(file_objs) == 1
    assert file_objs[0].unit_type == "FILE"
    assert file_objs[0].is_file_only is True


def test_document_level_file_only_processes_all_distinct_files(tmp_path):
    """STEP3(§2 공통 규칙): 조문0 문서에 서로 다른 실제 문서(원문+이유서)가 섞여 있으면 role
    구분 없이 둘 다 처리해야 한다 — 하나만 고르면 내용이 유실된다(실측 다수 확인)."""
    repo_root = tmp_path / "admrul_data"
    doc_dir = repo_root / "행정규칙" / "표본 원문전용 행정규칙"
    doc_dir.mkdir(parents=True)
    (doc_dir / "첨부파일").mkdir(parents=True)
    (doc_dir / "첨부파일" / "고시문.pdf").write_bytes(b"pdf1")
    (doc_dir / "첨부파일" / "조문별제개정이유서.hwp").write_bytes(b"hwp1")
    (doc_dir / "첨부파일" / "조문별제개정이유서.pdf").write_bytes(b"pdf2")  # hwp 와 형식 중복 — 1개로 합쳐져야 함
    data = {
        "law_id": "a1", "mst": "m1", "law_name": "표본 원문전용 행정규칙", "law_type": "고시",
        "doc_target": "admrul", "doc_kind": "행정규칙", "adm_uid": "admrul:a1",
        "enforcement_date": "20240101", "revision_date": "20240101",
        "body": {"format": "articles", "article_count": 0, "articles": []},
        "addenda": [], "appendices": [],
        "attachments": [{"filename": "고시문.pdf"}, {"filename": "조문별제개정이유서.pdf"},
                        {"filename": "조문별제개정이유서.hwp"}],
    }
    (doc_dir / "표본 원문전용 행정규칙.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    doc_parser = _FakeDocParser()

    result = index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "c1",
                             "https://github.com/genonai/admrul_data.git", recursive=True, doc_parser=doc_parser)

    assert result["attachments_success"] == 2 and result["attachments_failed"] == 0
    file_objs = [o for o in store.objects_in(settings.admrul_collection) if o.source_type == "FILE"]
    assert {o.file_name for o in file_objs} == {"고시문.pdf", "조문별제개정이유서.hwp"}  # pdf 형식중복은 제외됨


def test_document_level_file_only_missing_file_does_not_abort_batch(tmp_path):
    repo_root = tmp_path / "admrul_data"
    _write_document_level_file_only_admrul_doc(repo_root, create_local_file=False)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()

    result = index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "c1",
                             "https://github.com/genonai/admrul_data.git", recursive=True,
                             doc_parser=_FakeDocParser())

    assert result["files_failed"] == 0  # 문서(빈 JSON 자체)는 오류 아님
    assert result["attachments_failed"] == 1
    assert "찾을 수 없습니다" in result["errors"][0]["error"]


# ── STEP1: 조문 본문이미지([그림] 마커) OCR 병합 ───────────────────────────

def _write_admrul_doc_with_article_images(repo_root: Path, *, create_images: bool) -> Path:
    """실제 admrul_data(피난유도선 성능인증...)처럼 제N조 content 안에 [그림] 마커가 있고
    본문이미지/{article_no}_{순번}.gif 로 대응 이미지가 있는 문서를 재현한다."""
    doc_dir = repo_root / "행정규칙" / "표본 행정규칙"
    doc_dir.mkdir(parents=True)
    if create_images:
        img_dir = doc_dir / "본문이미지"
        img_dir.mkdir(parents=True)
        (img_dir / "제5조_1.gif").write_bytes(_MINIMAL_GIF)
        (img_dir / "제5조_2.gif").write_bytes(_MINIMAL_GIF)
    data = {
        "law_id": "a1", "mst": "m1", "law_name": "표본 행정규칙", "law_type": "고시",
        "doc_target": "admrul", "doc_kind": "행정규칙", "adm_uid": "admrul:a1",
        "enforcement_date": "20240101", "revision_date": "20240101",
        "body": {"format": "articles", "article_count": 1, "articles": [{
            "provision_id": "admrul:표본#JO0005", "article_no": "제5조", "article_title": "재질",
            "content": "제5조(재질) 앞부분.[그림]가운데 부분.[그림]뒷부분.", "relations": [],
        }]},
        "addenda": [], "appendices": [],
    }
    path = doc_dir / "표본 행정규칙.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return path


def test_resolve_article_images_replaces_markers_in_order(tmp_path):
    repo_root = tmp_path / "admrul_data"
    _write_admrul_doc_with_article_images(repo_root, create_images=True)
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙"}
    article = {"article_no": "제5조", "content": "앞.[그림]뒤1.[그림]뒤2."}
    doc_parser = _FakeDocParser(chunks=[{"text": "첫 그림 OCR", "i_chunk_on_doc": 0}])
    settings = Settings.from_env()

    new_content, success, failed, _transient = resolve_article_images(doc_parser, settings, repo_root, data, article)

    assert success == 2 and failed == 0
    assert new_content == "앞.첫 그림 OCR뒤1.첫 그림 OCR뒤2."  # 같은 FakeDocParser라 텍스트는 같지만 순서대로 치환됨
    assert len(doc_parser.calls) == 2  # 이미지 2개 각각 호출


def test_resolve_article_images_no_marker_returns_none(tmp_path):
    settings = Settings.from_env()
    result = resolve_article_images(_FakeDocParser(), settings, tmp_path, {}, {"content": "그림 없음"})
    assert result == (None, 0, 0, 0)


def test_resolve_article_images_missing_files_leaves_marker(tmp_path):
    repo_root = tmp_path / "admrul_data"
    (repo_root / "행정규칙" / "표본 행정규칙").mkdir(parents=True)  # 본문이미지 폴더 없음
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙"}
    article = {"article_no": "제5조", "content": "앞.[그림]뒤."}
    settings = Settings.from_env()

    new_content, success, failed, _transient = resolve_article_images(_FakeDocParser(), settings, repo_root, data, article)

    assert new_content is None and success == 0 and failed == 0  # 이미지 자체가 없으면 원문 그대로


def test_resolve_article_images_ocr_failure_keeps_marker_for_that_image(tmp_path):
    repo_root = tmp_path / "admrul_data"
    _write_admrul_doc_with_article_images(repo_root, create_images=True)
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙"}
    article = {"article_no": "제5조", "content": "앞.[그림]뒤1.[그림]뒤2."}
    doc_parser = _FakeDocParser(error=DocParserError("TIMEOUT", "timeout"))
    settings = Settings.from_env()

    new_content, success, failed, _transient = resolve_article_images(doc_parser, settings, repo_root, data, article)

    assert success == 0 and failed == 2
    assert new_content is None  # 전부 실패하면 원문(마커 그대로) 유지


def test_index_documents_merges_article_images_into_content_and_search_text(tmp_path):
    """STEP1이 index_documents 전체 파이프라인에 실제로 연결돼 있는지: 매핑 전에 raw 를
    고쳐야 최종 저장된 ARTICLE 객체의 content·search_text 에 반영된다."""
    repo_root = tmp_path / "admrul_data"
    _write_admrul_doc_with_article_images(repo_root, create_images=True)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    doc_parser = _FakeDocParser(chunks=[{"text": "표1 OCR 텍스트", "i_chunk_on_doc": 0}])

    result = index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "c1",
                             "https://github.com/genonai/admrul_data.git", recursive=True, doc_parser=doc_parser)

    assert result["files_failed"] == 0
    assert result["images_success"] == 2 and result["images_failed"] == 0
    article_obj = next(o for o in store.objects_in(settings.admrul_collection) if o.unit_type == "ARTICLE")
    assert "[그림]" not in article_obj.content
    assert article_obj.content.count("표1 OCR 텍스트") == 2
    assert "표1 OCR 텍스트" in article_obj.search_text


def test_index_documents_missing_article_images_does_not_fail_document(tmp_path):
    repo_root = tmp_path / "admrul_data"
    _write_admrul_doc_with_article_images(repo_root, create_images=False)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()

    result = index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "c1",
                             "https://github.com/genonai/admrul_data.git", recursive=True,
                             doc_parser=_FakeDocParser())

    assert result["files_failed"] == 0
    article_obj = next(o for o in store.objects_in(settings.admrul_collection) if o.unit_type == "ARTICLE")
    assert "[그림]" in article_obj.content  # 이미지 없으면 마커 그대로 남아 임베딩된다


def test_reindexing_same_document_yields_same_chunk_ids(tmp_path):
    """chunk_id(→ object_uuid)가 재실행 후에도 같아야 실제 Weaviate upsert가 교체(중복 없음)한다."""
    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=False, create_local_file=False)
    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()

    index_documents(store, embedder, settings, repo_root, "law", repo_root, "c1",
                    "https://github.com/genonai/law_data.git", recursive=True, doc_parser=_FakeDocParser())
    index_documents(store, embedder, settings, repo_root, "law", repo_root, "c1",
                    "https://github.com/genonai/law_data.git", recursive=True, doc_parser=_FakeDocParser())

    ids_first = {o.chunk_id for o in store.upserts[0]["objects"]}
    ids_second = {o.chunk_id for o in store.upserts[1]["objects"]}
    assert ids_first == ids_second


def test_reindexing_json_with_fewer_chunks_deletes_orphan_json_chunks(tmp_path):
    """OCR 재색인 등으로 JSON 본문 청크 수가 줄면 예전 조각이 검색에 남으면 안 된다."""
    repo_root = tmp_path / "admrul_data"
    path = _write_admrul_doc_with_article_images(repo_root, create_images=False)
    data = json.loads(path.read_text(encoding="utf-8"))
    data["body"]["articles"][0]["content"] = "\n".join([
        "① " + "가" * 1500,
        "② " + "나" * 1500,
        "③ " + "다" * 1500,
    ])
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    store, embedder, settings = _FakeStore(), _FakeEmbedder(), Settings.from_env()
    index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "c1",
                    "https://github.com/genonai/admrul_data.git", recursive=True,
                    doc_parser=_FakeDocParser())
    article_objs = [o for o in store.objects_in(settings.admrul_collection) if o.unit_type == "ARTICLE"]
    assert len(article_objs) == 3

    data["body"]["articles"][0]["content"] = "짧아진 OCR 반영 본문"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    result = index_documents(store, embedder, settings, repo_root, "admrul", repo_root, "c1",
                             "https://github.com/genonai/admrul_data.git", recursive=True,
                             doc_parser=_FakeDocParser())

    assert result["files_failed"] == 0
    article_objs_after = [o for o in store.objects_in(settings.admrul_collection) if o.unit_type == "ARTICLE"]
    assert len(article_objs_after) == 1
    assert article_objs_after[0].content == "짧아진 OCR 반영 본문"


def test_index_documents_records_repo_relative_git_path(tmp_path, monkeypatch):
    """`git_path` 는 **데이터 레포 상대경로**다 — 초기 전량 색인도 증분과 같은 뜻이어야 한다.

    예전엔 초기 색인만 `source.resolve()`(절대경로)를 넣어, 같은 필드가 적재 경로에 따라 뜻이
    달랐고 색인한 머신의 절대경로가 Weaviate 에 박혔다(증분 package 는 레포 상대경로).
    """
    import json as _json

    from law_indexer import pipeline as pl

    repo = tmp_path / "LAW"
    doc_dir = repo / "도로교통법" / "법률"
    doc_dir.mkdir(parents=True)
    payload = {
        "law_id": "001627", "mst": "111", "version_uid": "law:eflaw:001627:111:20260101",
        "law_name": "도로교통법", "law_type": "법률",
        "body": {"format": "articles", "articles": [
            {"article_no": "제1조", "article_title": "목적", "content": "이 법은 …",
             "provision_id": "law:도로교통법#JO0001"}]},
        "source": {"source_url": "https://law.go.kr/"},
    }
    json_path = doc_dir / "도로교통법.json"
    json_path.write_text(_json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    captured = []

    class _Store:
        def upsert(self, objects, dimension, model_name, collection):
            captured.extend(objects)
            return {"success": len(objects), "failed": 0}

        def count(self, collection):
            return len(captured)

        def has_chunks(self, *a, **k):
            return False

        def delete_stale_law_chunks(self, *a, **k):
            return 0

    class _Embedder:
        model_name = "test-model"
        dimension = 2

        def embed_documents(self, texts):
            return [[0.0, 1.0] for _ in texts]

    monkeypatch.setenv("DOC_PARSER_BASE_URL", "")
    monkeypatch.setenv("INDEX_PREPROCESS_FILES", "false")
    settings = pl.Settings.from_env()
    pl.index_documents(_Store(), _Embedder(), settings, repo, "law", repo, None, None, recursive=True)

    assert captured, "청크가 만들어지지 않았다"
    assert captured[0].git_path == "도로교통법/법률/도로교통법.json", captured[0].git_path


def test_request_overrides_beat_env(tmp_path, monkeypatch):
    """Temporal IndexRequest 의 요청 단위 오버라이드(preprocess_files·embed_flush)가 env 보다
    우선한다 — 워커 재시작 없이 요청만 바꿔 1단계(본문만)/2단계(전처리) 를 오갈 수 있어야 한다."""
    import law_indexer.pipeline as pl

    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=False, create_local_file=False)
    store, embedder = _FakeStore(), _FakeEmbedder()

    # env 는 전처리 on + base_url 있음이지만, 요청이 preprocess_files=False 로 끈다.
    monkeypatch.setenv("INDEX_PREPROCESS_FILES", "true")
    monkeypatch.setenv("DOC_PARSER_BASE_URL", "http://parser.example")
    monkeypatch.setenv("INDEX_EMBED_FLUSH", "999")
    settings = Settings.from_env()

    created = []
    monkeypatch.setattr(pl, "DocParserClient",
                        lambda *a, **k: created.append(1) or _FakeDocParser())

    flushes = []
    real_bulk = pl._BulkBuffer

    def _spy_bulk(*args, **kwargs):
        flushes.append(kwargs.get("min_flush"))
        return real_bulk(*args, **kwargs)

    monkeypatch.setattr(pl, "_BulkBuffer", _spy_bulk)

    result = index_documents(store, embedder, settings, repo_root, "law", repo_root, "abc123",
                             None, recursive=True, preprocess_files=False, embed_flush=64)

    assert result["files_success"] == 1
    assert created == []          # 오버라이드가 env(true)를 이겨 전처리기 미생성
    assert flushes == [64]        # embed_flush 오버라이드가 env(999)를 이김

    # 반대 방향: env 가 꺼져 있어도 요청이 켜면 전처리기가 생성된다.
    monkeypatch.setenv("INDEX_PREPROCESS_FILES", "false")
    settings2 = Settings.from_env()
    index_documents(store, embedder, settings2, repo_root, "law", repo_root, "abc123",
                    None, recursive=True, preprocess_files=True)
    assert created == [1]


def test_index_request_override_fields_roundtrip():
    """IndexRequest 신규 필드 기본값 — 워커/genos 어디서든 같은 요청이면 같은 동작."""
    from law_indexer.worker_workflows import IndexRequest
    req = IndexRequest()
    assert req.skip_existing is False and req.preprocess_files is None and req.embed_flush is None
    req2 = IndexRequest(source="admrul", skip_existing=True, preprocess_files=False, embed_flush=384)
    assert (req2.skip_existing, req2.preprocess_files, req2.embed_flush) == (True, False, 384)


def test_skip_existing_requires_complete_document(tmp_path):
    """skip = **완전한 문서만**. 기대 JSON 청크가 전부 있으면 skip, 하나라도 빠지면(부분 실패
    문서 — 실측 민법 400청크 누락 시나리오) 재색인한다."""
    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=False, create_local_file=False)
    settings = Settings.from_env()
    embedder = _FakeEmbedder()

    class _Store(_FakeStore):
        def __init__(self, missing_all: bool):
            super().__init__()
            self.missing_all = missing_all
            self.asked = []

        def missing_ids(self, uuids, collection):
            self.asked.append(list(uuids))
            return set(uuids) if self.missing_all else set()

    # ① 기대 청크 전부 존재 → skip (업서트 0회)
    complete = _Store(missing_all=False)
    result = index_documents(complete, embedder, settings, repo_root, "law", repo_root, "c1",
                             None, recursive=True, skip_existing=True)
    assert result["files_skipped"] == 1 and not complete.upserts
    assert complete.asked and complete.asked[0], "기대 chunk UUID 로 대조해야 한다"

    # ② 빠진 청크 존재(부분 실패 문서) → skip 하지 않고 재색인
    partial = _Store(missing_all=True)
    result = index_documents(partial, embedder, settings, repo_root, "law", repo_root, "c1",
                             None, recursive=True, skip_existing=True)
    assert result["files_skipped"] == 0 and partial.upserts
    assert result["files_success"] == 1


def test_find_preprocess_targets(tmp_path):
    """2단계 대상 추출 — [그림] 마커(admrul)·is_file_only 별표만 잡고 평범한 문서는 제외.
    법령은 [그림] OCR off 정책이라 마커가 있어도 대상이 아니다."""
    from law_indexer.pipeline import find_preprocess_targets

    law_root = tmp_path / "LAW"
    _write_law_doc(law_root, with_file_only_appendix=True, create_local_file=False)   # 파일전용 별표
    plain_dir = law_root / "평범법" / "법률"
    plain_dir.mkdir(parents=True)
    plain = {"law_id": "p1", "mst": "m", "version_uid": "p1:m:20240101", "law_name": "평범법",
             "law_type": "법률", "enforcement_date": "20240101",
             "body": {"format": "articles", "article_count": 1, "articles": [
                 {"provision_id": "law:평범법#JO0001", "article_no": "제1조", "article_title": "목적",
                  "content": "제1조(목적) [그림] 이 있어도 법령은 대상 아님.", "relations": []}]},
             "addenda": [], "appendices": []}
    (plain_dir / "평범법.json").write_text(json.dumps(plain, ensure_ascii=False), encoding="utf-8")

    result = find_preprocess_targets(law_root, "law")
    assert result["scanned"] == 2
    assert result["file_only_appendix_docs"] == 1 and result["image_docs"] == 0
    assert len(result["paths"]) == 1 and "표본법 시행령.json" in result["paths"][0]

    adm_root = tmp_path / "ADMRUL"
    _write_admrul_doc(adm_root)                       # 픽스처(그림 마커 없음 가정)
    img_dir = adm_root / "행정규칙" / "그림규정"
    img_dir.mkdir(parents=True)
    img = {"law_id": "g1", "mst": "m", "law_name": "그림규정", "law_type": "고시",
           "doc_target": "admrul", "doc_kind": "행정규칙", "adm_uid": "admrul:g1",
           "enforcement_date": "20240101", "revision_date": "20240101",
           "body": {"format": "articles", "article_count": 1, "articles": [
               {"provision_id": "admrul:그림규정#JO0001", "article_no": "제1조", "article_title": "목적",
                "content": "제1조(목적) 조직도는 다음과 같다. [그림]", "relations": []}]},
           "addenda": [], "appendices": [], "attachments": []}
    (img_dir / "그림규정.json").write_text(json.dumps(img, ensure_ascii=False), encoding="utf-8")

    result = find_preprocess_targets(adm_root, "admrul")
    assert result["image_docs"] == 1
    assert any("그림규정.json" in p for p in result["paths"])


def test_vector_cache_reuses_matching_vectors(tmp_path):
    """벡터 캐시 — 같은 uuid + 같은 search_text 면 임베딩 호출 없이 재사용,
    search_text 가 다르거나(개정) 캐시에 없으면(신규) 임베딩한다. 속성은 항상 새로 쓴다."""
    import json as _json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from law_indexer.mapper import object_uuid
    from law_indexer.pipeline import VectorCache, _embed_and_store

    repo_root = tmp_path / "law_data"
    _write_law_doc(repo_root, with_file_only_appendix=False, create_local_file=False)
    settings = Settings.from_env()
    store = _FakeStore()

    class _CountingEmbedder(_FakeEmbedder):
        calls = 0

        def embed_documents(self, texts):
            _CountingEmbedder.calls += len(texts)
            return super().embed_documents(texts)

    embedder = _CountingEmbedder()
    # 1차: 캐시 없이 매핑·적재해 기대 객체를 얻는다
    from law_indexer.mapper import map_law_data
    raw = json.loads(next((repo_root).rglob("*.json")).read_text(encoding="utf-8"))
    objects = map_law_data(raw, next((repo_root).rglob("*.json")))
    assert objects

    # parquet 캐시 구성: 첫 객체는 동일 search_text(히트), 나머지는 다른 텍스트(미스)
    rows_uuid, rows_vec, rows_props = [], [], []
    for i, o in enumerate(objects):
        rows_uuid.append(object_uuid(o.chunk_id))
        rows_vec.append([9.0, 9.0, 9.0, 9.0])
        st = o.search_text if i == 0 else o.search_text + " (개정됨)"
        rows_props.append(_json.dumps({"search_text": st}, ensure_ascii=False))
    f = tmp_path / "old.parquet"
    pq.write_table(pa.table({"uuid": rows_uuid, "vector": rows_vec, "props": rows_props}), f)

    cache = VectorCache(f)
    _CountingEmbedder.calls = 0
    result = _embed_and_store(store, embedder, objects, settings.law_collection, cache=cache)
    assert result["vectors_reused"] == 1                     # 첫 객체만 재사용
    assert _CountingEmbedder.calls == len(objects) - 1       # 나머지만 임베딩
    assert objects[0].vector == [9.0, 9.0, 9.0, 9.0]         # 재사용 벡터 반영
    assert store.upserts and len(store.upserts[0]["objects"]) == len(objects)
