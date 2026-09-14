import json
import urllib.error
from pathlib import Path

import pytest

from law_indexer.mapper import map_attachment_data
from law_indexer.pipeline import SKIP_JSON_NAMES, discover
from law_indexer.preprocess import (
    DocParserClient, DocParserError, appendix_local_path, attachment_local_path,
    body_has_meaningful_text, cleanup_staged_file, document_is_file_only, find_article_images,
    find_parent_law, find_pending_appendices, has_document_attachment,
    is_deleted_appendix_content, normalize_doc_parser_chunks, preprocess_file,
    resolve_pending_document_files, stage_for_doc_parser,
)


def _mirror(tmp_path: Path) -> Path:
    """data/law_data/{문서}/시행령/{문서.json + 서식/파일.hwp} 미러 구조를 만든다."""
    doc = tmp_path / "law_data" / "표본법" / "시행령"
    (doc / "서식").mkdir(parents=True)
    law = {
        "law_id": "l1", "version_uid": "v1", "law_name": "표본법 시행령", "law_type": "시행령",
        "body": {"articles": []},
        "appendices": [{"provision_id": "law:표본법시행령#FORM0001", "kind": "서식", "filename": "서식1.hwp",
                        "files": [{"name": "raw_원본.hwp"}]}],
    }
    (doc / "표본법 시행령.json").write_text(json.dumps(law, ensure_ascii=False), encoding="utf-8")
    (tmp_path / "law_data" / "_manifest.json").write_text("{}", encoding="utf-8")
    (doc / "서식" / "서식1.hwp").write_bytes(b"hwp")
    (doc / "서식" / "무관첨부.pdf").write_bytes(b"pdf")
    return tmp_path


def test_find_parent_law_climbs_to_document_json(tmp_path):
    root = _mirror(tmp_path)
    hwp = root / "law_data" / "표본법" / "시행령" / "서식" / "서식1.hwp"
    parent = find_parent_law(hwp)
    assert parent is not None and parent["version_uid"] == "v1"


def test_preprocess_matches_appendix_and_builds_chunks(tmp_path):
    root = _mirror(tmp_path)
    hwp = root / "law_data" / "표본법" / "시행령" / "서식" / "서식1.hwp"
    data = preprocess_file(hwp, find_parent_law(hwp))
    assert data["version_uid"] == "v1"
    assert data["provision_id"] == "law:표본법시행령#FORM0001"  # filename 매칭
    assert data["chunks"] and data["chunks"][0]["chunk_index"] == 0
    obj = map_attachment_data(data, hwp)[0]
    assert obj.unit_type == "APPENDIX" and obj.source_type == "FILE"


def test_preprocess_unmatched_file_is_general_attachment(tmp_path):
    root = _mirror(tmp_path)
    pdf = root / "law_data" / "표본법" / "시행령" / "서식" / "무관첨부.pdf"
    data = preprocess_file(pdf, find_parent_law(pdf))
    assert data["provision_id"] is None
    assert map_attachment_data(data, pdf)[0].unit_type == "FILE"


def test_discover_skips_manifest_and_filters_suffix(tmp_path):
    root = _mirror(tmp_path)
    jsons = discover(root, recursive=True, limit=None, suffixes={".json"}, skip_names=SKIP_JSON_NAMES)
    assert all(p.name != "_manifest.json" for p in jsons)
    assert any(p.name == "표본법 시행령.json" for p in jsons)
    hwps = discover(root, recursive=True, limit=None, suffixes={".hwp", ".pdf"})
    assert {p.name for p in hwps} == {"서식1.hwp", "무관첨부.pdf"}


# ── 로컬 첨부파일 경로 역산 (temporal_law/collector/mdexport.py 규칙 재구현) ──

def test_appendix_local_path_resolves_law_dir_convention(tmp_path):
    repo_root = tmp_path / "law_data"
    doc_dir = repo_root / "표본법" / "시행령"
    (doc_dir / "별표").mkdir(parents=True)
    target = doc_dir / "별표" / "별표1_기준표.hwp"
    target.write_bytes(b"hwp")
    data = {"law_name": "표본법 시행령"}
    appendix = {"no": "0001", "branch": "00", "kind": "별표", "title": "기준표",
               "files": [{"type": "hwp", "url": "https://x", "name": ""}]}
    assert appendix_local_path(repo_root, data, appendix) == target


def test_appendix_local_path_none_when_file_missing(tmp_path):
    repo_root = tmp_path / "law_data"
    data = {"law_name": "표본법 시행령"}
    appendix = {"no": "0001", "branch": "00", "kind": "별표", "title": "기준표",
               "files": [{"type": "hwp", "url": "https://x", "name": ""}]}
    assert appendix_local_path(repo_root, data, appendix) is None  # 배치를 막지 않고 None 반환


def test_appendix_local_path_none_when_no_files_listed(tmp_path):
    repo_root = tmp_path / "law_data"
    data = {"law_name": "표본법 시행령"}
    assert appendix_local_path(repo_root, data, {"no": "0001", "kind": "별표", "files": []}) is None


def test_find_pending_appendices_only_is_file_only_true():
    data = {"appendices": [{"is_file_only": False, "provision_id": "p1"},
                          {"is_file_only": True, "provision_id": "p2"}]}
    pending = find_pending_appendices(data)
    assert [p["provision_id"] for p in pending] == ["p2"]


def test_appendix_local_path_uses_filename_field_directly(tmp_path):
    """실제 law_data/admrul_data 전수 검증(2026-07-28) 결과 appendix.filename 이 100% 신뢰
    가능해 이제 이 필드를 최우선으로 쓴다(라벨 재구성은 폴백일 뿐)."""
    repo_root = tmp_path / "law_data"
    doc_dir = repo_root / "표본법" / "시행령"
    (doc_dir / "별표").mkdir(parents=True)
    target = doc_dir / "별표" / "실제파일명이라벨과다름.hwp"
    target.write_bytes(b"hwp")
    data = {"law_name": "표본법 시행령"}
    appendix = {"no": "0001", "kind": "별표", "title": "라벨은 다른데", "filename": "실제파일명이라벨과다름.hwp",
               "files": [{"type": "hwp", "name": ""}]}
    assert appendix_local_path(repo_root, data, appendix) == target


def test_body_has_meaningful_text_true_for_real_content():
    data = {"body": {"articles": [{"content": "제1조(목적) 이 규칙은 표본을 정한다."}]}}
    assert body_has_meaningful_text(data) is True


def test_body_has_meaningful_text_false_for_empty_articles():
    assert body_has_meaningful_text({"body": {"articles": []}}) is False


def test_body_has_meaningful_text_false_for_placeholder_only():
    data = {"body": {"articles": [{"content": "[첨부파일 참조]"}]}}
    assert body_has_meaningful_text(data) is False


def test_body_has_meaningful_text_false_for_whitespace_only():
    data = {"body": {"articles": [{"content": "   \n  "}]}}
    assert body_has_meaningful_text(data) is False


def test_has_document_attachment_checks_top_level_attachments():
    assert has_document_attachment({"attachments": [{"name": "a.pdf"}]}) is True
    assert has_document_attachment({"attachments": []}) is False
    assert has_document_attachment({}) is False


def test_document_is_file_only_true_only_when_no_text_and_has_attachment():
    no_text_with_att = {"body": {"articles": []}, "attachments": [{"name": "a.pdf"}]}
    has_text_with_att = {"body": {"articles": [{"content": "실제 조문 내용"}]}, "attachments": [{"name": "a.pdf"}]}
    no_text_no_att = {"body": {"articles": []}, "attachments": []}
    assert document_is_file_only(no_text_with_att) is True
    assert document_is_file_only(has_text_with_att) is False  # 본문 있으면 최상위 파일 자동처리 안 함
    assert document_is_file_only(no_text_no_att) is False


def test_attachment_local_path_uses_filename_field(tmp_path):
    repo_root = tmp_path / "admrul_data"
    doc_dir = repo_root / "행정규칙" / "표본 행정규칙"
    (doc_dir / "첨부파일").mkdir(parents=True)
    target = doc_dir / "첨부파일" / "표본 행정규칙 원문.pdf"
    target.write_bytes(b"pdf")
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙"}
    attachment = {"filename": "표본 행정규칙 원문.pdf", "url": "https://x"}
    assert attachment_local_path(repo_root, data, attachment) == target


# ── STEP3(§2 공통 규칙): 조문0 문서의 attachments[] 다중 파일 처리 ─────────

def _admrul_doc_only_base(repo_root: Path) -> Path:
    doc_dir = repo_root / "행정규칙" / "표본 행정규칙"
    (doc_dir / "첨부파일").mkdir(parents=True)
    return doc_dir


def test_resolve_pending_document_files_none_when_body_has_text(tmp_path):
    repo_root = tmp_path / "admrul_data"
    _admrul_doc_only_base(repo_root)
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙",
            "body": {"articles": [{"content": "실제 본문"}]},
            "attachments": [{"filename": "a.pdf"}]}
    assert resolve_pending_document_files(repo_root, data) == []


def test_resolve_pending_document_files_dedups_same_basename_by_priority(tmp_path):
    """실측(전원개발사업 실시계획 문서): 같은 문서가 hwp+pdf 로 중복 수출돼 있으면 hwpx>hwp>docx>pdf
    우선순위로 1개만 남긴다."""
    repo_root = tmp_path / "admrul_data"
    doc_dir = _admrul_doc_only_base(repo_root)
    (doc_dir / "첨부파일" / "고시.hwp").write_bytes(b"hwp")
    (doc_dir / "첨부파일" / "고시.pdf").write_bytes(b"pdf")
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙",
            "body": {"articles": []},
            "attachments": [{"filename": "고시.pdf"}, {"filename": "고시.hwp"}]}
    result = resolve_pending_document_files(repo_root, data)
    assert len(result) == 1 and result[0]["local_path"].name == "고시.hwp"


def test_resolve_pending_document_files_keeps_distinct_documents_separate(tmp_path):
    """실측: 원문+조문별제개정이유서+고시문처럼 basename 이 다른 서로 다른 실제 문서는 role 구분
    없이 전부 처리 대상이어야 한다(하나만 고르면 내용이 유실된다)."""
    repo_root = tmp_path / "admrul_data"
    doc_dir = _admrul_doc_only_base(repo_root)
    (doc_dir / "첨부파일" / "개정본문.pdf").write_bytes(b"pdf1")
    (doc_dir / "첨부파일" / "조문별제개정이유서.pdf").write_bytes(b"pdf2")
    (doc_dir / "첨부파일" / "고시문.pdf").write_bytes(b"pdf3")
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙",
            "body": {"articles": []},
            "attachments": [{"filename": "개정본문.pdf"}, {"filename": "조문별제개정이유서.pdf"},
                            {"filename": "고시문.pdf"}]}
    result = resolve_pending_document_files(repo_root, data)
    assert {r["local_path"].name for r in result} == {"개정본문.pdf", "조문별제개정이유서.pdf", "고시문.pdf"}


def test_resolve_pending_document_files_skips_unsupported_extension(tmp_path):
    repo_root = tmp_path / "admrul_data"
    doc_dir = _admrul_doc_only_base(repo_root)
    (doc_dir / "첨부파일" / "첨부.zip").write_bytes(b"zip")
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙",
            "body": {"articles": []}, "attachments": [{"filename": "첨부.zip"}]}
    assert resolve_pending_document_files(repo_root, data) == []


def test_resolve_pending_document_files_dedups_identical_duplicate_entries(tmp_path):
    """실측: 같은 파일명이 attachments[] 에 정확히 두 번 들어있는 경우(데이터 이슈)도 1개로 합쳐진다."""
    repo_root = tmp_path / "admrul_data"
    doc_dir = _admrul_doc_only_base(repo_root)
    (doc_dir / "첨부파일" / "고시.pdf").write_bytes(b"pdf")
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙",
            "body": {"articles": []},
            "attachments": [{"filename": "고시.pdf"}, {"filename": "고시.pdf"}]}
    assert len(resolve_pending_document_files(repo_root, data)) == 1


# ── STEP2: 삭제된 별표/별지/서식 스킵 ──────────────────────────────────────

def test_is_deleted_appendix_content_matches_real_patterns():
    assert is_deleted_appendix_content("[별지 제1호서식] <삭제>") is True
    assert is_deleted_appendix_content("삭제 <2026. 4. 15.>") is True
    assert is_deleted_appendix_content("[별표 2] 삭제") is True
    assert is_deleted_appendix_content("삭제") is True


def test_is_deleted_appendix_content_false_for_real_content():
    assert is_deleted_appendix_content("제1조(목적) 이 규칙은 삭제 관련 절차를 정한다.") is False
    assert is_deleted_appendix_content("") is False


def test_find_pending_appendices_ignores_top_level_attachments():
    """attachments[] 는 is_file_only 필드 자체가 없어 이 함수의 대상이 아니다(조건부 트리거 유지)."""
    data = {"appendices": [], "attachments": [{"name": "원문.pdf", "url": "https://x"}],
            "doc_target": "admrul"}
    assert find_pending_appendices(data) == []


# ── 조문 본문이미지 ([그림] 마커, STEP1) ──────────────────────────────────

def test_find_article_images_matches_article_no_prefix_in_seq_order(tmp_path):
    """실제 admrul_data(피난유도선 성능인증...) 확인 결과: article_no 필드 값 자체가 이미
    "제5조" 형태를 담고 있고, 파일명은 {article_no}_{순번}.gif 다(제·조를 따로 안 붙인다)."""
    repo_root = tmp_path / "admrul_data"
    doc_dir = repo_root / "행정규칙" / "표본 행정규칙"
    img_dir = doc_dir / "본문이미지"
    img_dir.mkdir(parents=True)
    (img_dir / "제5조_2.gif").write_bytes(b"gif2")
    (img_dir / "제5조_1.gif").write_bytes(b"gif1")
    (img_dir / "제10조의1_1.gif").write_bytes(b"other")  # 다른 조 — 매칭되면 안 됨
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙"}
    article = {"article_no": "제5조"}
    images = find_article_images(repo_root, data, article)
    assert [p.name for p in images] == ["제5조_1.gif", "제5조_2.gif"]  # 순번 순 정렬


def test_find_article_images_empty_when_no_image_dir(tmp_path):
    repo_root = tmp_path / "admrul_data"
    data = {"law_name": "표본 행정규칙", "doc_target": "admrul", "doc_kind": "행정규칙"}
    assert find_article_images(repo_root, data, {"article_no": "제1조"}) == []


def test_normalize_doc_parser_chunks_maps_fields():
    raw = [{"text": "본문", "i_page": 2, "e_page": 3, "i_chunk_on_doc": 5,
           "chunk_bboxes": "bbox", "media_files": None, "guardrail_categories": None,
           "n_char": 10, "n_word": 2, "n_line": 1, "reg_date": "2026-07-28T00:00:00Z"}]
    normalized = normalize_doc_parser_chunks(raw)
    assert normalized[0] == {
        "content": "본문", "chunk_index": 5, "page_no": 2, "start_page": 2, "end_page": 3,
        "chunk_bboxes": "bbox", "media_files": None, "guardrail_categories": None,
        "n_char": 10, "n_word": 2, "n_line": 1, "parser_reg_date": "2026-07-28T00:00:00Z",
    }


def test_stage_for_doc_parser_copies_to_shared_dir_and_cleanup(tmp_path):
    local = tmp_path / "원본.hwp"
    local.write_bytes(b"hwp")
    shared = tmp_path / "shared"
    staged_host, request_path, was_staged = stage_for_doc_parser(local, shared)
    assert was_staged is True and staged_host.exists() and staged_host != local
    assert request_path == str(staged_host)  # container_dir 없으면 호스트 경로 그대로
    cleanup_staged_file(staged_host, was_staged, keep=False)
    assert not staged_host.exists()


def test_stage_for_doc_parser_without_shared_dir_uses_original_path(tmp_path):
    local = tmp_path / "원본.hwp"
    local.write_bytes(b"hwp")
    staged_host, request_path, was_staged = stage_for_doc_parser(local, None)
    assert staged_host == local and request_path == str(local) and was_staged is False


def test_stage_for_doc_parser_translates_host_to_container_path(tmp_path):
    """실제 doc-parser-local 컨테이너처럼 호스트 마운트 경로와 컨테이너 내부 경로가 다른 경우."""
    local = tmp_path / "원본.hwp"
    local.write_bytes(b"hwp")
    shared_host = tmp_path / "shared"
    staged_host, request_path, was_staged = stage_for_doc_parser(local, shared_host, "/data")
    assert was_staged is True and staged_host.exists()
    assert staged_host.parent == shared_host
    assert request_path == f"/data/{staged_host.name}"  # 컨테이너 관점 경로로 변환됨


# ── Doc Parser HTTP 클라이언트 (urllib.request.urlopen 모킹) ──────────────

class _FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _patched_client(monkeypatch, responder, max_retries=0):
    monkeypatch.setattr("law_indexer.preprocess.urllib.request.urlopen", responder)
    return DocParserClient("http://localhost:8080", timeout=1, max_retries=max_retries)


def test_health_check_true_on_2xx(monkeypatch):
    client = _patched_client(monkeypatch, lambda req, timeout=None: _FakeResponse(200, b"ok"))
    assert client.health_check() is True


def test_health_check_false_on_exception(monkeypatch):
    def _raise(req, timeout=None):
        raise OSError("boom")
    client = _patched_client(monkeypatch, _raise)
    assert client.health_check() is False


def test_run_success_filters_empty_text_chunks(monkeypatch):
    body = json.dumps({"code": 0, "errMsg": "success", "data": [
        {"text": "본문1", "i_page": 1, "e_page": 1, "i_chunk_on_doc": 0},
        {"text": "  ", "i_page": 1, "e_page": 1, "i_chunk_on_doc": 1},
    ]}).encode("utf-8")
    client = _patched_client(monkeypatch, lambda req, timeout=None: _FakeResponse(200, body))
    chunks = client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert len(chunks) == 1 and chunks[0]["text"] == "본문1"


def test_run_application_error_on_nonzero_code(monkeypatch):
    body = json.dumps({"code": 1, "errMsg": "실패"}).encode("utf-8")
    client = _patched_client(monkeypatch, lambda req, timeout=None: _FakeResponse(200, body))
    with pytest.raises(DocParserError) as exc:
        client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert exc.value.code == "APPLICATION_ERROR"


def test_run_empty_result_when_all_chunks_blank(monkeypatch):
    body = json.dumps({"code": 0, "errMsg": "success", "data": [{"text": ""}]}).encode("utf-8")
    client = _patched_client(monkeypatch, lambda req, timeout=None: _FakeResponse(200, body))
    with pytest.raises(DocParserError) as exc:
        client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert exc.value.code == "EMPTY_RESULT"


def test_run_timeout_is_distinguished(monkeypatch):
    def _raise(req, timeout=None):
        raise TimeoutError("timed out")
    client = _patched_client(monkeypatch, _raise)
    with pytest.raises(DocParserError) as exc:
        client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert exc.value.code == "TIMEOUT"


def test_run_connection_error_is_distinguished(monkeypatch):
    def _raise(req, timeout=None):
        raise urllib.error.URLError("Connection refused")
    client = _patched_client(monkeypatch, _raise)
    with pytest.raises(DocParserError) as exc:
        client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert exc.value.code == "CONNECTION_ERROR"


def test_run_http_error_is_distinguished(monkeypatch):
    def _raise(req, timeout=None):
        raise urllib.error.HTTPError("http://x/run", 500, "Server Error", hdrs=None, fp=None)
    client = _patched_client(monkeypatch, _raise)
    with pytest.raises(DocParserError) as exc:
        client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert exc.value.code == "HTTP_ERROR"


def test_run_retries_configured_times_then_stops(monkeypatch):
    calls = {"n": 0}

    def _raise(req, timeout=None):
        calls["n"] += 1
        raise TimeoutError("timed out")

    client = _patched_client(monkeypatch, _raise, max_retries=2)
    with pytest.raises(DocParserError):
        client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert calls["n"] == 3  # 최초 1회 + 재시도 2회 — 무한 반복하지 않는다


def test_run_does_not_retry_deterministic_errors(monkeypatch):
    """결정적 오류(code!=0)는 재시도해도 결과가 같으므로 max_retries 와 무관하게 1번만 호출한다
    — 전처리기가 못 읽는 파일(초소형 수식 글리프 등)에서 3번씩 재시도하던 낭비 제거."""
    calls = {"n": 0}

    def _resp(req, timeout=None):
        calls["n"] += 1
        return _FakeResponse(200, json.dumps({"code": 1, "errMsg": "실패"}).encode("utf-8"))

    client = _patched_client(monkeypatch, _resp, max_retries=2)
    with pytest.raises(DocParserError) as exc:
        client.run(Path("/tmp/sample.hwpx"), 2000, 200)
    assert exc.value.code == "APPLICATION_ERROR"
    assert calls["n"] == 1


def test_docparser_multipart_body(tmp_path):
    """업로드 모드 multipart 조립 — file(확장자 보존)·params 필드·boundary 포함."""
    from law_indexer.preprocess import DocParserClient
    f = tmp_path / "별표1_한글이름.hwp"
    f.write_bytes(b"HWPBYTES")
    body, ctype = DocParserClient._multipart_body(f, '{"a": 1}')
    assert ctype.startswith("multipart/form-data; boundary=")
    boundary = ctype.split("boundary=")[1]
    assert boundary.encode() in body
    assert b'name="file"; filename="attachment.hwp"' in body   # 확장자 보존, ASCII 이름
    assert b"HWPBYTES" in body                                  # 파일 바이트
    assert b'name="params"' in body and b'{"a": 1}' in body


# ── 레포 3분할 + 문서종 래퍼 제거 레이아웃 (2026-08) ─────────────────────────

def _write_admrul_doc(root, name, *, sub=None, law_id="1234", doc_target="admrul"):
    """새 저장 레이아웃(`{repo}/{문서명}[/{suffix}]/`)으로 문서 하나를 만든다."""
    base = root / name if sub is None else root / name / sub
    (base / "별표").mkdir(parents=True, exist_ok=True)
    data = {"law_id": law_id, "law_name": name, "doc_target": doc_target, "doc_kind": "행정규칙"}
    (base / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    (base / "별표" / "별표1_서식.hwp").write_bytes(b"x")
    return base, data


def test_appendix_local_path_uses_doc_dir_when_given(tmp_path):
    """디스크 순회 색인은 JSON 의 부모 폴더가 곧 문서 폴더 — 역산하지 않는다."""
    base, data = _write_admrul_doc(tmp_path, "어떤규정")
    ap = {"kind": "별표", "no": "1", "filename": "별표1_서식.hwp",
          "local_path": "별표/별표1_서식.hwp"}
    got = appendix_local_path(tmp_path, data, ap, doc_dir=base)
    assert got == base / "별표" / "별표1_서식.hwp"


def test_appendix_local_path_reverses_new_flat_layout(tmp_path):
    """역산 폴백도 문서종 래퍼 없는 현재 레이아웃(`{repo}/{문서명}`)을 찾아야 한다."""
    base, data = _write_admrul_doc(tmp_path, "어떤규정")
    ap = {"kind": "별표", "no": "1", "local_path": "별표/별표1_서식.hwp"}
    assert appendix_local_path(tmp_path, data, ap) == base / "별표" / "별표1_서식.hwp"


def test_appendix_local_path_split_dir_matches_by_law_id(tmp_path):
    """동명 분할(`{문서명}/{suffix}`)은 하위 JSON 의 law_id 로 **확인**하고 고른다(추측 금지)."""
    root = tmp_path
    base_a, data_a = _write_admrul_doc(root, "같은이름규정", sub="admrul_111", law_id="111")
    base_b, data_b = _write_admrul_doc(root, "같은이름규정", sub="admrul_222", law_id="222")
    ap = {"kind": "별표", "no": "1", "local_path": "별표/별표1_서식.hwp"}
    assert appendix_local_path(root, data_a, ap) == base_a / "별표" / "별표1_서식.hwp"
    assert appendix_local_path(root, data_b, ap) == base_b / "별표" / "별표1_서식.hwp"


def test_appendix_local_path_split_dir_ignores_other_document(tmp_path):
    """law_id 가 안 맞으면 같은 이름이어도 그 폴더의 파일을 물지 않는다."""
    root = tmp_path
    _write_admrul_doc(root, "같은이름규정", sub="admrul_111", law_id="111")
    other = {"law_id": "999", "law_name": "같은이름규정", "doc_target": "admrul", "doc_kind": "행정규칙"}
    ap = {"kind": "별표", "no": "1", "local_path": "별표/별표1_서식.hwp"}
    assert appendix_local_path(root, other, ap) is None
