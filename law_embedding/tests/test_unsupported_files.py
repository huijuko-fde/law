"""원천 파일 한계(결정오류) 분리 검증 — DRM/손상/빈 결과는 재시도 큐·package 오류를 오염시키지
않고 '보류(원천 한계)'로만 남는다. 일시 오류(timeout·5xx)는 종전대로 재시도 대상."""
import base64

from law_indexer.config import Settings
from law_indexer.preprocess import (
    APPLICATION_ERROR, CONNECTION_ERROR, EMPTY_RESULT, HTTP_ERROR, TIMEOUT,
    DocParserError, is_permanent_parser_error,
)


def test_is_permanent_parser_error_classification():
    assert is_permanent_parser_error(DocParserError(APPLICATION_ERROR, "code=1 errMsg=DRM 문서"))
    assert is_permanent_parser_error(DocParserError(EMPTY_RESULT, "응답 data 배열이 비어 있습니다"))
    assert is_permanent_parser_error(DocParserError(HTTP_ERROR, "HTTP 404: Not Found"))
    # 일시 오류 — 재시도 가치가 있다
    assert not is_permanent_parser_error(DocParserError(TIMEOUT, "timeout(60s)"))
    assert not is_permanent_parser_error(DocParserError(CONNECTION_ERROR, "연결 실패"))
    assert not is_permanent_parser_error(DocParserError(HTTP_ERROR, "HTTP 503: Service Unavailable"))
    assert not is_permanent_parser_error(DocParserError(HTTP_ERROR, "HTTP 429: Too Many Requests"))


def _ctx(settings):
    from law_indexer.package import _Context
    from law_indexer.mapper import map_admrul_data
    return _Context(
        source="admrul", collection=settings.collection_for("admrul"),
        mapper_fn=map_admrul_data, source_repository=None, preprocess_files=True,
        inbox_dir=None, doc_parser=object(), settings=settings, original=None)


def _file_record():
    return {"record_type": "file", "law_id": "A1", "file_id": "f1",
            "file_name": "attachment.hwp",
            "content_b64": base64.b64encode(b"dummy").decode()}


def test_package_permanent_parser_error_is_pending_not_error(monkeypatch):
    """DRM 류 결정오류 → package 오류 없음(재시도·격리 안 탐) + 보류 사유 기록."""
    from law_indexer import package as pkg
    settings = Settings.from_env()

    def boom(*a, **k):
        raise DocParserError(APPLICATION_ERROR, "code=1 errMsg=암호화/배포용(DRM) HWP 문서")
    monkeypatch.setattr(pkg, "run_doc_parser", boom)

    totals = pkg._new_totals()
    out = pkg._preprocess_file_record(_ctx(settings), "A1", _file_record(), totals, None)
    assert out is not None and "_pending" in out
    assert out["_pending"]["reason"] == "unsupported_source_file"
    assert totals["errors"] == []                       # package 는 성공 처리된다
    assert totals["files_unsupported"] == 1 and totals["files_pending"] == 1


def test_package_transient_parser_error_stays_retryable(monkeypatch):
    """timeout 류 일시 오류 → 종전대로 package 오류(재시도 대상)."""
    from law_indexer import package as pkg
    settings = Settings.from_env()

    def boom(*a, **k):
        raise DocParserError(TIMEOUT, "timeout(60s)")
    monkeypatch.setattr(pkg, "run_doc_parser", boom)

    totals = pkg._new_totals()
    out = pkg._preprocess_file_record(_ctx(settings), "A1", _file_record(), totals, None)
    assert out is not None and out["_pending"]["reason"] == "preprocess_failed"
    assert len(totals["errors"]) == 1                   # 재시도/격리 판정으로 이어진다
