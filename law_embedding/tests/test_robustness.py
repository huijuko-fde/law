"""임베더·벡터검증·DocParser 견고화 검증(HIGH 수정) — 실제 서버 없이 monkeypatch 로.

  · RemoteEmbedder : 5xx/429/타임아웃 재시도, 4xx 즉시중단, 응답 개수 불일치 시 중단.
  · WeaviateStore._validate : NaN/Inf/전부-0 벡터 거부(길이만이 아니라 값도).
  · DocParserClient.run : 5xx/429 재시도, 4xx 즉시중단.
"""
import json
import urllib.error
from types import SimpleNamespace

import pytest

from law_indexer import embedder as emb_mod
from law_indexer import preprocess as pp
from law_indexer.embedder import RemoteEmbedder
from law_indexer.preprocess import DocParserClient, DocParserError
from law_indexer.weaviate_store import WeaviateStore


class _Resp:
    """urlopen context manager 흉내 — read() 가 JSON 바이트를 돌려준다."""

    def __init__(self, body):
        self._body = body

    def read(self):
        return json.dumps(self._body).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _http_error(code):
    return urllib.error.HTTPError("http://x", code, f"status {code}", None, None)


# ── RemoteEmbedder ───────────────────────────────────────────────────────

def _emb_body(n, dim=3):
    return {"data": [{"index": i, "embedding": [0.1] * dim} for i in range(n)]}


def test_remote_embedder_retries_5xx_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _http_error(503)
        return _Resp(_emb_body(2))
    monkeypatch.setattr(emb_mod.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(emb_mod.time, "sleep", lambda *_: None)
    vecs = RemoteEmbedder("http://x/v1/embeddings", "m", max_retries=3).embed_documents(["a", "b"])
    assert len(vecs) == 2 and calls["n"] == 2       # 1회 재시도 후 성공


def test_remote_embedder_4xx_no_retry(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise _http_error(400)
    monkeypatch.setattr(emb_mod.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(emb_mod.time, "sleep", lambda *_: None)
    with pytest.raises(RuntimeError, match="HTTP 400"):
        RemoteEmbedder("http://x/v1/embeddings", "m", max_retries=3).embed_documents(["a"])
    assert calls["n"] == 1                          # 4xx 는 재시도 안 함


def test_remote_embedder_count_mismatch_raises(monkeypatch):
    monkeypatch.setattr(emb_mod.urllib.request, "urlopen", lambda req, timeout=None: _Resp(_emb_body(1)))
    with pytest.raises(RuntimeError, match="개수 불일치"):
        RemoteEmbedder("http://x/v1/embeddings", "m").embed_documents(["a", "b"])   # 입력2, 응답1


# ── WeaviateStore._validate (self 미사용 → None 으로 직접 호출) ─────────────

def _obj(vec):
    return SimpleNamespace(chunk_id="c", embedding_model="m", embedding_dimension=3, vector=vec)


def test_validate_rejects_nan():
    with pytest.raises(ValueError, match="비정상 벡터"):
        WeaviateStore._validate(None, [_obj([0.1, float("nan"), 0.2])], 3, "m")


def test_validate_rejects_inf():
    with pytest.raises(ValueError, match="비정상 벡터"):
        WeaviateStore._validate(None, [_obj([0.1, float("inf"), 0.2])], 3, "m")


def test_validate_rejects_all_zero():
    with pytest.raises(ValueError, match="비정상 벡터"):
        WeaviateStore._validate(None, [_obj([0.0, 0.0, 0.0])], 3, "m")


def test_validate_accepts_normal():
    WeaviateStore._validate(None, [_obj([0.1, 0.2, 0.3])], 3, "m")   # 예외 없어야


# ── DocParserClient.run ──────────────────────────────────────────────────

def test_docparser_retries_5xx_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _http_error(503)
        return _Resp({"code": 0, "data": [{"text": "hello"}]})
    monkeypatch.setattr(pp.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(pp.time, "sleep", lambda *_: None)
    chunks = DocParserClient("http://x", 5, 3, upload=False).run("f.pdf", 100, 20)
    assert chunks == [{"text": "hello"}] and calls["n"] == 2


def test_docparser_4xx_no_retry(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise _http_error(400)
    monkeypatch.setattr(pp.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(pp.time, "sleep", lambda *_: None)
    with pytest.raises(DocParserError):
        DocParserClient("http://x", 5, 3, upload=False).run("f.pdf", 100, 20)
    assert calls["n"] == 1                          # 4xx 는 재시도 안 함


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
