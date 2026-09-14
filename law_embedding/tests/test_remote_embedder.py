"""RemoteEmbedder(OpenAI 호환 /v1/embeddings) + make_embedder 팩토리 검증 — 실제 호출 없이 mock.

genos 모델 서빙으로 임베딩을 통일할 때 쓰는 원격 임베더. 요청 형식·index 정렬·인증 헤더·차원
확정을 확인한다."""
import dataclasses
import json
import urllib.request

import pytest

from law_indexer.config import Settings
from law_indexer.embedder import RemoteEmbedder, make_embedder


class _FakeResp:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_urlopen(capture):
    def fake(request, timeout=None):
        capture["url"] = request.full_url
        capture["headers"] = dict(request.header_items())
        body = json.loads(request.data.decode("utf-8"))
        n = len(body["input"])
        # 응답은 일부러 index 역순으로 → 소비자가 index 로 정렬해 대응 보장하는지 검증
        data = [{"index": i, "embedding": [float(i)] * 4} for i in range(n)]
        return _FakeResp(json.dumps({"data": list(reversed(data)), "model": body["model"]}).encode("utf-8"))
    return fake


def test_documents_dimension_and_order(monkeypatch):
    cap = {}
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen(cap))
    e = RemoteEmbedder("http://gw/rep/serving/10/v1/embeddings", "m", api_key=None, batch_size=2)
    vecs = e.embed_documents(["a", "b", "c"])
    assert len(vecs) == 3 and all(len(v) == 4 for v in vecs)
    assert vecs[1] == [1.0, 1.0, 1.0, 1.0]        # index 정렬로 "b"↔index1 대응
    assert e.dimension == 4
    assert cap["url"].endswith("/v1/embeddings")
    assert "Authorization" not in cap["headers"]   # 무인증(in-mesh)


def test_query_and_auth_header(monkeypatch):
    cap = {}
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen(cap))
    e = RemoteEmbedder("http://gw/v1/embeddings", "m", api_key="tok")
    v = e.embed_query("hi")
    assert len(v) == 4
    assert cap["headers"].get("Authorization") == "Bearer tok"


def test_make_embedder_remote_requires_url():
    s = dataclasses.replace(Settings.from_env(), embedding_backend="remote", embedding_api_url=None)
    with pytest.raises(ValueError):
        make_embedder(s)


def test_make_embedder_builds_remote():
    s = dataclasses.replace(Settings.from_env(), embedding_backend="remote",
                            embedding_api_url="http://gw/v1/embeddings", embedding_api_key="k")
    e = make_embedder(s)
    assert isinstance(e, RemoteEmbedder) and e.api_key == "k"
