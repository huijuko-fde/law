"""Weaviate API-키 인증 배선 검증 — 실제 연결 없이 connect_to_custom 호출 인자만 확인.

genos 등 인증 Weaviate: WEAVIATE_API_KEY 있으면 auth 로 붙고, 없으면 익명(로컬 그대로)."""
import dataclasses

import weaviate

from law_indexer.config import Settings
from law_indexer.weaviate_store import WeaviateStore


def _no_dotenv(monkeypatch):
    """.env 파일 로딩을 끈다 — 이 테스트가 보는 건 '환경변수 → Settings' 매핑이지 파일 내용이
    아니다. 끄지 않으면 .env 가 있는 환경(컨테이너·운영)에서 delenv 한 값이 파일에서 되살아난다."""
    monkeypatch.setattr("law_indexer.config.load_dotenv", lambda *a, **k: False)


def test_settings_reads_api_key_and_secure(monkeypatch):
    _no_dotenv(monkeypatch)
    monkeypatch.delenv("WEAVIATE_API_KEY", raising=False)
    monkeypatch.delenv("WEAVIATE_SECURE", raising=False)
    s = Settings.from_env()
    assert s.weaviate_api_key is None and s.weaviate_secure is False

    monkeypatch.setenv("WEAVIATE_API_KEY", "sk-genos")
    monkeypatch.setenv("WEAVIATE_SECURE", "true")
    s = Settings.from_env()
    assert s.weaviate_api_key == "sk-genos" and s.weaviate_secure is True


def test_admrul_key_falls_back_to_law_key(monkeypatch):
    """ADMRUL_WEAVIATE_API_KEY 를 안 주면 법령 키를 쓴다(같은 VDB 를 쓰는 로컬 등)."""
    _no_dotenv(monkeypatch)
    monkeypatch.delenv("ADMRUL_WEAVIATE_API_KEY", raising=False)
    monkeypatch.setenv("WEAVIATE_API_KEY", "sk-law")
    s = Settings.from_env()
    assert s.api_key_for("law") == "sk-law"
    assert s.api_key_for("admrul") == "sk-law"
    assert s.api_key_for(None) == "sk-law"


def test_store_uses_per_source_key(monkeypatch):
    """genos 는 컬렉션마다 키가 달라(RBAC) source 에 맞는 키로 붙어야 한다."""
    _no_dotenv(monkeypatch)
    monkeypatch.setenv("WEAVIATE_API_KEY", "sk-law")
    monkeypatch.setenv("ADMRUL_WEAVIATE_API_KEY", "sk-admrul")
    settings = Settings.from_env()
    assert settings.api_key_for("admrul") == "sk-admrul"
    assert settings.collection_for("admrul") == settings.admrul_collection
    assert settings.collection_for("law") == settings.law_collection

    from weaviate.classes.init import Auth
    for source, expected in (("law", "sk-law"), ("admrul", "sk-admrul")):
        captured = _capture_connect(monkeypatch)
        WeaviateStore(settings, source)
        assert captured["auth_credentials"] == Auth.api_key(expected), source


def _capture_connect(monkeypatch):
    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)

        class _Client:
            def close(self):
                pass

        return _Client()

    monkeypatch.setattr(weaviate, "connect_to_custom", fake_connect)
    return captured


def test_store_uses_api_key_when_present(monkeypatch):
    captured = _capture_connect(monkeypatch)
    settings = dataclasses.replace(Settings.from_env(), weaviate_api_key="sk-genos", weaviate_secure=True)
    WeaviateStore(settings)
    assert captured["auth_credentials"] is not None          # 인증 붙음
    assert captured["http_secure"] is True and captured["grpc_secure"] is True


def test_store_anonymous_when_no_key(monkeypatch):
    captured = _capture_connect(monkeypatch)
    settings = dataclasses.replace(Settings.from_env(), weaviate_api_key=None, weaviate_secure=False)
    WeaviateStore(settings)
    assert captured["auth_credentials"] is None              # 익명(로컬 그대로)
    assert captured["http_secure"] is False


def test_collections_merge_by_default(monkeypatch):
    """기본은 단일 컬렉션 — 세 source 가 같은 컬렉션을 가리킨다.

    스키마 35속성이 세 레포에서 동일하고 코퍼스 구분은 `domain` 필터가 하므로 나눌 이유가 없다.
    나눠 두면 법령↔행정규칙 상호참조를 한 질의로 못 풀고 키도 따로 필요하다(RBAC).
    """
    _no_dotenv(monkeypatch)
    for key in ("LAW_COLLECTION", "WEAVIATE_COLLECTION", "ADMRUL_COLLECTION", "SCHLPUB_COLLECTION"):
        monkeypatch.delenv(key, raising=False)
    s = Settings.from_env()
    assert s.collection_for("law") == "LegalProvisionIndex"
    assert s.collection_for("admrul") == "LegalProvisionIndex"
    assert s.collection_for("schlpub") == "LegalProvisionIndex"


def test_law_collection_override_carries_to_others(monkeypatch):
    """LAW_COLLECTION 만 바꿔도 나머지가 따라온다 — 합친 구성을 한 곳에서 돌린다."""
    _no_dotenv(monkeypatch)
    for key in ("WEAVIATE_COLLECTION", "ADMRUL_COLLECTION", "SCHLPUB_COLLECTION"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("LAW_COLLECTION", "MergedIndex")
    s = Settings.from_env()
    assert s.collection_for("admrul") == "MergedIndex"
    assert s.collection_for("schlpub") == "MergedIndex"


def test_explicit_admrul_collection_still_splits(monkeypatch):
    """명시하면 분리 구성도 그대로 된다 — 되돌릴 길을 막지 않는다."""
    _no_dotenv(monkeypatch)
    monkeypatch.delenv("WEAVIATE_COLLECTION", raising=False)
    monkeypatch.delenv("SCHLPUB_COLLECTION", raising=False)
    monkeypatch.setenv("LAW_COLLECTION", "LegalProvisionIndex")
    monkeypatch.setenv("ADMRUL_COLLECTION", "AdmrulProvisionIndex")
    s = Settings.from_env()
    assert s.collection_for("law") == "LegalProvisionIndex"
    assert s.collection_for("admrul") == "AdmrulProvisionIndex"
