"""law_id 삭제 경로의 도메인 가드 검증 — 실제 연결 없이 필터 조립만 확인.

**배경.** law_id 는 생산자의 doc_id 를 그대로 쓰는데 법령과 행정규칙이 서로 다른 ID 공간이면서
네임스페이스가 없다. 지금 충돌이 0건인 건 법제처가 법령ID 를 6자리 zero-padding 으로 주기
때문이지(`010704` vs `10704`) 코드가 보장하는 게 아니다 — 앞 0 을 무시하면 실측 5건이 겹친다.
컬렉션이 분리돼 있을 때는 경계가 막아 주지만 한 컬렉션으로 합치면 그 방어막이 사라진다.

그래서 law_id 로 좁히는 연산에 `domain` 조건을 AND 로 덧댄다. 이 테스트는 그 조건이 실제로
필터에 들어가는지, 그리고 source 를 모르는 기존 경로의 동작이 바뀌지 않는지를 고정한다.
"""
import weaviate
from weaviate.classes.query import Filter

from law_indexer.config import Settings
from law_indexer.weaviate_store import WeaviateStore


def _store(monkeypatch, source):
    """연결 없이 WeaviateStore 를 만든다 — 보는 건 필터 조립이지 네트워크가 아니다."""
    monkeypatch.setattr("law_indexer.config.load_dotenv", lambda *a, **k: False)
    monkeypatch.setattr(weaviate, "connect_to_custom", lambda *a, **k: object())
    return WeaviateStore(Settings.from_env(), source)


def _leaves(flt) -> dict:
    """조립된 필터를 {프로퍼티: 비교값} 으로 편다(중첩 AND 포함).

    weaviate 4.x 는 단일 조건을 `_FilterValue(target=, value=)`, 결합을 `_FilterAnd(filters=[...])`
    로 표현한다. 같은 프로퍼티가 여러 번 나오면(version_uid not_equal 체인) 마지막 값이 남지만
    이 테스트가 보는 건 '조건이 들어갔는가'라 무해하다.
    """
    out = {}
    stack = [flt]
    while stack:
        node = stack.pop()
        children = getattr(node, "filters", None)
        if isinstance(children, (list, tuple)):
            stack.extend(children)
            continue
        target = getattr(node, "target", None)
        if isinstance(target, str):
            out[target] = getattr(node, "value", None)
    return out


def test_scoped_adds_domain_when_source_known(monkeypatch):
    """source 를 알고 연 연결이면 domain 조건이 덧붙는다."""
    store = _store(monkeypatch, "admrul")
    leaves = _leaves(store._scoped(Filter.by_property("law_id").equal("10704")))
    assert leaves.get("law_id") == "10704"
    assert leaves.get("domain") == "admrul", "law_id 단독으로 남으면 합친 컬렉션에서 교차 삭제가 난다"


def test_scoped_is_noop_without_source(monkeypatch):
    """source 를 모르면 덧대지 않는다 — 틀린 도메인으로 좁히면 지워야 할 청크를 못 지운다.

    기존 동작(법령 키로 열던 경로)을 그대로 보존하기 위한 의도된 통과다.
    """
    store = _store(monkeypatch, None)
    base = Filter.by_property("law_id").equal("010704")
    assert store._scoped(base) is base


def test_scoped_keeps_existing_conditions(monkeypatch):
    """원래 조건을 지우지 않고 AND 로만 덧댄다."""
    store = _store(monkeypatch, "law")
    scoped = store._scoped(
        Filter.by_property("law_id").equal("010704")
        & Filter.by_property("version_uid").equal("010704:222:20240101")
        & Filter.by_property("source_type").equal("JSON")
    )
    leaves = _leaves(scoped)
    for expected in ("law_id", "version_uid", "source_type", "domain"):
        assert expected in leaves, f"{expected} 조건이 사라졌다"
    assert leaves["domain"] == "law"


def test_domain_value_follows_store_source(monkeypatch):
    """덧대는 값은 그 연결의 source 다 — law 연결이 admrul 을 지우면 안 된다."""
    for source in ("law", "admrul"):
        store = _store(monkeypatch, source)
        leaves = _leaves(store._scoped(Filter.by_property("law_id").equal("10704")))
        assert leaves.get("domain") == source, f"{source} 연결인데 필터 도메인이 다르다"
