"""lawgo_raw(복사본) + inbox_convert 조립 == 수집기 temporal_law build_payload 출력.

수집기가 로컬에 있을 때만 돈다(운영·CI 에는 없다). 복사가 어긋났거나 조립부를 잘못 옮겼으면
여기서 잡힌다. 비교에서 빼는 것: 시각(fetched_at·synced_at), relation_mode(수집기는 환경변수),
그리고 수집기만 하는 네트워크 관계 추출 결과(두 fixture 는 with_relations=False 라 어차피 빈 값).
"""
import json
import sys
import zipfile
from pathlib import Path

import pytest

TL = Path("/Users/huiju.ko/workspace/law_ai/temporal_law")
FIX = Path(__file__).parent / "fixtures" / "inbox"
pytestmark = pytest.mark.skipif(not (TL / "collector").is_dir(), reason="수집기 temporal_law 가 로컬에 없음")


def _raw(name):
    with zipfile.ZipFile(FIX / f"{name}.zip") as z:
        jn = [n for n in z.namelist() if n.endswith(".json")][0]
        return json.loads(z.read(jn).decode("utf-8-sig")), Path(jn).stem
    

def _strip(p):
    p = json.loads(json.dumps(p, ensure_ascii=False))
    for k in ("source", "sync", "relation_mode"):
        p.pop(k, None)
    return p


@pytest.fixture(scope="module")
def collector(monkeypatch_module=None):
    sys.path.insert(0, str(TL))
    import importlib
    law = importlib.import_module("collector.law")
    adm = importlib.import_module("collector.admrul")
    return law, adm


def test_law_payload_equals_collector_output(collector, monkeypatch):
    from law_indexer.inbox_convert import assemble_law
    law, _ = collector
    body, name = _raw("batch-20261021-001")
    meta = {"ID": "001282", "MST": "290605", "_is_current": True}
    monkeypatch.setattr(law, "fetch_law_body", lambda m, n: body)
    monkeypatch.setattr(law, "fetch_delegated", lambda m, n: {})
    monkeypatch.setenv("LAW_NO_CRAWL", "1")
    theirs = _strip(law.build_payload(name, law_meta=meta, with_relations=False, resolve_current=False))
    ours = _strip(assemble_law(body, "001282", "290605", name))
    assert ours == theirs


def test_admrul_payload_equals_collector_output(collector, monkeypatch):
    from law_indexer.inbox_convert import assemble_admrul
    _, adm = collector
    body, name = _raw("batch-20261021-002")
    meta = {"행정규칙ID": "37584", "행정규칙일련번호": "2100000285778", "_is_current": True}
    monkeypatch.setattr(adm, "fetch_admrul_body", lambda m, n, target="admrul": body)
    monkeypatch.setenv("LAW_NO_CRAWL", "1")
    theirs = _strip(adm.build_admrul_payload(name, meta=meta, with_relations=False))
    ours = _strip(assemble_admrul(body, "37584", "2100000285778", name))
    assert ours == theirs
