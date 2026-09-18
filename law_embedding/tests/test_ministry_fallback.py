"""결함 D 회귀: 대법원·헌법재판소 계열 ADMRUL 문서의 `ministry` 필터가 비지 않는다.

배경 — 최종 검증에서 ADMRUL 55문서(청크 1,071개)의 `ministry` 가 비어 부처 필터에서
통째로 사라졌다. payload 를 실제로 열어 보면 원인이 분명하다: 이 문서들은
`basic_info.소관부처명` 이 **빈 문자열**로 오고 부처명은 `상위부처명` 에만 있다.

    "소관부처명": "",  "소관부처코드": "9740000",  "상위부처명": "대법원"

실측(데이터 레포 ADMRUL 22,649문서 전수):
  · 소관부처명·상위부처명 둘 다 있음 22,413건 — 이때는 소관부처명(더 좁은 기관명)이 정답이다
    (예: 소관부처명=농림축산검역본부 / 상위부처명=농림축산식품부).
  · 상위부처명만 있고 소관부처명이 빈 문서 정확히 55건 — 대법원 51, 헌법재판소 4.
그래서 폴백은 **맨 마지막**이어야 한다(docs/schema.md §2 의 "부처명만 뽑은 값" 규약 유지).
"""
import json
from pathlib import Path

import pytest

from law_indexer.mapper import map_admrul_data, map_law_data

ADMRUL_FIXTURE = Path(__file__).parent / "fixtures" / "admrul.json"
LAW_FIXTURE = Path(__file__).parent / "fixtures" / "law.json"
REPO_ROOT = Path("/Users/huiju.ko/workspace/law_ai/data/ADMRUL")
# 실제로 ministry 가 비었던 문서들(대법원 51 · 헌법재판소 4 중 표본)
REPO_CASES = [
    ("개인회생사건 처리지침(재민 2004-4)", "대법원"),
    ("관공서의 촉탁등기에 관한 예규", "대법원"),
    ("헌법재판소 사건의 배당에 관한 내규", "헌법재판소"),
    ("국선대리인 선정 및 보수 지급에 관한 내규", "헌법재판소"),
]


def _admrul_with_basic(basic: dict):
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data["basic_info"] = basic
    return map_admrul_data(data, Path("t.json"))


def test_supreme_court_documents_get_ministry_from_parent_ministry_key():
    """소관부처명이 빈 문자열이면 상위부처명으로 채운다 — 이게 없으면 55문서가 필터에서 사라진다."""
    objects = _admrul_with_basic({
        "행정규칙명": "개인회생사건 처리지침(재민 2004-4)",
        "소관부처명": "", "소관부처코드": "9740000", "상위부처명": "대법원",
        "담당부서기관명": "대법원(대법원)",
    })
    assert objects
    assert {o.ministry for o in objects} == {"대법원"}


def test_constitutional_court_documents_too():
    objects = _admrul_with_basic({
        "소관부처명": "", "상위부처명": "헌법재판소",
        "담당부서기관명": "헌법재판소(심판지원총괄과)",
    })
    assert {o.ministry for o in objects} == {"헌법재판소"}


def test_parent_ministry_is_only_a_fallback_never_an_override():
    """소관부처명이 있으면 그걸 쓴다 — 상위부처명은 더 넓은 상위기관이라 덮어쓰면 안 된다.

    실측 5,254건이 둘 다 있으면서 서로 다르다(농림축산검역본부 vs 농림축산식품부 등)."""
    objects = _admrul_with_basic({"소관부처명": "농림축산검역본부", "상위부처명": "농림축산식품부"})
    assert {o.ministry for o in objects} == {"농림축산검역본부"}


def test_nested_law_style_key_still_wins():
    """법령 API 의 중첩객체 `소관부처`({content, 소관부처코드}) 규약은 그대로다."""
    objects = _admrul_with_basic({
        "소관부처": {"content": "고용노동부", "소관부처코드": "1492000"},
        "소관부처명": "지방고용노동청", "상위부처명": "고용노동부",
    })
    assert {o.ministry for o in objects} == {"고용노동부"}


def test_whitespace_only_values_are_treated_as_empty():
    objects = _admrul_with_basic({"소관부처": "   ", "소관부처명": "\n", "상위부처명": "대법원"})
    assert {o.ministry for o in objects} == {"대법원"}


def test_ministry_stays_none_when_no_key_carries_a_value():
    """부처 정보가 정말 없으면 None 이다(빈 문자열로 채워 필터를 오염시키지 않는다).

    대한민국헌법이 실제로 이 경우다 — basic_info 에 부처 키 자체가 없다."""
    objects = _admrul_with_basic({"행정규칙명": "표본", "소관부처명": "", "상위부처명": ""})
    assert {o.ministry for o in objects} == {None}


def test_law_fixture_ministry_is_unchanged():
    """법령 경로(중첩객체)의 기존 동작이 그대로인지 확인한다."""
    data = json.loads(LAW_FIXTURE.read_text(encoding="utf-8"))
    data["basic_info"] = {"소관부처": {"content": "법무부", "소관부처코드": "1270000"}}
    assert {o.ministry for o in map_law_data(data, Path("t.json"))} == {"법무부"}


@pytest.mark.parametrize("doc_name,expected", REPO_CASES)
def test_real_repo_documents_that_used_to_have_empty_ministry(doc_name, expected):
    """실제로 비어 있던 데이터 레포 문서로 확인한다(레포가 있을 때만, 읽기 전용)."""
    source = REPO_ROOT / doc_name / f"{doc_name}.json"
    if not source.exists():
        pytest.skip(f"데이터 레포 없음: {source}")
    raw = json.loads(source.read_text(encoding="utf-8"))
    assert not (raw.get("basic_info") or {}).get("소관부처명"), "이 문서는 소관부처명이 비어 있어야 재현이 된다"
    objects = map_admrul_data(raw, source)
    assert objects
    assert {o.ministry for o in objects} == {expected}
