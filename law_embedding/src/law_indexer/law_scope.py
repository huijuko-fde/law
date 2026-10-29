"""GenOS '법 목록 관리' 화면을 적재 범위로 쓴다 — 로그인 토큰도, 캐시도 쓰지 않는다.

화면(admin-front `/data/law-scope`)이 `genos_config.key='law_scope'` 에 저장한
`{"law_names": [...], "ministries": [...]}` 를 admin-api 조회 API 로 매번 새로 읽는다.
조회 API 는 클러스터 내부 소비처(코드서빙 워커)를 위해 무인증이다 — `/prompt/runtime` 과 같은 규약.

비교 규칙은 **수집기(temporal_law)와 같아야 한다**. 수집기는 `collector/common.compact()` 로
공백·가운뎃점·전각을 정규화한 뒤 비교하는데, 여기서 `strip()` 만 하면 같은 부처명이 양쪽에서
다르게 판정된다(수집기는 담고 색인기는 버린다 → 조용한 누락). 그래서 `compact()` 를 옮겨 왔다.
"""
import html
import json
import logging
import os
import re
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

SCOPE_PATH = "/data/law-scope"
DEFAULT_ADMIN_API_URL = "http://llmops-admin-api-service:8080"
_ENABLED_VALUES = frozenset({"1", "true", "yes", "on"})

# 가운뎃점 변형 5종 — 수집기 compact() 와 같은 집합을 지운다(한 글자라도 다르면 매칭이 갈린다).
_MIDDLE_DOTS = "ㆍ·․‧・"
# 전각 영숫자·괄호·문장부호 → 반각. 수집기 _FULLWIDTH_TO_ASCII 와 같은 범위(U+FF01~U+FF5E).
_FULLWIDTH = {c: c - 0xFEE0 for c in range(0xFF01, 0xFF5F)}
_DROP = {ord(c): None for c in " 　\n\t\r" + _MIDDLE_DOTS}
# 복수 소관부처는 한 문자열에 묶여 온다(실측: '경찰청,법무부'). 전체 문자열 비교만 하면
# 이런 문서가 통째로 빠진다 — 법제처 데이터의 8%가 이 형태다.
_SPLIT = re.compile(r"[,;]")


def compact(text) -> str:
    """비교용 정규화 — 수집기 `collector/common.compact()` 와 같은 규칙.

    HTML 엔티티 복원 → 공백·가운뎃점 제거 → 전각을 반각으로. 이 함수가 수집기와 어긋나면
    범위 판정이 양쪽에서 갈리므로, 수집기 쪽이 바뀌면 여기도 같이 바꿔야 한다.
    """
    if not text:
        return ""
    return html.unescape(str(text)).translate(_FULLWIDTH).translate(_DROP).strip()


def _ministries_of(basic: dict) -> list:
    """`basic_info` 에서 소관부처명을 꺼내 정규화된 개별 부처 목록으로 편다.

    필드가 흔들린다 — `소관부처` / `소관부처명`, 문자열 / `{"content": ...}` / 배열,
    그리고 한 문자열에 콤마로 묶인 복수 부처. 전부 평탄화해서 개별 이름으로 돌려준다.
    """
    raw = basic.get("소관부처") or basic.get("소관부처명")
    items = raw if isinstance(raw, (list, tuple)) else [raw]
    out = []
    for item in items:
        if isinstance(item, dict):
            item = item.get("content")
        if not isinstance(item, str):
            continue
        out.extend(p for p in _SPLIT.split(compact(item)) if p)
    return out


@dataclass(frozen=True)
class LawScope:
    """적재 범위. `law_names` / `ministries` 는 {정규화 키: 원본 표기} 이다.

    원본 표기를 들고 있는 이유는 미매칭 경고에 운영자가 화면에 **입력한 그대로** 보여주기
    위해서다(정규화된 키를 보여주면 오타를 찾기 어렵다).
    """

    law_names: dict
    ministries: dict
    # 미매칭(오타·구명칭) 감지용 누적 — matches() 가 채운다. 동등비교에서는 제외.
    matched_names: set = field(default_factory=set, compare=False)
    matched_ministries: set = field(default_factory=set, compare=False)

    def matches(self, raw) -> bool:
        """법령명 목록 ∪ 부처 목록. 둘 중 하나라도 걸리면 대상(OR)."""
        if not isinstance(raw, dict):
            raise ValueError("법령 JSON은 객체여야 합니다")
        basic = raw.get("basic_info") or {}
        if not isinstance(basic, dict):
            raise ValueError("basic_info는 객체여야 합니다")

        hit = False
        # 이름이 걸려도 부처까지 끝까지 본다 — 조기 반환하면 '걸린 부처'를 놓쳐
        # 미매칭 경고가 멀쩡한 부처를 오타로 신고한다.
        key = compact(raw.get("law_name"))
        if key and key in self.law_names:
            self.matched_names.add(key)
            hit = True
        for mkey in _ministries_of(basic):
            if mkey in self.ministries:
                self.matched_ministries.add(mkey)
                hit = True
        return hit

    def unmatched(self) -> tuple:
        """범위에 적었는데 한 번도 안 걸린 항목(원본 표기) — (법령명, 부처).

        대개 오타이거나 구명칭(예: 안전행정부)이다. 이게 조용하면 범위가 의도보다 좁아진
        채로 '성공' 이 찍힌다 — 수집기도 같은 경고를 낸다.
        """
        return ([o for k, o in self.law_names.items() if k not in self.matched_names],
                [o for k, o in self.ministries.items() if k not in self.matched_ministries])


def _as_map(items, key: str) -> dict:
    if not isinstance(items, list) or any(not isinstance(x, str) or not x.strip() for x in items):
        raise ValueError(f"{key}는 빈 항목 없는 문자열 배열이어야 합니다")
    out = {}
    for x in items:
        k = compact(x)
        if k:                       # 정규화하면 사라지는 값(예: '·')은 버린다
            out[k] = x.strip()
    return out


def parse_scope(payload) -> LawScope:
    """admin-api 응답 `{"code":0,"errMsg":...,"data":{law_names,ministries}}` → LawScope.

    화면이 두 목록을 모두 비워 둔 상태(= 설정 미초기화 포함)는 '전량 적재'가 아니라 **중단**이다 —
    빈 목록을 무필터로 해석하면 실수로 전량 적재가 돌아간다.
    """
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise ValueError("법 목록 조회 실패: 정상 응답이 아닙니다")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("법 목록 응답의 data 가 객체가 아닙니다")
    names = _as_map(data.get("law_names"), "law_names")
    ministries = _as_map(data.get("ministries"), "ministries")
    if not (names or ministries):
        raise ValueError("법 목록 관리 화면의 목록이 비어 있어 적재를 시작하지 않습니다")
    return LawScope(names, ministries)


def load_law_scope():
    # 기존 배포의 무필터 동작을 깨지 않도록 명시적으로 켠다(켜지 않으면 전량 적재).
    if (os.getenv("LAW_SCOPE_ENABLED") or "").strip().lower() not in _ENABLED_VALUES:
        return None
    base = (os.getenv("LLMOPS_ADMIN_API_URL") or DEFAULT_ADMIN_API_URL).rstrip("/")
    request = Request(f"{base}{SCOPE_PATH}", headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"법 목록 조회 HTTP {exc.code}: 적재 중단") from None
    except (URLError, TimeoutError):
        raise RuntimeError("법 목록 API 연결 실패: 적재 중단") from None
    except json.JSONDecodeError:
        raise RuntimeError("법 목록 응답이 JSON 이 아닙니다: 적재 중단") from None
    return parse_scope(payload)
