"""law.go.kr 본문 API 원본 응답 → 조문·부칙·별표 구조 (수집기에서 가져온 순수 변환 함수).

출처: temporal_law/collector/law.py (HEAD 8ef506d + 2026-10-22 작업 트리). **네트워크·환경 의존 없음.**

왜 복사했나: 외부 수집기(투라인)가 law.go.kr 응답을 **그대로** 배달하고, 우리 수집기(temporal_law)는
운영에 올라가지 않는다. 적재기가 raw 형식을 직접 이해해야 하고, 수집기는 더 발전하지 않으므로
복사본이 어긋날 일이 없다. 적재기가 파일명 규칙(_appendix_label·_safe_name)을 같은 이유로
재구현해 둔 것의 연장이다. 수집기 원본과의 동일성은 tests/test_lawgo_raw_equivalence.py 가
실제 배달 ZIP 2건으로 고정한다.
"""

import re
from .common import (
    compact, enrich_file_meta, normalize_text,
)



def _drop_phantom_self_refs(articles: list) -> list:
    """자기 문서를 가리키는 internal_ref 중 **실재하지 않는 조문**을 가리키는 것을 제거한다.

    self 참조는 대상이 자기 문서라 조문 목록을 이미 알고 있으므로 **추측 없이 검증 가능**하다.
    law.go.kr 이 인용 앵커를 self 로 잘못 거는 경우가 있다(실측: 가맹사업법 제34조의3 의
    `「독점규제 및 공정거래에 관한 법률」 … "제124조부터 제127조까지"` 를 self 로 해석 → 44개 조문뿐인
    법에 제124~127조 유령 참조 4건). 외부 참조는 대상 문서를 아직 안 받아 검증할 수 없으므로 건드리지 않는다.

    반환: 제거된 항목 리스트(통계·추적용).
    """
    existing = {a.get("article_no") for a in articles if isinstance(a, dict)}
    # 번호 조문이 없는 문서(전문 1건짜리 특수법 등)는 검증 근거가 없다 → 아무것도 버리지 않는다.
    if not any(re.match(r"제\d+조", str(no or "")) for no in existing):
        return []
    dropped = []
    for art in articles:
        if not isinstance(art, dict):
            continue
        keep = []
        for r in art.get("relations") or []:
            tno = r.get("target_article_no")
            if (r.get("relation_type") == "internal_ref" and tno and tno not in existing):
                dropped.append({"source_article_no": art.get("article_no"),
                                "target_article_no": tno,
                                "link_text": r.get("link_text", ""),
                                "resolve_method": r.get("resolve_method", "")})
                continue                                  # 유령 — 남기면 안 물리는 dead ref
            keep.append(r)
        art["relations"] = keep
    return dropped


def _fill_line_text_from_content(art: dict) -> None:
    content = art.get("content") or ""
    if not content:
        return
    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if not lines:
        return
    cache: dict[str, str] = {}
    for rel in art.get("relations", []):
        if rel.get("line_text") or not rel.get("link_text"):
            continue
        lt = rel["link_text"]
        if lt not in cache:
            cache[lt] = next((ln for ln in lines if lt in ln), "")
        if cache[lt]:
            rel["line_text"] = cache[lt]


def _enrich_file_meta(appendices: list, articles: list) -> None:
    """공용 구현(common.enrich_file_meta)에 위임 — 법령·행정규칙류가 같은 파일명 규칙을 쓰게 한다.
    (예전엔 이 함수가 law.py 에만 있어 행정규칙·학칙공단 payload 에는 filename 이 없었다.)"""
    enrich_file_meta({"appendices": appendices or [], "body": {"articles": articles or []}})


def _name_aliases(law_name: str, law_abbr, basic_info: dict) -> list:
    """이 문서를 가리킬 수 있는 다른 표기들(한자명·영문명·약칭). 추측이 아니라 **API 제공 값**만 담는다."""
    out, seen = [], {compact(law_name)}
    for v in (law_abbr, basic_info.get("법령명_한자"), basic_info.get("법령명_영어"),
              basic_info.get("법령명약칭")):
        t = normalize_text(str(v or ""))
        if t and compact(t) not in seen:
            seen.add(compact(t))
            out.append(t)
    return out

