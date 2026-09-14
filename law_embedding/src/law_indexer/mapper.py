import hashlib
import json
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .chunking import split_prose, split_table_like
from .models import LegalProvision
from .preprocess import is_deleted_appendix_content


class MappingError(ValueError):
    """입력 JSON을 Weaviate 적재 객체로 바꿀 수 없을 때 쓰는 매핑 오류."""

    pass


def stable_id(*parts: Any) -> str:
    """여러 식별자 조각을 합쳐 재실행해도 같은 SHA-256 id를 만든다."""
    normalized = "\x1f".join("" if part is None else str(part).strip() for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def iso_date(value: Any) -> Optional[str]:
    """수집기 날짜 문자열을 Weaviate DATE가 받는 ISO 문자열로 정규화한다."""
    if value is None or value == "":
        return None
    text = str(value).strip().replace(".", "-").rstrip("-")
    compact = text.replace("-", "")
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:]}T00:00:00Z"
    if "T" in text:
        return text if text.endswith("Z") or "+" in text[10:] else text + "Z"
    # 이상한 날짜 하나 때문에 문서 전체를 드롭하지 않는다 — 그 필드만 비운다(None).
    return None


def build_search_text(*values: Any) -> str:
    """법령명·조문번호·본문 등을 줄바꿈으로 합쳐 임베딩 대상 텍스트를 만든다."""
    return "\n".join(str(value).strip() for value in values if value is not None and str(value).strip())


def object_uuid(chunk_id: str) -> str:
    """chunk_id에서 Weaviate 객체 UUID를 결정적으로 만든다."""
    import uuid
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"law-indexer:{chunk_id}"))


def _references(unit: Dict[str, Any]) -> List[str]:
    """조문 relations에서 중복 없는 reference_id 목록을 뽑는다."""
    found = []
    for relation in unit.get("relations") or []:
        reference_id = relation.get("reference_id") if isinstance(relation, dict) else None
        if reference_id and reference_id not in found:
            found.append(reference_id)
    return found


def _scheduled(unit: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """조문의 미래 시행 예고(article.scheduled)를 [{enforcement_date, change, content}] 로 보존한다.

    change ∈ 개정/신설/삭제. 현재조 청크 meta 에 붙여 "이 조가 언제 어떻게 바뀐다"를 같이 낸다
    (별도 청크·벡터 아님). 삭제는 content 가 비어 있다."""
    out = []
    for s in unit.get("scheduled") or []:
        if not isinstance(s, dict):
            continue
        try:
            ed = iso_date(s.get("enforcement_date"))
        except MappingError:
            ed = _text(s.get("enforcement_date"))
        out.append({"enforcement_date": ed, "change": _text(s.get("change")),
                    "content": _text(s.get("content"))})
    return out or None


def _relation_refs(unit: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """relations 에서 참조별 정보를 보존한다(meta). 대상 판단은 reference_ids(필터)로 하고,
    여기에는 인용/위임 문맥과 확정정보를 남긴다. reference_id 로 파싱 가능한 값(target_law_name·
    target_unit·target_category)은 넣지 않는다(중복). 빈 문자열은 null 로 접는다.

    target_mst 는 admrul·render 참조에선 비어 있을 수 있다(null) — 링크 해석은 이름
    기반(reference_id=provision_id 형식)이라 mst 없이도 된다."""
    refs, seen = [], set()
    for relation in unit.get("relations") or []:
        if not isinstance(relation, dict):
            continue
        rid = relation.get("reference_id")
        if not rid or rid in seen:
            continue
        seen.add(rid)
        ref = {
            "reference_id": rid,
            "target_mst": relation.get("target_mst") or None,
            "relation_type": relation.get("relation_type") or None,
            "source_clause": relation.get("source_clause") or None,
            "line_text": relation.get("line_text") or None,
            "link_text": relation.get("link_text") or None,
            "target_article_title": relation.get("target_article_title") or None,
            "target_url": relation.get("target_url") or None,
            "resolve_method": relation.get("resolve_method") or None,
        }
        # 원문 되짚기용 경로. RAG 가 "벡터검색 → 애매하면 relation 따라가 원문 읽기" 를 하려면
        #   relation 하나로 파일에 닿아야 한다. 수집기는 **모든** 관계에 추정 경로를 붙이고
        #   (target_repo·target_git_path_guess), 전체 인덱스를 가진 enrich 가 돌면 확정 경로로
        #   덮어쓴다(target_git_path·target_source_repository). 둘 다 실어 소비자가 확정 우선,
        #   없으면 추정으로 폴백하게 한다 — 추정은 빗나가면 그 경로에 파일이 없을 뿐 다른 문서를
        #   가리키지는 않는다. 예전엔 확정 키만 읽어, enrich 를 안 돌리면 경로가 통째로 빠졌다.
        for key in (
            "source_admrul_seq",
            "target_doc_target",
            "target_doc_kind",
            "target_git_path",
            "target_source_repository",
            "target_repo",
            "target_git_file_path",
            "target_git_path_guess",
            # 동명 대상 정보 — enrich 가 이름이 겹치는 대상에 남긴다. target_candidates 는 후보 수,
            # target_candidate_paths 는 후보 전원 목록(경로·진짜 id·현행여부·시행일). 대표를 못
            # 정한(동률) 관계는 확정 경로 없이 목록만 오므로, 이걸 안 실으면 소비자가 동명 상황
            # 자체를 알 수 없다.
            "target_candidates",
            "target_candidate_paths",
            "target_missing_unit",
        ):
            if relation.get(key):
                ref[key] = relation.get(key)
        refs.append(ref)
    return refs or None


def _synthesize_admrul_version_uid(data: Dict[str, Any]) -> Optional[str]:
    """행정규칙류는 version_uid 가 없다(실제 샘플 전수 확인 — admrul/school/pi/public 공통).

    대신 문서 저장 키 adm_uid(예: "admrul:67925")가 항상 있어, adm_uid + revision_date(없으면
    enforcement_date)를 합치면 법령의 version_uid(law_id:mst:enforcement_date)와 같은 역할
    (문서+버전 식별)을 한다. 행정규칙류도 이제 연혁·시행예정을 함께 보관하지만, 데이터레포의
    한 시점 체크아웃에는 문서당 JSON 이 하나뿐이고 버전마다 개정일·시행일이 달라 충돌하지 않는다."""
    adm_uid = _text(data.get("adm_uid"))
    version_basis = _text(data.get("revision_date")) or _text(data.get("enforcement_date"))
    if adm_uid and version_basis:
        return f"{adm_uid}:{version_basis}"
    return adm_uid


def _common(data: Dict[str, Any], path: Path, doc_source: str = "law",
           source_repository: Optional[str] = None) -> Dict[str, Any]:
    """법령/행정규칙 JSON 최상위 공통 메타데이터를 LegalProvision 필드 dict로 변환한다."""
    source = data.get("source") or {}
    basic = data.get("basic_info") or {}
    version_uid = _text(data.get("version_uid"))
    if not version_uid and doc_source == "admrul":
        version_uid = _synthesize_admrul_version_uid(data)
    return dict(
        law_id=_text(data.get("law_id")), mst=_text(data.get("mst")),
        version_uid=version_uid, law_name=_text(data.get("law_name")),
        law_abbr=_text(data.get("law_abbr")), law_type=_text(data.get("law_type")),
        promulgation_date=iso_date(data.get("promulgation_date")),
        revision_date=iso_date(data.get("revision_date")), revision_type=_text(data.get("revision_type")),
        is_current=data.get("is_current"), is_future=data.get("is_future"),
        source_url=_text(source.get("source_url")), git_path=str(path),
        git_commit=_text(data.get("git_commit") or source.get("git_commit")),
        # domain 은 payload 의 doc_target 로 정한다 — map_admrul_data 가 학칙공단 문서에도
        #   doc_source='admrul' 을 넘기므로 그대로 쓰면 학칙공단 청크 domain 이 admrul 이 된다.
        collection_type=_collection_source(data, doc_source), source_repository=source_repository,
        adm_uid=_text(data.get("adm_uid")),
        # 소관부처 — 정규화된 top-level 필드엔 없고 원본 basic_info 에만 있다(부처별 필터 facet).
        # 법령 API 는 `소관부처`({content: 부처명, 소관부처코드: ...} 중첩객체)로 주는데,
        #   행정규칙류 API(행정규칙기본정보)는 `소관부처명`(평문)으로 준다 — 키 이름이 다르다.
        #   법령 키만 보면 행정규칙·학칙·공단정관 세 컬렉션의 ministry 가 전부 비어 부처 필터가
        #   죽는다(실측: admrul 10/10 · public 2/2 None). 두 키를 다 받는다.
        ministry=_ministry(basic.get("소관부처") or basic.get("소관부처명")),
    )


def _ministry(value: Any) -> Optional[str]:
    """basic_info.소관부처 에서 부처명만 뽑는다. 값이 {content, 소관부처코드} 중첩객체라
    content(부처명)를 쓰고, 혹시 평문이면 그대로 쓴다."""
    if isinstance(value, dict):
        return _text(value.get("content"))
    return _text(value)


def _text(value: Any) -> Optional[str]:
    """빈 문자열은 None으로 접고, 나머지는 trim된 문자열로 만든다."""
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _compact_name(value: Any) -> Optional[str]:
    """collector.common.compact 와 맞춘 provision_id 이름부 정규화."""
    text = _text(value)
    if not text:
        return None
    return (unescape(text).replace(" ", "").replace("　", "").replace("\n", "").replace("\t", "")
            .replace("ㆍ", "").replace("·", "").replace("․", "").replace("‧", "").replace("・", "").strip()) or None


def _id_prefix(data: Dict[str, Any], doc_source: str) -> str:
    """fallback provision_id 에 쓸 문서 prefix 를 payload 정체성에서 정한다.

    Weaviate 컬렉션은 law/admrul 두 개지만, ID 는 relation reference_id 와 같은 큰 분류로 물려야 한다.
    학칙/공단정관/공공기관은 개별 target 이 모호하게 들어오는 경우가 많아 SchlPubRul 으로 접고,
    자치법규/조례는 ordin 으로 둔다. eflaw 는 내부 ID 에서 law 로 정규화한다.
    """
    raw = _text(data.get("doc_target")) or _text(data.get("collection_type")) or doc_source
    if raw in ("eflaw", "law", "법령"):
        return "law"
    if raw in ("admrul", "행정규칙"):
        return "admrul"
    if raw in ("school", "pi", "public", "SchlPubRul", "schlpub", "학칙공단", "학칙", "공단정관", "공공기관"):
        return "SchlPubRul"
    if raw in ("ordin", "ordinance", "자치법규", "조례"):
        return "ordin"
    return raw or "law"


def _doc_id_base(data: Dict[str, Any], id_prefix: str, law_id: Optional[str]) -> str:
    """fallback provision_id 의 문서부를 만든다.

    조문/별표 self ID 는 수집기가 공식 문서명 compact 기반으로 만들기 때문에, 임베딩기 fallback
    부칙/개정문도 같은 이름부를 써야 한 문서 안에서 ID 스킴이 갈라지지 않는다.
    """
    name = _compact_name(data.get("law_name"))
    return f"{id_prefix}:{name or law_id or 'unknown'}"


def _collection_source(data: Dict[str, Any], fallback: Optional[str]) -> str:
    """Weaviate collection/domain 값. **레포·컬렉션과 1:1** 로 law/admrul/schlpub 세 갈래다.

    예전엔 학칙공단(school·pi·public)을 admrul 로 접었는데, 컬렉션이 3개로 갈린 뒤에는 그러면
    `SchlPubRulProvisionIndex` 안의 청크 domain 이 전부 `admrul` 이 되어 컬렉션과 어긋난다.
    doc_target(payload 정체성)이 fallback(호출 경로의 doc_source)보다 우선한다 — map_admrul_data
    가 학칙공단 문서에도 doc_source='admrul' 을 넘기기 때문이다.
    (조례 `ordin` 은 수집 대상이 아니라 문서로는 오지 않는다 — 관계 대상 전용 접두.)"""
    raw = _text(fallback) or _text(data.get("collection_type")) or _text(data.get("domain"))
    target = _text(data.get("doc_target"))
    schlpub = ("school", "pi", "public", "schlpub", "SchlPubRul")
    if target in schlpub or raw in schlpub:
        return "schlpub"
    if target in ("admrul", "ordin", "ordinance") or raw in ("admrul", "ordin", "ordinance"):
        return "admrul"
    if raw == "eflaw" or target == "eflaw":
        return "law"
    return raw or "law"


def _to_text(value: Any) -> Optional[str]:
    """chunk_bboxes/media_files/guardrail_categories 같은 부가 메타를 저장 가능한 문자열로 만든다.

    Doc Parser 응답에서 이 필드들의 실제 타입이 문서화돼 있지 않아(예시엔 "..." 로만 표시),
    문자열이면 그대로 쓰고 아니면 JSON으로 직렬화해 원본 구조를 보존한다."""
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


def _ordinance_summaries(data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """payload 최상위 ordinance_delegations(조례 위임)를 조문별 **요약**으로 접는다(법령 전용).

    조문 하나가 위임하는 조례가 수백 개일 수 있어(지방자치법 제7조=638개) 개별 조례를
    나열하지 않고 [{link_text, count}] 만 만든다 — "이 조항은 각 지자체 조례로 위임(N건)"
    수준의 답변 근거. 전체 조례 목록·URL 은 git 원문 JSON 에 그대로 있다.
    행정규칙류 payload 에는 이 필드가 없어 빈 dict 가 된다."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    for entry in data.get("ordinance_delegations") or []:
        if not isinstance(entry, dict):
            continue
        article_no = _text(entry.get("source_article_no"))
        if not article_no:
            continue
        count = entry.get("ordinance_count")
        if count is None:
            count = len(entry.get("ordinances") or [])
        out.setdefault(article_no, []).append({
            "link_text": _text(entry.get("link_text")), "count": count,
        })
    return out


def _make_json(data: Dict[str, Any], path: Path, unit: Dict[str, Any], unit_type: str,
               unit_no: Optional[str], title: Optional[str], provision_id: str,
               doc_source: str = "law", source_repository: Optional[str] = None,
               identity_id: Optional[str] = None,
               ordinance_delegations: Optional[List[Dict[str, Any]]] = None) -> List[LegalProvision]:
    """조문/부칙/별표 단위 JSON 객체 하나를 LegalProvision 목록으로 변환한다(§5).

    설정된 최대 크기(chunking.MAX_CHUNK_CHARS) 이하면 그대로 1개다(실제 admrul_data 로 확인:
    조문 대부분 4000자 이하 — 무의미하게 쪼개지 않는다). 넘을 때만 문단/행 경계로 나눈다.
    별표(APPENDIX)는 표 형태일 수 있어 행 경계 분할(split_table_like), 조문/부칙은 문단 경계
    분할(split_prose)을 쓴다. 여러 조각으로 나뉘어도 각 조각의 search_text 에 장·조번호·제목을
    그대로 반복한다(§5-1) — 문맥이 빠지지 않도록."""
    common = _common(data, path, doc_source=doc_source, source_repository=source_repository)
    content = str(unit.get("content") or "")
    chapter = _text(unit.get("chapter")) if unit_type == "ARTICLE" else None
    # 임베딩 문맥은 **장 경로 전체**를 쓴다(있으면). `chapter` 는 가장 가까운 한 단계라
    #   "제2절 해고" 만 남아 어느 장 밑인지가 사라진다. 필드값은 종전대로 nearest 를 유지하고
    #   (표시·facet), 검색 텍스트에만 경로를 넣는다.
    chapter_ctx = (_text(unit.get("chapter_path")) or chapter) if unit_type == "ARTICLE" else None
    # 약칭·별칭(영문명·한자명)은 search_text 에 넣지 않는다.
    #   · 벡터는 search_text 로 만들어지는데, 별칭을 섞으면 같은 조문이라도 별칭 유무에 따라
    #     텍스트가 달라져 이전에 계산해 둔 벡터를 재사용할 수 없다(실측: 재사용률 43%까지 하락).
    #   · 별칭 키워드 검색은 `law_abbr` 속성으로 커버한다(벡터와 무관하므로 재임베딩 불필요).
    #   · relation 대상 해석은 payload 의 `name_aliases` 를 쓰므로 여기와 무관하다
    #     (수집기 enrich_relation_targets._name_alias_keys).
    images = unit.get("images") if unit_type == "ARTICLE" else None
    image_urls = _to_text(images) if images else None
    enforcement_date = iso_date(unit.get("enforcement_date") or data.get("enforcement_date"))
    parent_provision_id = _text(unit.get("parent_provision_id"))
    reference_ids = _references(unit)
    relation_refs = _relation_refs(unit)
    scheduled = _scheduled(unit)

    splitter = split_table_like if unit_type == "APPENDIX" else split_prose
    parts = splitter(content) if content else [content]
    total = len(parts)

    result = []
    chunk_identity = identity_id or provision_id
    for index, part in enumerate(parts):
        chunk_id = stable_id(common["version_uid"], chunk_identity, unit_type, unit_no, "JSON", "", index)
        result.append(LegalProvision(
            chunk_id=chunk_id, provision_id=provision_id, parent_provision_id=parent_provision_id,
            reference_ids=reference_ids if index == 0 else [], file_id=None, unit_type=unit_type,
            unit_no=unit_no, unit_title=title, chapter=chapter, content=part,
            search_text=build_search_text(common["law_name"], common["law_type"],
                                          chapter_ctx, unit_no, title, part),
            source_type="JSON", file_name=None, file_url=None, page_no=None,
            chunk_index=index, chunk_count=total,
            enforcement_date=enforcement_date,
            content_hash=hashlib.sha256(part.encode("utf-8")).hexdigest(),
            image_urls=image_urls if index == 0 else None,
            relation_refs=relation_refs if index == 0 else None,
            scheduled=scheduled if index == 0 else None,
            ordinance_delegations=ordinance_delegations if index == 0 else None, **common,
        ))
    return result


def _combined_addendum_unit(addenda: Iterable[Any]) -> Optional[Dict[str, Any]]:
    """부칙 목록을 문서당 ADDENDUM 청크 하나로 합친다.

    법제처 payload 는 과거 부칙을 여러 항목으로 주지만 RAG 에서는 "이 문서의 부칙" 단위로 찾는 편이
    안정적이다. 개별 부칙마다 hash id 를 만들면 재수집 때 순서/본문 미세 차이로 ID 가 흔들리고,
    relation 도 문서 부칙 전체를 가리키는 경우가 많아 문서당 #ADDENDUM 하나가 낫다.
    """
    blocks = []
    for index, addendum in enumerate(addenda or [], start=1):
        if not isinstance(addendum, dict):
            continue
        content = _text(addendum.get("content"))
        if not content:
            continue
        labels = []
        if addendum.get("is_future"):
            labels.append("시행예정")
        if addendum.get("enforcement_date"):
            labels.append(f"시행 {str(addendum.get('enforcement_date')).strip()}")
        if addendum.get("promulgation_date"):
            labels.append(f"공포 {str(addendum.get('promulgation_date')).strip()}")
        if addendum.get("promulgation_no"):
            labels.append(f"제{str(addendum.get('promulgation_no')).strip()}호")
        heading = f"[부칙 {index}" + (f" / {' '.join(labels)}" if labels else "") + "]"
        blocks.append(f"{heading}\n{content}")
    if not blocks:
        return None
    return {"content": "\n\n".join(blocks)}


def _map_provisions(data: Dict[str, Any], path: Path, doc_source: str,
                    source_repository: Optional[str] = None) -> List[LegalProvision]:
    """법령/행정규칙 JSON 하나를 조문·부칙·별표 단위 LegalProvision 목록으로 펼친다(공용 구현).

    is_file_only=True 인 별표/별지/서식은 여기서 스킵한다 — 실체가 파일에만 있어 본문으로 만들
    content 가 없고(collector 쪽에서 content 를 비워 둠), 실제 임베딩 대상 청크는
    pipeline.py 가 첨부파일을 찾아 Doc Parser 로 전처리한 뒤 별도로 만든다."""
    if not isinstance(data, dict) or not isinstance(data.get("body"), dict):
        raise MappingError("법령 JSON 최상위 객체와 body 객체가 필요합니다")
    law_id = _text(data.get("law_id"))
    version_uid = _text(data.get("version_uid"))
    if not version_uid and doc_source == "admrul":
        version_uid = _synthesize_admrul_version_uid(data)
    if not law_id or not version_uid:
        if doc_source == "admrul":
            raise MappingError("필수 식별자 law_id와 adm_uid(또는 version_uid)가 필요합니다")
        raise MappingError("필수 식별자 law_id와 version_uid가 필요합니다")

    id_prefix = _id_prefix(data, doc_source)
    doc_id = _doc_id_base(data, id_prefix, law_id)
    ordinance_by_article = _ordinance_summaries(data)
    result = []
    for article in data["body"].get("articles") or []:
        if not isinstance(article, dict):        # 비정상 원소(문자열/None) 하나로 문서 전체 드롭 방지
            continue
        provision_id = _text(article.get("provision_id"))
        if not provision_id:
            raise MappingError(f"조문 provision_id 누락: {article.get('article_no')}")
        article_no = _text(article.get("article_no"))
        result.extend(_make_json(data, path, article, "ARTICLE", article_no,
                                 _text(article.get("article_title")), provision_id,
                                 doc_source=doc_source, source_repository=source_repository,
                                 ordinance_delegations=ordinance_by_article.get(article_no or "")))
    addendum_unit = _combined_addendum_unit(data.get("addenda") or [])
    if addendum_unit:
        provision_id = _text(data.get("addendum_provision_id")) or f"{doc_id}#ADDENDUM"
        result.extend(_make_json(data, path, addendum_unit, "ADDENDUM", None, "부칙", provision_id,
                                 doc_source=doc_source, source_repository=source_repository))
    for appendix in data.get("appendices") or []:
        if not isinstance(appendix, dict):
            continue
        if appendix.get("is_file_only") is True:
            continue  # 파일 전용 별표/별지/서식 — pipeline.py 의 Doc Parser 경로에서 처리
        if is_deleted_appendix_content(str(appendix.get("content") or "")):
            continue  # "[라벨] 삭제" 안내문뿐인 삭제된 별표/별지/서식 — 실질 내용 없어 임베딩 제외
        no = _text(appendix.get("no"))
        branch = _text(appendix.get("branch"))
        unit_no = no + (f"-{branch}" if branch and branch != "00" else "") if no else branch
        provision_id = _text(appendix.get("provision_id"))
        if not provision_id:
            provision_id = f"{doc_id}#APPENDIX-{stable_id(version_uid, no, branch, appendix.get('kind'), appendix.get('title'))[:24]}"
        result.extend(_make_json(data, path, appendix, "APPENDIX", unit_no,
                                 _text(appendix.get("title")), provision_id,
                                 doc_source=doc_source, source_repository=source_repository,
                                 identity_id=_text(appendix.get("appendix_instance_id")) or None))
    # 개정문(amendment_text)·개정이유(revision_reason)는 별도 청크 1개로 넣는다(검색 대상).
    # 문서레벨이라 조문마다 복제하거나 meta 로 묶지 않고, 문서당 '개정' 청크 하나로 둔다.
    amendment = _text(data.get("amendment_text"))
    reason = _text(data.get("revision_reason"))
    if amendment or reason:
        blocks = []
        if amendment:
            blocks.append(f"[개정문]\n{amendment}")
        if reason:
            blocks.append(f"[개정이유]\n{reason}")
        amend_pid = _text(data.get("amendment_provision_id")) or f"{doc_id}#AMENDMENT"
        result.extend(_make_json(data, path, {"content": "\n\n".join(blocks)}, "AMENDMENT",
                                 None, "개정문·개정이유", amend_pid,
                                 doc_source=doc_source, source_repository=source_repository))
    # 그 외 문서레벨 메타는 문서당 첫 청크에만 붙인다(청크마다 복제 지양).
    # 완전한 연혁은 git_history 가 커밋별로 제공하므로 여기선 현행 버전 값만 최소 보존한다.
    if result:
        header = result[0]
        header.promulgation_no = _text(data.get("promulgation_no"))
        header.future_enforcement_dates = data.get("future_enforcement_dates") or None
        # 문서 폐지 상태(폐지예정 등) — 문서레벨이라 첫 청크에만.
        header.repealed = data.get("repealed")
        header.repeal_scheduled = data.get("repeal_scheduled")
        try:
            header.repealed_at = iso_date(data.get("repealed_at")) if data.get("repealed_at") else None
        except MappingError:
            header.repealed_at = _text(data.get("repealed_at"))
    return result


def map_law_data(data: Dict[str, Any], path: Path, source_repository: Optional[str] = None) -> List[LegalProvision]:
    """법령 본문 JSON 하나를 조문·부칙·별표 단위 LegalProvision 목록으로 펼친다."""
    return _map_provisions(data, path, doc_source="law", source_repository=source_repository)


def map_admrul_data(data: Dict[str, Any], path: Path, source_repository: Optional[str] = None) -> List[LegalProvision]:
    """행정규칙류(admrul/school/pi/public) 본문 JSON 하나를 LegalProvision 목록으로 펼친다.

    법령 mapper와 body/addenda/appendices 구조는 동일하지만(실제 샘플로 확인), 최상위
    version_uid 가 없다 — 법령 mapper의 필수 검증을 그대로 강제하지 않고 adm_uid 로 대체한다."""
    return _map_provisions(data, path, doc_source="admrul", source_repository=source_repository)


def build_attachment_provisions(data: Dict[str, Any], path: Path, doc_source: str,
                                source_repository: Optional[str], chunks: List[Dict[str, Any]], *,
                                provision_id: Optional[str], unit_type: str, file_name: str,
                                file_url: Optional[str], source_file_path: str,
                                appendix: Optional[Dict[str, Any]] = None,
                                source_relative_path: Optional[str] = None,
                                file_hash: Optional[str] = None) -> List[LegalProvision]:
    """is_file_only 별표/별지/서식 또는 문서 전체 원문을 Doc Parser로 전처리한 결과
    (preprocess.normalize_doc_parser_chunks 형식)를 LegalProvision 목록으로 만든다.

    map_attachment_data 와 달리 이미 알고 있는 상위 문서(data)·조항(provision_id)을 그대로 쓴다
    (파일명 매칭으로 best-effort 추정하지 않는다 — pipeline.py 가 JSON의 is_file_only 항목에서
    바로 호출하므로 어떤 문서·어떤 항목인지 이미 확정돼 있다).

    appendix 를 주면(별표/별지/서식인 경우) search_text 에 별표 종류·번호·제목까지 포함해
    문맥을 보강한다(§23-3) — 첨부파일 텍스트 안에 문서명이 안 적혀 있어도 검색되게 하기 위함."""
    common = _common(data, path, doc_source=doc_source, source_repository=source_repository)
    file_id = stable_id(common["version_uid"], provision_id or "ATTACHMENT", file_name)
    total = len(chunks)
    file_stem = Path(file_name).stem
    if appendix:
        no = _text(appendix.get("no"))
        branch = _text(appendix.get("branch"))
        unit_no_label = (f"{appendix.get('kind') or '별표'} {no}" + (f"의{branch}" if branch and branch != "00" else "")) if no else None
        title_ctx = [appendix.get("kind"), unit_no_label, _text(appendix.get("title"))]
        file_kind = "attachment"       # 별표/별지/서식 파일
    else:
        title_ctx = ["문서 전체 원문", file_stem]
        file_kind = "document_file"    # 문서 전체 원문 파일
    result = []
    for fallback_index, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            continue
        content = str(chunk.get("content") or "")
        raw_index = chunk.get("chunk_index")
        try:
            index = int(raw_index) if raw_index is not None else fallback_index
        except (ValueError, TypeError):
            index = fallback_index
        chunk_id = stable_id(common["version_uid"], provision_id or "ATTACHMENT", "FILE", file_id, index)
        page_no = chunk.get("page_no")
        page_ctx = f"{page_no}페이지" if page_no is not None else None
        result.append(LegalProvision(
            chunk_id=chunk_id, provision_id=provision_id, parent_provision_id=None,
            reference_ids=[], file_id=file_id, unit_type=unit_type, unit_no=None,
            unit_title=file_stem, chapter=None, content=content,
            search_text=build_search_text(
                common["law_name"], common["law_type"], *title_ctx, page_ctx, content),
            source_type="FILE", file_name=file_name, file_url=file_url,
            page_no=page_no, chunk_index=index, chunk_count=total,
            enforcement_date=iso_date(data.get("enforcement_date")),
            content_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            is_file_only=True, source_file_path=source_file_path, file_kind=file_kind,
            source_relative_path=source_relative_path, file_hash=file_hash,
            start_page=chunk.get("start_page"), end_page=chunk.get("end_page"),
            chunk_bboxes=_to_text(chunk.get("chunk_bboxes")), media_files=_to_text(chunk.get("media_files")),
            guardrail_categories=_to_text(chunk.get("guardrail_categories")),
            n_char=chunk.get("n_char"), n_word=chunk.get("n_word"), n_line=chunk.get("n_line"),
            parser_reg_date=_text(chunk.get("parser_reg_date")),
            **common,
        ))
    return result


def load_law_json(path: Path, source_repository: Optional[str] = None) -> List[LegalProvision]:
    """법령 JSON 파일을 읽어 map_law_data 결과로 반환한다."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MappingError(f"JSON 읽기 실패 ({path}): {exc}") from exc
    return map_law_data(data, path.resolve(), source_repository=source_repository)


def load_admrul_json(path: Path, source_repository: Optional[str] = None) -> List[LegalProvision]:
    """행정규칙류 JSON 파일을 읽어 map_admrul_data 결과로 반환한다."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MappingError(f"JSON 읽기 실패 ({path}): {exc}") from exc
    return map_admrul_data(data, path.resolve(), source_repository=source_repository)


def map_attachment_data(data: Dict[str, Any], path: Path,
                        collection_type: Optional[str] = None) -> List[LegalProvision]:
    """전처리된 첨부 청크 JSON을 FILE source LegalProvision 목록으로 변환한다."""
    if not isinstance(data, dict) or not isinstance(data.get("chunks"), list):
        raise MappingError("첨부 청크 입력에는 chunks 배열이 필요합니다")
    chunks = data["chunks"]
    resolved_collection_type = _collection_source(data, collection_type)
    version_uid = _text(data.get("version_uid"))
    provision_id = _text(data.get("provision_id"))
    file_name, file_url = _text(data.get("file_name")), _text(data.get("file_url"))
    # package 생산자가 file_id를 주면 그대로 사용한다. 없을 때만 초기색인
    # build_attachment_provisions와 같은 계산식으로 맞춰 중복 FILE 청크를 막는다.
    file_id = _text(data.get("file_id")) or stable_id(version_uid, provision_id or "ATTACHMENT", file_name)
    result = []
    for fallback_index, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            continue
        try:
            index = int(chunk.get("chunk_index", fallback_index))
        except (ValueError, TypeError):
            index = fallback_index
        content = str(chunk.get("content") or "")
        chunk_id = _text(chunk.get("chunk_id")) or stable_id(version_uid, provision_id or "ATTACHMENT", "FILE", file_id, index)
        # unit_type 은 4값(ARTICLE/ADDENDUM/APPENDIX/FILE)으로 정규화한다. 옛 §6/외부 레코드가
        # DOCUMENT_FILE/ATTACHMENT 를 보내도 FILE 로 접는다(원문/첨부 구분은 file_kind 가 담당).
        provided = _text(data.get("unit_type"))
        if provided in ("DOCUMENT_FILE", "ATTACHMENT"):
            provided = "FILE"
        unit_type = provided or ("APPENDIX" if provision_id else "FILE")
        result.append(LegalProvision(
            chunk_id=chunk_id, provision_id=provision_id, parent_provision_id=_text(data.get("parent_provision_id")),
            reference_ids=list(data.get("reference_ids") or []), file_id=file_id,
            law_id=_text(data.get("law_id")), mst=_text(data.get("mst")), version_uid=version_uid,
            law_name=_text(data.get("law_name")), law_abbr=_text(data.get("law_abbr")), law_type=_text(data.get("law_type")),
            unit_type=unit_type, unit_no=_text(data.get("unit_no")), unit_title=_text(data.get("unit_title") or file_name),
            chapter=None, content=content, collection_type=resolved_collection_type,
            search_text=build_search_text(data.get("law_name"), data.get("law_type"), data.get("unit_no"), data.get("unit_title") or file_name, content),
            source_type="FILE", file_name=file_name, file_url=file_url, page_no=chunk.get("page_no"),
            chunk_index=index, chunk_count=len(chunks), promulgation_date=iso_date(data.get("promulgation_date")),
            enforcement_date=iso_date(data.get("enforcement_date")), revision_date=iso_date(data.get("revision_date")),
            revision_type=_text(data.get("revision_type")), is_current=data.get("is_current"), is_future=data.get("is_future"),
            source_url=_text(data.get("source_url")),
            git_path=_text(data.get("git_path") or data.get("source_relative_path")) or str(path.resolve()),
            git_commit=_text(data.get("git_commit")),
            content_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            source_repository=_text(data.get("source_repository")),
            source_file_path=_text(data.get("source_file_path")), file_kind=_text(data.get("file_kind")) or "attachment",
            is_file_only=True, start_page=chunk.get("start_page", chunk.get("page_no")),
            end_page=chunk.get("end_page", chunk.get("page_no")),
            chunk_bboxes=_to_text(chunk.get("chunk_bboxes")), media_files=_to_text(chunk.get("media_files")),
            guardrail_categories=_to_text(chunk.get("guardrail_categories")),
        ))
    return result


def load_attachment_json(path: Path) -> List[LegalProvision]:
    """전처리된 첨부 청크 JSON 파일을 읽어 map_attachment_data 결과로 반환한다."""
    try:
        return map_attachment_data(json.loads(path.read_text(encoding="utf-8")), path)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MappingError(f"첨부 청크 JSON 읽기 실패 ({path}): {exc}") from exc
