import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

# meta(JSON 문자열 1개)로 묶는 필드 — top-level 프로퍼티로 안 두고 조회/표시용으로만 보관한다
# (weaviate-schema.md "meta 내부 키"). 새 meta 키를 추가하면 여기에 이름만 넣으면 자동 포함된다.
_META_FIELDS = frozenset({
    "git_commit", "content_hash", "source_repository", "source_file_path",
    "start_page", "end_page", "chunk_bboxes", "media_files", "guardrail_categories",
    "image_urls", "file_hash", "source_relative_path", "n_char", "n_word", "n_line",
    "parser_reg_date", "relation_refs", "file_kind",
    # 조문별 미래 시행 예고(현재조 청크에 붙임 — "언제부터 바뀐다"). 벡터엔 안 섞고 표시용.
    "scheduled",
    # 조례 위임 요약(법령 전용) — payload 최상위 ordinance_delegations 를 조문별로 축약해 붙인다.
    #   조문 하나에 위임 조례가 수백 개(지방자치법 제7조=638개)라 개별 나열 대신
    #   [{link_text, count}] 만 싣는다. 전체 목록은 git 원문 JSON 에 있다.
    "ordinance_delegations",
    # 문서레벨(첫 청크에만 붙임 — 복제 지양). 개정문·개정이유는 별도 AMENDMENT 청크로 뺐다(검색 대상, meta 아님).
    "promulgation_no", "future_enforcement_dates",
    "repealed", "repeal_scheduled", "repealed_at",
})
# 스키마에서 뺀 필드. embedding_* = 컬렉션 단위 메타. is_file_only = source_type="FILE" 와 중복.
_DROP_FIELDS = ("vector", "embedding_model", "embedding_dimension", "is_file_only")


@dataclass
class LegalProvision:
    """Weaviate에 들어가는 조문/부칙/별표/첨부 청크 단위의 표준 payload."""

    chunk_id: str
    provision_id: Optional[str]
    parent_provision_id: Optional[str]
    reference_ids: List[str]
    file_id: Optional[str]
    law_id: Optional[str]
    mst: Optional[str]
    version_uid: Optional[str]
    law_name: Optional[str]
    law_abbr: Optional[str]
    law_type: Optional[str]
    unit_type: str
    unit_no: Optional[str]
    unit_title: Optional[str]
    chapter: Optional[str]
    content: str
    search_text: str
    source_type: str
    # 이 chunk가 어느 컬렉션 소속인지(wire key 'domain'). source_type(JSON/FILE 유래 구분)과는
    # 별개 축이라 이름을 겹치지 않게 분리했다. 값: "law" | "admrul" | "schlpub" — 레포·컬렉션과 1:1.
    collection_type: str
    file_name: Optional[str]
    file_url: Optional[str]
    page_no: Optional[int]
    chunk_index: int
    chunk_count: int
    promulgation_date: Optional[str]
    enforcement_date: Optional[str]
    revision_date: Optional[str]
    revision_type: Optional[str]
    is_current: Optional[bool]
    is_future: Optional[bool]
    source_url: Optional[str]
    git_path: Optional[str]
    git_commit: Optional[str] = None
    content_hash: Optional[str] = None
    embedding_model: Optional[str] = None
    embedding_dimension: Optional[int] = None
    # Git 연동 메타(적재 근거 추적용)
    source_repository: Optional[str] = None
    # JSON 파일 자체는 git_path 가 이미 담당한다(입력 파일 경로). 이 필드는 is_file_only
    # 첨부 처리 시 실제로 전처리기에 넘긴 로컬 첨부파일 경로 전용.
    source_file_path: Optional[str] = None
    is_file_only: Optional[bool] = None
    # 전처리기(Doc Parser) 응답의 페이지 범위 — 기존 page_no(단일 페이지)와 별개로 시작/끝 페이지 보존.
    start_page: Optional[int] = None
    end_page: Optional[int] = None
    # 전처리기 사용 시에만 채워지는 부가 메타(문자열이 아니면 JSON 직렬화해 저장).
    chunk_bboxes: Optional[str] = None
    media_files: Optional[str] = None
    guardrail_categories: Optional[str] = None
    # 조문 본문 안 인라인 이미지(article.images[])를 JSON 직렬화해 보존한다. OCR/설명 청크는
    # 아직 안 만들지만(§4), 이미지 URL·연결 관계는 버리지 않는다.
    image_urls: Optional[str] = None
    # 행정규칙류 저장 키(예: "admrul:67925"). version_uid 에도 녹아있지만(합성 규칙), 별도
    # 필터링/조회를 위해 독립 필드로도 보존한다. 법령류에는 없음(None).
    adm_uid: Optional[str] = None
    # FILE 청크 전용: 원본 첨부파일 자체의 해시(청크 텍스트 해시인 content_hash 와 다르다).
    file_hash: Optional[str] = None
    # source_file_path(로컬 절대경로, 디버깅용)와 별개로 Git 저장소 기준 상대경로를 보존한다.
    # 다른 서버/환경에서도 재현 가능하도록.
    source_relative_path: Optional[str] = None
    # Doc Parser 원시 응답의 부가 통계(§23-8) — 검색·필터에 자주 안 쓰여 독립 필드로만 최소 보존.
    n_char: Optional[int] = None
    n_word: Optional[int] = None
    n_line: Optional[int] = None
    parser_reg_date: Optional[str] = None
    # 소관부처(basic_info.소관부처) — 부처별 검색 facet. top-level 필터.
    ministry: Optional[str] = None
    # 관계 확정정보 [{reference_id,target_mst,resolve_method}] — meta. 판단은 reference_ids 로.
    relation_refs: Optional[Any] = None
    # FILE 세부 구분(document_file/attachment) — meta.
    file_kind: Optional[str] = None
    # 문서레벨 개정 메타 — 문서당 첫 청크에만 붙인다(청크 복제 지양). meta 로 묶임.
    amendment_text: Optional[str] = None
    revision_reason: Optional[str] = None
    promulgation_no: Optional[str] = None
    future_enforcement_dates: Optional[Any] = None
    # 조문별 미래 시행 예고(현재조 청크에 붙임): [{enforcement_date, change}] — "언제부터 바뀐다".
    # 미래 전문(content)은 payload 비대화 피하려고 제외한다.
    scheduled: Optional[Any] = None
    # 조례 위임 요약 [{link_text, count}] — 법령 조문 전용(meta 로 묶임)
    ordinance_delegations: Optional[Any] = None
    # 문서 폐지 상태 — 문서레벨(첫 청크). repeal_scheduled=폐지예정(아직 현행).
    repealed: Optional[bool] = None
    repeal_scheduled: Optional[bool] = None
    repealed_at: Optional[str] = None
    vector: Optional[List[float]] = field(default=None, repr=False)

    def properties(self) -> Dict[str, Any]:
        """Weaviate properties 로 변환한다(weaviate-schema.md 반영).

        - vector·embedding_model·embedding_dimension 는 저장 안 함(_DROP_FIELDS).
        - collection_type 은 wire key 'domain' 으로 내보낸다.
        - _META_FIELDS 는 top-level 대신 'meta'(JSON 문자열 1개)로 묶는다.
        - None 값은 top-level·meta 양쪽에서 생략한다.
        """
        data = asdict(self)
        for dropped in _DROP_FIELDS:
            data.pop(dropped, None)
        domain = data.pop("collection_type", None)
        meta = {}
        for key in _META_FIELDS:
            value = data.pop(key, None)
            if value is not None:
                meta[key] = value
        props = {key: value for key, value in data.items() if value is not None}
        if domain is not None:
            props["domain"] = domain
        if meta:
            props["meta"] = json.dumps(meta, ensure_ascii=False)
        return props
