import json
import logging
import math
from contextlib import AbstractContextManager
from typing import Iterable, List

from .config import Settings
from .mapper import object_uuid
from .models import LegalProvision

logger = logging.getLogger("law_indexer.weaviate_store")


def _swap_renamed_head(rid, old_head: str, new_head: str):
    """reference_id 의 머리(`{접두}:{compact}`)가 old_head 와 정확히 일치할 때만 교체한다.

    머리 바로 뒤가 '#'(단위) 또는 ':'(동명 접미) 또는 문자열 끝이어야 한다 — 그래야
    '법인세법' 개명이 '법인세법시행령' 참조를 건드리지 않는다."""
    if (isinstance(rid, str) and rid.startswith(old_head)
            and (len(rid) == len(old_head) or rid[len(old_head)] in "#:")):
        return new_head + rid[len(old_head):]
    return rid


def _patch_renamed_meta(meta_raw, swap_fn, old_name: str, new_name: str):
    """meta(JSON 문자열)의 relation_refs 에서 개명 대상을 가리키던 참조만 고친다.

    reference_id 가 swap_fn 으로 바뀌는 항목만 대상으로, 그 항목의 경로 키(수집기 데이터 레포
    상대경로 — 폴더명이 문서명이라 이름이 바뀌면 경로도 바뀐다)에서 옛 이름을 새 이름으로
    치환한다. 인용 원문(line_text/link_text)은 손대지 않는다. 바뀐 게 없으면 None."""
    if not meta_raw or not isinstance(meta_raw, str):
        return None
    try:
        meta = json.loads(meta_raw)
    except ValueError:
        return None
    if not isinstance(meta, dict):
        return None
    changed = False
    for ref in meta.get("relation_refs") or []:
        if not isinstance(ref, dict):
            continue
        rid = ref.get("reference_id")
        new_rid = swap_fn(rid)
        if new_rid == rid:
            continue
        ref["reference_id"] = new_rid
        changed = True
        if old_name and new_name and old_name != new_name:
            for key in ("target_git_path", "target_git_path_guess", "target_git_file_path"):
                v = ref.get(key)
                if isinstance(v, str) and old_name in v:
                    ref[key] = v.replace(old_name, new_name)
            for cand in ref.get("target_candidate_paths") or []:
                if isinstance(cand, dict):
                    for k, v in list(cand.items()):
                        if isinstance(v, str) and old_name in v:
                            cand[k] = v.replace(old_name, new_name)
    if not changed:
        return None
    return json.dumps(meta, ensure_ascii=False)


def _f(name, type_, description, *, tokenization="field", range_filters=False):
    """필터 속성(indexFilterable ON · BM25 OFF)."""
    return dict(name=name, type=type_, description=description, filterable=True,
                searchable=False, tokenization=tokenization, range_filters=range_filters)


def _d(name, type_, description):
    """표시 전용 속성(인덱스 없음). 키워드 검색은 search_text 가 전부 커버한다."""
    return dict(name=name, type=type_, description=description, filterable=False,
                searchable=False, tokenization=None, range_filters=False)


# ── 컬렉션 스키마 (법령·행정규칙·학칙공단 3컬렉션 공용 — 클래스 이름만 다름) ─────────
#
# 속성 35 = 필터 18 + 키워드검색 1 + 표시 15 + 부가 1.
# 이 상수가 **유일한 출처**다 — 벡터DB 정의서(산출물)도 여기서 뽑는다.
#
# 설계 의도:
#   · 필터 텍스트는 전부 `field` 토크나이저 = 정확일치용. BM25 를 걸지 않는다.
#   · 키워드 검색은 `search_text` 하나만 담당한다(문서명+종류+조번호+제목+본문 합성).
#     본문·제목·장은 이미 그 안에 합성돼 있어 개별 속성에 BM25 를 중복으로 걸 이유가 없다.
#   · `enforcement_date` 는 시점 질의("2024년 기준")를 위해 범위 필터를 켠다.
#   · `meta` 는 나머지 추적 키를 JSON 문자열 1개로 — where 필터 대상이 아니다.
SCHEMA = [
    # 필터 18
    _f("chunk_id", "text", "필터 · 청크 UUID 근거"),
    _f("provision_id", "text", "필터 · 조문/부칙/별표 단위 ID"),
    _f("law_id", "text", "필터 · 문서 단위 ID(삭제/재적재 기준)"),
    _f("version_uid", "text", "필터 · 버전 식별자"),
    _f("file_id", "text", "필터 · 파일 청크 그룹 식별"),
    _f("unit_type", "text", "필터 · ARTICLE/ADDENDUM/APPENDIX/AMENDMENT/FILE"),
    _f("is_current", "boolean", "필터 · 현행 여부", tokenization=None),
    _f("enforcement_date", "date", "필터 · 시행일(as-of, 범위검색)",
       tokenization=None, range_filters=True),
    _f("reference_ids", "text[]", "필터 · 위임·인용 대상 provision_id 목록"),
    _f("parent_provision_id", "text", "필터 · 상위 조문/별표"),
    _f("ministry", "text", "필터 · 소관부처"),
    _f("domain", "text", "필터 · law/admrul/schlpub — 데이터 레포·컬렉션과 1:1"),
    _f("law_name", "text", "필터 · 법령/행정규칙명 (이 법 안에서만 검색용 exact match)"),
    _f("law_type", "text", "필터 · 법률/대통령령/부령/고시 등"),
    _f("source_type", "text", "필터 · JSON 본문/FILE 첨부"),
    _f("is_future", "boolean", "필터 · 시행예정(is_current 와 함께 현행/미래 분리)",
       tokenization=None),
    _f("revision_type", "text", "필터 · 제정/일부개정/전부개정/타법개정"),
    # git_path 를 필터로 승격(2026-08, MCP 요청) — 검색 결과의 git_path 로 같은 문서/파일의
    #   다른 청크를 바로 모으는 용도. field 토크나이저 = 경로 전체 정확일치.
    _f("git_path", "text", "필터 · 원본 JSON 경로(데이터 저장소 상대경로 — 연혁 추적·정확일치)"),
    # 키워드 검색 1 — 하이브리드의 BM25 절반을 이 속성 하나가 담당한다
    dict(name="search_text", type="text", filterable=False, searchable=True,
         tokenization="kagome_kr", range_filters=False,
         description="BM25+표시 · 임베딩 대상 원문(법령명+종류+조번호+제목+content 합성)."
                     " tokenization=kagome_kr(한국어 형태소). 모듈 미탑재 시 trigram 대체"),
    # 표시 15
    _d("content", "text", "표시 · 청크 본문 (키워드 검색은 search_text 로 커버)"),
    _d("law_abbr", "text", "표시 · 약칭"),
    _d("unit_no", "text", "표시 · 조문/부칙/별표 번호"),
    _d("unit_title", "text", "표시 · 단위 제목"),
    _d("chapter", "text", "표시 · 장/절"),
    _d("source_url", "text", "표시 · law.go.kr URL"),
    _d("mst", "text", "표시 · 법제처 MST (링크 해석은 이름 기반이라 필터 불필요)"),
    _d("adm_uid", "text", "표시 · 행정규칙 저장키"),
    _d("file_name", "text", "표시 · 파일명"),
    _d("file_url", "text", "표시 · 파일 URL"),
    _d("page_no", "int", "표시 · 대표 페이지"),
    _d("chunk_index", "int", "표시 · 단위 내 청크 순번"),
    _d("chunk_count", "int", "표시 · 단위 내 청크 수"),
    _d("promulgation_date", "date", "표시 · 공포일"),
    _d("revision_date", "date", "표시 · 개정일"),
    # 부가 1
    _d("meta", "text", "meta · 나머지 부가·추적 키를 JSON 문자열 1개로"
                       "(relation_refs·file_kind·개정문 등). where 필터 불가"),
]


class WeaviateStore(AbstractContextManager):
    """law_embedding 전용 Weaviate 연결·스키마 생성·upsert·검색 래퍼."""

    def __init__(self, settings: Settings, source: str | None = None):
        """설정된 HTTP/gRPC 주소로 Weaviate client를 연결한다.

        API 키가 있으면 키 인증으로 붙는다(genos 등 인증 Weaviate). 비우면 익명 접속(로컬 개발
        그대로). WEAVIATE_SECURE 로 http/grpc TLS 여부를 함께 켠다.

        source("law"|"admrul"|"schlpub")를 주면 **그 컬렉션의 키**로 붙는다 — genos 는 VDB 를
        컬렉션 단위로 나눠 발급해 키가 서로 다르고, 각 키는 자기 컬렉션만 보인다(RBAC). 그래서
        한 연결로 여러 컬렉션을 다룰 수 없다. 생략하면 법령 키(기존 동작)."""
        import weaviate
        from weaviate.classes.init import Auth
        self.settings = settings
        self.source = source
        api_key = settings.api_key_for(source)
        auth = Auth.api_key(api_key) if api_key else None
        self.client = weaviate.connect_to_custom(
            http_host=settings.weaviate_http_host, http_port=settings.weaviate_http_port,
            http_secure=settings.weaviate_secure,
            grpc_host=settings.weaviate_grpc_host, grpc_port=settings.weaviate_grpc_port,
            grpc_secure=settings.weaviate_secure,
            auth_credentials=auth,
        )

    def __exit__(self, *args):
        """context manager 종료 시 Weaviate client 연결을 닫는다."""
        self.client.close()

    def health(self) -> bool:
        """Weaviate 서버가 요청을 받을 준비가 됐는지 확인한다."""
        return self.client.is_ready()

    def create_collection(self, name: str, recreate: bool = False) -> bool:
        """법령/행정규칙 컬렉션을 만들고, recreate면 기존 컬렉션을 지운 뒤 다시 만든다.

        세 컬렉션은 **properties 가 완전히 동일**하고 이름만 다르다(법령 LegalProvisionIndex /
        행정규칙 AdmrulProvisionIndex / 학칙공단 SchlPubRulProvisionIndex). 스키마 정의는
        모듈 상수 SCHEMA 하나에 모아 두었다 —
        벡터DB 정의서(산출물)와 실제 컬렉션이 어긋나지 않게 하려고 코드가 유일한 출처다.

        키워드 검색은 `search_text` 하나로 처리한다(BM25). 한국어 형태소 토크나이저
        `kagome_kr` 을 쓰는데, Weaviate 에 해당 모듈(ENABLE_TOKENIZER_KAGOME_KR)이 없으면
        컬렉션 생성이 거부되므로 그때는 `trigram` 으로 한 번 더 시도한다(검색 품질은 떨어지지만
        하이브리드 검색 자체는 동작한다). 어느 쪽으로 만들었는지는 반환 대신 로그로 남긴다.
        """
        exists = self.client.collections.exists(name)
        if exists and recreate:
            self.client.collections.delete(name)
            exists = False
        if exists:
            return False

        wanted = (self.settings.search_text_tokenization or "kagome_kr").strip().lower()
        order = [wanted] + [t for t in ("kagome_kr", "trigram") if t != wanted]
        last_error = None
        for tok in order:
            try:
                self._create_with_tokenization(name, tok)
                if tok != wanted:
                    logger.warning("search_text 토크나이저 %s 실패 → %s 로 생성했습니다"
                                   " (한국어 키워드 검색 품질이 낮아집니다)", wanted, tok)
                else:
                    logger.info("컬렉션 %s 생성 (search_text 토크나이저=%s)", name, tok)
                return True
            except Exception as exc:                     # 모듈 미탑재·설정 거부 등
                last_error = exc
                # 생성이 부분 실패로 껍데기를 남겼으면 지우고 다음 토크나이저로.
                if self.client.collections.exists(name):
                    self.client.collections.delete(name)
        raise RuntimeError(
            f"컬렉션 {name} 생성 실패 (시도한 토크나이저: {', '.join(order)}): {last_error}")

    def _create_with_tokenization(self, name: str, tokenization: str) -> None:
        """SCHEMA 정의대로 컬렉션을 만든다. search_text 토크나이저만 인자로 갈아 끼운다."""
        from weaviate.classes.config import (Configure, DataType, Property, Tokenization,
                                             VectorDistances)
        dtypes = {"text": DataType.TEXT, "text[]": DataType.TEXT_ARRAY, "int": DataType.INT,
                  "date": DataType.DATE, "boolean": DataType.BOOL}
        toks = {"field": Tokenization.FIELD, "kagome_kr": Tokenization.KAGOME_KR,
                "trigram": Tokenization.TRIGRAM, "word": Tokenization.WORD}

        properties = []
        for spec in SCHEMA:
            tok = spec.get("tokenization")
            if spec["name"] == "search_text":
                tok = tokenization
            kwargs = dict(name=spec["name"], data_type=dtypes[spec["type"]],
                          description=spec["description"],
                          index_filterable=spec["filterable"])
            if spec["type"] in ("text", "text[]"):
                kwargs["index_searchable"] = spec["searchable"]
                if tok:
                    kwargs["tokenization"] = toks[tok]
            if spec.get("range_filters"):
                kwargs["index_range_filters"] = True
            properties.append(Property(**kwargs))

        # ── 정의서에 있으나 예전엔 코드가 지정하지 않던 값들 ─────────────────
        # 안 넘기면 Weaviate 기본값으로 만들어져 정의서와 어긋난다(실측):
        #   filterStrategy   sweeping ≠ acorn      — 필터 걸린 벡터검색 경로가 달라진다
        #   replication      1 ≠ 3, async off      — 노드 장애 시 조회 불가
        #   sharding         노드 수 ≠ 3
        # 복제·샤드는 **노드 수를 넘길 수 없다** — 1노드 개발기에서 3 을 요구하면 생성이 거부되므로
        #   env 로 낮춘다(WEAVIATE_REPLICATION_FACTOR=1). acorn·async·deletionStrategy 는
        #   노드 수와 무관해 개발기에서도 정의서 그대로 만들어진다.
        from weaviate.classes.config import ReplicationDeletionStrategy, VectorFilterStrategy
        self.client.collections.create(
            name=name,
            properties=properties,
            vector_config=Configure.Vectors.self_provided(
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE,
                    ef_construction=128, max_connections=32,
                    filter_strategy=VectorFilterStrategy.ACORN)),
            inverted_index_config=Configure.inverted_index(
                bm25_b=0.75, bm25_k1=1.2,
                index_null_state=False, index_property_length=False, index_timestamps=False),
            replication_config=Configure.replication(
                factor=self.settings.weaviate_replication_factor,
                async_enabled=True,
                deletion_strategy=ReplicationDeletionStrategy.TIME_BASED_RESOLUTION),
            sharding_config=Configure.sharding(
                desired_count=self.settings.weaviate_shard_count,
                virtual_per_physical=128),
            multi_tenancy_config=Configure.multi_tenancy(enabled=False),
        )

    def drop_collection(self, name: str) -> bool:
        """컬렉션이 있으면 완전히 삭제한다(재생성 없이 독립적으로 지우고 싶을 때)."""
        if not self.client.collections.exists(name):
            return False
        self.client.collections.delete(name)
        return True

    def _validate(self, objects: Iterable[LegalProvision], dimension: int, model_name: str):
        """upsert 전 객체의 임베딩 모델명·벡터 차원·값이 정상인지 검증한다.

        차원(길이)뿐 아니라 **값**도 본다: NaN/Inf 나 전부-0 벡터는 길이가 맞아도 검색 랭킹을
        오염(NaN)하거나 recall 을 죽인다(0 벡터). 부분 serving 오류·양자화 버그로 조용히 들어오는
        이런 벡터를 여기서 거른다(적재 전 fail-fast)."""
        for obj in objects:
            if obj.embedding_model != model_name:
                raise ValueError(f"임베딩 모델 불일치: {obj.embedding_model!r} != {model_name!r}")
            vec = obj.vector or []
            actual = len(vec)
            if actual != dimension or obj.embedding_dimension != dimension:
                raise ValueError(f"벡터 차원 불일치 ({obj.chunk_id}): 실제 {actual}, 예상 {dimension}")
            if not any(vec) or not all(math.isfinite(v) for v in vec):
                raise ValueError(f"비정상 벡터 ({obj.chunk_id}): NaN/Inf 또는 전부 0 — 임베딩 서버 응답 확인")

    def upsert(self, objects: List[LegalProvision], dimension: int, model_name: str, collection: str) -> dict:
        """LegalProvision 객체들을 결정적 UUID로 교체 삽입해 재실행을 idempotent하게 만든다.

        collection 은 settings.law_collection/admrul_collection 중 하나를 명시적으로 받는다 —
        같은 chunk_id 라도 컬렉션이 다르면 완전히 별개 객체라 컬렉션마다 독립적으로 upsert된다."""
        self._validate(objects, dimension, model_name)
        collection = self.client.collections.get(collection)
        # embedding_model/dimension 은 청크마다 저장하지 않는다(weaviate-schema.md 생략). 배치 내
        # 일관성은 _validate 가 보장하고, 컬렉션 차원 혼용은 Weaviate self-provided 가 첫 적재 차원으로
        # 잠가 서로 다른 차원 벡터를 거부한다. 모델을 바꾸면 create-collection --recreate 후 재색인.
        existing = collection.query.fetch_objects(limit=1, include_vector=True).objects
        if existing:
            stored = existing[0].vector or {}
            stored_vec = stored.get("default") if isinstance(stored, dict) else stored
            stored_dimension = len(stored_vec) if stored_vec else None
            if stored_dimension and stored_dimension != dimension:
                raise ValueError(
                    f"컬렉션 벡터 차원 불일치: 저장됨 {stored_dimension}, 입력 {dimension}. "
                    "모델 변경 시 create-collection --recreate 후 다시 색인하세요."
                )
        # 기존 객체는 delete 후 add 하지 않고 replace 한다. add 실패·네트워크 오류가 나도
        # 이전 청크가 검색에서 사라지는 창을 만들지 않기 위해서다. 신규 객체만 batch add 한다.
        # 존재 확인은 **ID 일괄 조회**로 한다 — 예전처럼 객체마다 exists() 를 부르면 배치당
        # 수백 번의 왕복이 생겨 업서트가 임베딩만큼 느려졌다(실측 384건에 29s → 왕복이 지배).
        from weaviate.classes.query import Filter
        uids = [object_uuid(obj.chunk_id) for obj in objects]
        existing_ids: set = set()
        for start in range(0, len(uids), 500):
            group = uids[start:start + 500]
            res = collection.query.fetch_objects(
                filters=Filter.by_id().contains_any(group), limit=len(group))
            existing_ids.update(str(o.uuid) for o in res.objects)
        to_add: List[LegalProvision] = []
        replace_failed: List[str] = []
        for obj, uid in zip(objects, uids):
            if uid in existing_ids:
                try:
                    collection.data.replace(
                        uuid=uid, properties=obj.properties(), vector=obj.vector)
                except Exception:
                    replace_failed.append(uid)
            else:
                to_add.append(obj)
        with collection.batch.fixed_size(batch_size=100) as batch:
            for obj in to_add:
                batch.add_object(properties=obj.properties(), vector=obj.vector, uuid=object_uuid(obj.chunk_id))
        failed = list(collection.batch.failed_objects)
        failed_ids = replace_failed + [getattr(item, "original_uuid", None) or str(item) for item in failed]
        return {"success": len(objects) - len(failed_ids), "failed": len(failed_ids), "failed_ids": failed_ids}

    def delete_orphan_file_chunks(self, file_id: str, keep_chunk_ids: List[str], collection: str) -> int:
        """같은 file_id 로 이미 저장된 FILE 청크 중, 이번에 새로 만든 chunk_id 목록에 없는 것만
        지운다(§23-7) — 재처리 결과 청크 수가 줄어들면(예: Doc Parser 파라미터 변경, 원본 파일
        교체) 예전 청크가 그대로 남는 문제를 막는다. 다른 파일의 객체는 절대 건드리지 않는다
        (file_id 로 정확히 좁혀서 조회)."""
        from weaviate.classes.query import Filter

        coll = self.client.collections.get(collection)
        existing = coll.query.fetch_objects(
            filters=Filter.by_property("file_id").equal(file_id),
            limit=1000, return_properties=["chunk_id"],
        ).objects
        keep = set(keep_chunk_ids)
        removed = 0
        for obj in existing:
            if obj.properties.get("chunk_id") not in keep:
                coll.data.delete_by_id(obj.uuid)
                removed += 1
        return removed

    def delete_orphan_json_chunks(self, law_id: str, version_uid: str, keep_chunk_ids: List[str],
                                  collection: str) -> int:
        """같은 문서 버전의 JSON 청크 중 이번 재색인 결과에 없는 청크만 지운다.

        OCR 재색인처럼 기존 조문 content 가 바뀌면 청크 개수가 달라질 수 있다. upsert 는 같은
        chunk_id 만 교체하므로, 새 결과에 포함되지 않은 옛 ARTICLE/ADDENDUM/APPENDIX/AMENDMENT
        청크가 남지 않도록 source_type=JSON 으로 좁혀 정리한다. FILE 청크는 file_id 기준 정리와
        수명주기가 달라 여기서 건드리지 않는다.
        """
        from weaviate.classes.query import Filter

        if not (law_id and version_uid and keep_chunk_ids):
            return 0
        coll = self.client.collections.get(collection)
        existing = coll.query.fetch_objects(
            filters=(
                Filter.by_property("law_id").equal(law_id)
                & Filter.by_property("version_uid").equal(version_uid)
                & Filter.by_property("source_type").equal("JSON")
            ),
            limit=10000,
            return_properties=["chunk_id"],
        ).objects
        keep = set(keep_chunk_ids)
        removed = 0
        for obj in existing:
            if obj.properties.get("chunk_id") not in keep:
                coll.data.delete_by_id(obj.uuid)
                removed += 1
        return removed

    def delete_by_law_id(self, law_id: str, collection: str) -> int:
        """한 법(law_id)의 모든 객체를 지정 컬렉션에서 삭제한다 — 증분 재적재 시 옛 버전 청크 제거용.

        개정되면 version_uid 가 바뀌고 그에 따라 chunk_id·UUID 가 전부 바뀌므로, 단순 upsert 만
        하면 옛 버전 청크가 고아로 남는다(delete_orphan_file_chunks 는 같은 file_id FILE 청크만
        정리 — 조문/버전 교체는 못 잡는다). 그래서 변경·폐지 문서는 재적재 전에 law_id 단위로
        싹 지운다. 다른 법의 객체는 건드리지 않는다(law_id 로 정확히 좁혀 삭제)."""
        from weaviate.classes.query import Filter
        coll = self.client.collections.get(collection)
        result = coll.data.delete_many(where=Filter.by_property("law_id").equal(law_id))
        return getattr(result, "successful", None) or getattr(result, "matches", 0) or 0

    def delete_stale_law_chunks(self, law_id: str, keep_version_uids: List[str], collection: str) -> int:
        """law_id 의 저장 청크 중 version_uid 가 keep 목록에 **없는** 것만 지운다.

        'upsert 먼저 → 옛 버전 삭제' 순서용. 개정되면 새 version_uid 로 청크를 먼저 upsert 하고(새
        UUID 라 옛 청크와 공존), 그다음 이 함수로 옛 버전 청크만 정리한다. delete_by_law_id 처럼
        '먼저 전부 지우고 재적재'하지 않으므로, upsert 가 실패해도 옛 청크가 살아 있어 그 법이
        검색에서 통째로 사라지지 않는다. keep 이 비면(방어) 아무것도 지우지 않는다(전량삭제 사고 방지)."""
        keep = [v for v in (keep_version_uids or []) if v]
        if not keep:
            return 0
        from weaviate.classes.query import Filter
        coll = self.client.collections.get(collection)
        where = Filter.by_property("law_id").equal(law_id)
        for vuid in keep:
            where = where & Filter.by_property("version_uid").not_equal(vuid)
        result = coll.data.delete_many(where=where)
        return getattr(result, "successful", None) or getattr(result, "matches", 0) or 0

    def patch_renamed_references(self, old_head: str, new_head: str,
                                 old_name: str, new_name: str, collection: str) -> dict:
        """개명된 문서를 **참조하던** 청크들의 참조 meta 를 새 이름 기준으로 고친다(벡터 보존).

        reference_id 는 `{접두}:{compact(문서명)}[:{접미}]#{단위}` 꼴이라 문서명이 바뀌면 머리
        (`{접두}:{compact}`)가 바뀐다. old_head 로 시작하고 바로 뒤가 '#'/':'/끝 인 id 만 바꾼다 —
        like 검색은 후보 초과수집이다('법인세법*' 이 '법인세법시행령…' 도 잡는다) → 정밀 판정은
        파이썬에서 한다. 고치는 것: ① reference_ids 배열 ② meta(JSON 문자열)의 relation_refs 안
        reference_id 와 경로 키(target_git_path 류). 인용 원문(line_text/link_text)은 출처 조문이
        실제로 쓴 문구라 **바꾸지 않는다**. 본문·벡터는 그대로(재임베딩 불필요). 멱등 —
        같은 record 를 두 번 받아도 두 번째는 바꿀 것이 없다."""
        empty = {"chunks": 0, "refs": 0}
        if not old_head or old_head == new_head:
            return empty
        if not self.client.collections.exists(collection):
            return empty                                   # 이 배포에 없는 컬렉션 — 조용히 스킵
        from weaviate.classes.query import Filter
        coll = self.client.collections.get(collection)

        def _swap(rid):
            return _swap_renamed_head(rid, old_head, new_head)

        seen, chunks, refs = set(), 0, 0
        offset = 0
        while True:
            # 커서(after)는 필터와 함께 못 쓴다 → offset 페이징. patch 된 객체는 필터에서 빠지므로
            # 진전이 있으면 offset 을 유지하고, 전부 초과수집(다른 문서)이면 창을 넘긴다.
            res = coll.query.fetch_objects(
                filters=Filter.by_property("reference_ids").like(f"{old_head}*"),
                limit=200, offset=offset, return_properties=["reference_ids", "meta"])
            todo = [o for o in res.objects if str(o.uuid) not in seen]
            if not todo:
                if len(res.objects) < 200:
                    break
                offset += 200
                continue
            progressed = False
            for obj in todo:
                seen.add(str(obj.uuid))
                props = {}
                rids = list(obj.properties.get("reference_ids") or [])
                new_rids = [_swap(r) for r in rids]
                changed_refs = sum(1 for a, b in zip(rids, new_rids) if a != b)
                if changed_refs:
                    props["reference_ids"] = new_rids
                new_meta = _patch_renamed_meta(obj.properties.get("meta"), _swap,
                                               old_name, new_name)
                if new_meta is not None:
                    props["meta"] = new_meta
                if props:
                    coll.data.update(uuid=obj.uuid, properties=props)
                    chunks += 1
                    refs += changed_refs
                    progressed = True
            if not progressed:
                if len(res.objects) < 200:
                    break
                offset += 200
        return {"chunks": chunks, "refs": refs}

    def has_provision_head(self, head: str, collection: str) -> bool:
        """provision_id 가 `head`(#단위/:접미 경계 포함)로 시작하는 청크가 있는지 — 개명 패치의
        동명 가드용. 개명 후에도 이 머리를 가진 청크가 남아 있다면, 옛 이름을 그대로 쓰는 **다른**
        문서(동명)가 살아 있다는 뜻이라 그 이름을 인용하던 참조를 함부로 새 이름으로 돌리면 안 된다."""
        if not self.client.collections.exists(collection):
            return False
        from weaviate.classes.query import Filter
        coll = self.client.collections.get(collection)
        res = coll.query.fetch_objects(
            filters=Filter.by_property("provision_id").like(f"{head}*"),
            limit=50, return_properties=["provision_id"])
        for obj in res.objects:
            pid = obj.properties.get("provision_id") or ""
            if pid.startswith(head) and (len(pid) == len(head) or pid[len(head)] in "#:"):
                return True
        return False

    def delete_renamed_self_chunks(self, law_id: str, old_head: str, collection: str) -> int:
        """개명된 문서 **자신**의 옛 이름 청크를 지운다(law_id 일치 + provision_id 가 old_head 로
        시작). 개명이 새 버전(MST) 없이 오면 version_uid 가 그대로라 stale 정리
        (delete_stale_law_chunks)가 못 잡고, 새 이름 청크(새 provision_id → 새 UUID)와 옛 이름
        청크가 중복 공존한다 — 그 잔존분 정리용. 새 이름이 옛 이름으로 시작하는 경우
        (예: 갑법 → 갑법개정법)를 like 가 못 가르므로 경계는 파이썬에서 판정하고 id 로 지운다."""
        if not self.client.collections.exists(collection):
            return 0
        from weaviate.classes.query import Filter
        coll = self.client.collections.get(collection)
        removed = 0
        while True:
            res = coll.query.fetch_objects(
                filters=(Filter.by_property("law_id").equal(law_id)
                         & Filter.by_property("provision_id").like(f"{old_head}*")),
                limit=500, return_properties=["provision_id"])
            doomed = []
            for obj in res.objects:
                pid = obj.properties.get("provision_id") or ""
                if pid.startswith(old_head) and (len(pid) == len(old_head)
                                                 or pid[len(old_head)] in "#:"):
                    doomed.append(obj.uuid)
            if not doomed:
                return removed
            result = coll.data.delete_many(where=Filter.by_id().contains_any(doomed))
            got = getattr(result, "successful", None) or 0
            removed += int(got)
            if not got:                                    # 방어 — 진전 없으면 무한루프 방지
                return removed

    def has_chunks(self, law_id: str, collection: str) -> bool:
        """이 law_id 로 이미 적재된 청크가 하나라도 있는지 — 이어받기(--skip-existing) 판단용.

        성공한 법은 청크가 다 있고, 실패한 법은 0개(임베딩 배치 실패 시 문서 통째로 미저장)라
        '청크 있으면 skip' 만으로 성공분은 건너뛰고 실패·미처리분만 다시 처리된다."""
        from weaviate.classes.query import Filter
        coll = self.client.collections.get(collection)
        result = coll.query.fetch_objects(
            filters=Filter.by_property("law_id").equal(law_id), limit=1, return_properties=["law_id"])
        return len(result.objects) > 0

    def missing_ids(self, uuids: List[str], collection: str) -> set:
        """주어진 객체 UUID 중 컬렉션에 **없는** 것들을 돌려준다 — skip-existing 의
        '완전한 문서' 판정용. upsert 와 같은 ID 일괄조회(500개 배치)라 문서당 왕복이 1~2회다."""
        from weaviate.classes.query import Filter
        coll = self.client.collections.get(collection)
        ids = [str(u) for u in uuids]
        missing = set(ids)
        for i in range(0, len(ids), 500):
            group = ids[i:i + 500]
            result = coll.query.fetch_objects(
                filters=Filter.by_id().contains_any(group), limit=len(group),
                return_properties=[])
            for obj in result.objects:
                missing.discard(str(obj.uuid))
        return missing

    def search(self, vector: List[float], collection: str, limit: int = 5):
        """수동 확인용 near_vector 검색을 수행한다."""
        from weaviate.classes.query import MetadataQuery
        collection = self.client.collections.get(collection)
        return collection.query.near_vector(
            near_vector=vector, limit=limit, return_metadata=MetadataQuery(distance=True)
        ).objects

    def count(self, collection: str) -> int:
        """지정한 컬렉션의 전체 객체 수를 반환한다."""
        result = self.client.collections.get(collection).aggregate.over_all(total_count=True)
        return result.total_count or 0
