import json
from pathlib import Path

import pytest

from law_indexer.mapper import (
    MappingError, build_attachment_provisions, load_admrul_json, load_law_json,
    map_admrul_data, map_attachment_data, map_law_data, object_uuid,
)


FIXTURE = Path(__file__).parent / "fixtures" / "law.json"
ADMRUL_FIXTURE = Path(__file__).parent / "fixtures" / "admrul.json"


def mapped():
    return load_law_json(FIXTURE)


def admrul_mapped():
    return load_admrul_json(ADMRUL_FIXTURE)


def test_article_addendum_appendix_mapping():
    article, addendum, appendix, amendment = mapped()
    assert [article.unit_type, addendum.unit_type, appendix.unit_type, amendment.unit_type] == [
        "ARTICLE", "ADDENDUM", "APPENDIX", "AMENDMENT"]
    assert article.provision_id == "law:표본법#JO0001"
    assert article.reference_ids == ["law:다른법#JO0002"]
    assert appendix.provision_id == "law:표본법#BYL0001"
    assert appendix.reference_ids == []


def test_json_defaults_content_and_dates():
    article = mapped()[0]
    assert article.content == "제1조(목적) 원문을 보존한다."
    assert article.chunk_index == 0 and article.chunk_count == 1
    assert article.source_type == "JSON" and article.file_id is None
    assert article.promulgation_date == "2024-01-01T00:00:00Z"
    assert article.enforcement_date == "2024-02-01T00:00:00Z"


def test_missing_addendum_id_and_chunk_id_are_deterministic():
    first, second = mapped(), mapped()
    assert first[1].provision_id == second[1].provision_id
    assert first[1].provision_id == "law:표본법#ADDENDUM"
    assert first[1].chunk_id == second[1].chunk_id
    assert object_uuid(first[1].chunk_id) == object_uuid(second[1].chunk_id)


def test_bad_json_and_shape(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    with pytest.raises(MappingError, match="JSON 읽기 실패"):
        load_law_json(bad)
    with pytest.raises(MappingError, match="body"):
        map_law_data({}, bad)


def test_attachment_chunks_and_general_attachment(tmp_path):
    data = {"version_uid": "v1", "file_name": "일반.pdf", "chunks": [
        {"page_no": 1, "chunk_index": 0, "content": "첫 청크"},
        {"page_no": 2, "chunk_index": 1, "content": "둘째 청크"}]}
    objects = map_attachment_data(data, tmp_path / "chunks.json")
    assert len(objects) == 2
    assert objects[0].unit_type == "FILE"
    assert objects[0].provision_id is None and objects[0].parent_provision_id is None
    assert objects[0].source_type == "FILE" and objects[0].chunk_count == 2
    assert objects[0].file_id == objects[1].file_id


def test_attachment_keeps_appendix_provision_id(tmp_path):
    data = {"version_uid": "v1", "provision_id": "law:x#BYL0001", "file_name": "별표.hwp",
            "chunks": [{"content": "표 내용"}]}
    obj = map_attachment_data(data, tmp_path / "chunks.json")[0]
    assert obj.unit_type == "APPENDIX" and obj.provision_id == "law:x#BYL0001"


def test_oversized_article_splits_into_multiple_chunks_with_repeated_context():
    """§5-1: 조문이 설정된 최대 크기를 넘으면 문단 경계로 나누되, 각 조각의 search_text 에
    문서명·조번호·제목이 반복돼야 한다(문맥이 빠지면 검색 품질이 떨어진다)."""
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    long_para = "긴 문단 내용입니다. " * 300  # 4000자 훌쩍 넘게
    data["body"]["articles"][0]["content"] = f"{long_para}\n\n{long_para}\n\n{long_para}"
    objects = map_law_data(data, FIXTURE)
    article_chunks = [o for o in objects if o.unit_type == "ARTICLE"]
    assert len(article_chunks) > 1
    for i, chunk in enumerate(article_chunks):
        assert chunk.chunk_index == i
        assert chunk.chunk_count == len(article_chunks)
        assert "표본법" in chunk.search_text  # law_name 반복
        assert chunk.unit_no in chunk.search_text  # 조번호 반복
    # 청크 id 는 같은 문서를 다시 매핑해도 동일해야 한다(재적재 중복 방지)
    objects2 = map_law_data(data, FIXTURE)
    assert [o.chunk_id for o in objects] == [o.chunk_id for o in objects2]


def test_small_article_stays_single_chunk_unchanged():
    """4000자 이하 조문은 예전과 동일하게 1청크·chunk_index=0 이어야 한다(회귀 방지)."""
    objects = mapped()
    article = objects[0]
    assert article.chunk_index == 0 and article.chunk_count == 1


def test_law_chunks_have_collection_type_law():
    for obj in mapped():
        assert obj.collection_type == "law"


def test_admrul_mapping_works_without_version_uid():
    """실제 샘플(admrul/school/pi/public 전수 확인) 처럼 version_uid 가 없어도 adm_uid 로 처리돼야 한다."""
    objects = admrul_mapped()
    assert [o.unit_type for o in objects] == ["ARTICLE", "ADDENDUM", "APPENDIX"]
    for obj in objects:
        assert obj.collection_type == "admrul"
        assert obj.version_uid is not None and obj.version_uid.startswith("admrul:sample-admrul:")


def test_admrul_missing_adm_uid_still_raises():
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data.pop("adm_uid")
    with pytest.raises(MappingError, match="adm_uid"):
        map_admrul_data(data, ADMRUL_FIXTURE)


def test_admrul_appendix_is_file_only_true_is_skipped():
    """is_file_only=True 별표/별지는 본문 JSON 매핑에서 빠진다 — Doc Parser 경로가 대신 채운다."""
    objects = admrul_mapped()
    appendix_provision_ids = {o.provision_id for o in objects if o.unit_type == "APPENDIX"}
    assert appendix_provision_ids == {"admrul:표본행정규칙#BYL0001"}  # is_file_only=false 별표만 남음
    assert "admrul:표본행정규칙#BJ0002" not in appendix_provision_ids  # is_file_only=true 별지는 스킵됨


def test_admrul_appendix_deleted_content_is_skipped():
    """STEP2: content 가 "[라벨] 삭제" 안내문뿐인 별표/별지/서식은 실질 내용이 없어 매핑에서 빠진다."""
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data["appendices"].append({
        "provision_id": "admrul:표본행정규칙#BYL9999", "no": "9999", "kind": "별지",
        "title": "삭제됨", "content": "[별지 제9999호서식] <삭제>", "is_file_only": False,
    })
    objects = map_admrul_data(data, ADMRUL_FIXTURE)
    appendix_provision_ids = {o.provision_id for o in objects if o.unit_type == "APPENDIX"}
    assert "admrul:표본행정규칙#BYL9999" not in appendix_provision_ids


def test_admrul_reindex_is_idempotent():
    """같은 admrul JSON을 두 번 매핑해도 같은 chunk_id(=같은 UUID)가 나와야 재적재가 중복되지 않는다."""
    first, second = admrul_mapped(), admrul_mapped()
    for a, b in zip(first, second):
        assert a.chunk_id == b.chunk_id
        assert object_uuid(a.chunk_id) == object_uuid(b.chunk_id)


def test_build_attachment_provisions_preserves_doc_parser_metadata(tmp_path):
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    chunks = [
        {"content": "첫 페이지 추출 텍스트", "chunk_index": 0, "page_no": 1, "start_page": 1, "end_page": 1,
         "chunk_bboxes": [{"x": 0, "y": 0}], "media_files": ["img1.png"], "guardrail_categories": None},
    ]
    objects = build_attachment_provisions(
        data, tmp_path / "표본.json", "admrul", "https://github.com/genonai/admrul_data.git", chunks,
        provision_id="admrul:표본행정규칙#BJ0002", unit_type="APPENDIX",
        file_name="별지2_신청서.hwp", file_url="https://example.test/f2",
        source_file_path=str(tmp_path / "별지" / "별지2_신청서.hwp"),
    )
    assert len(objects) == 1
    obj = objects[0]
    assert obj.collection_type == "admrul" and obj.source_type == "FILE"
    assert obj.is_file_only is True
    assert obj.source_repository == "https://github.com/genonai/admrul_data.git"
    assert obj.start_page == 1 and obj.end_page == 1
    assert obj.chunk_bboxes == '[{"x": 0, "y": 0}]'  # 문자열이 아니면 JSON 직렬화해 보존
    assert obj.media_files == '["img1.png"]'
    assert obj.guardrail_categories is None



def test_properties_schema_shape():
    """properties() 가 weaviate-schema.md 대로 나오는지 — domain 리네임·embedding 제외·meta 번들·ministry."""
    article = mapped()[0]
    props = article.properties()
    # collection_type -> domain, embedding 필드는 저장 안 함
    assert props["domain"] == "law"
    assert "collection_type" not in props
    assert "embedding_model" not in props and "embedding_dimension" not in props
    # ministry(소관부처.content)는 top-level 필터
    assert props["ministry"] == "표본부"
    # meta 는 JSON 문자열 1개, 개별 키는 top-level 로 새지 않는다
    assert "relation_refs" not in props and "n_char" not in props and "amendment_text" not in props
    meta = json.loads(props["meta"])
    assert meta["relation_refs"] == [
        {"reference_id": "law:다른법#JO0002", "target_mst": "200", "relation_type": "citation",
         "source_clause": "제1조", "line_text": "다른법 제2조를 준용한다", "link_text": "다른법",
         "target_article_title": "정의", "target_url": "https://www.law.go.kr/다른법",
         "resolve_method": "pop"}
    ]
    # 개정문·개정이유는 meta 가 아니라 별도 AMENDMENT 청크로 나간다(첫 청크 meta 엔 없음)
    assert "amendment_text" not in meta and "revision_reason" not in meta
    assert meta["promulgation_no"] == "제100호"
    assert meta["future_enforcement_dates"] == ["2025-01-01"]


def test_doc_level_meta_only_on_first_chunk():
    """promulgation_no 등 문서레벨 값은 첫 청크에만 — 뒤 청크(부칙/별표)엔 복제 안 함."""
    objs = mapped()
    first = json.loads(objs[0].properties().get("meta", "{}"))
    later = json.loads(objs[1].properties().get("meta", "{}"))
    assert first.get("promulgation_no") == "제100호"
    assert "promulgation_no" not in later


def test_amendment_chunk_is_searchable_not_meta():
    """개정문·개정이유는 별도 AMENDMENT 청크로 — content/search_text 에 담겨 검색되고, meta 로 안 뺀다."""
    amend = [o for o in mapped() if o.unit_type == "AMENDMENT"]
    assert len(amend) == 1
    a = amend[0]
    assert "[개정문]" in a.content and "[개정이유]" in a.content   # 둘 다 content 에 실림
    assert a.content in a.search_text                            # search_text 포함 → 임베딩·검색 대상
    assert a.provision_id == "law:표본법#AMENDMENT"
    props = a.properties()
    assert "amendment_text" not in props and "revision_reason" not in props
    assert "amendment_text" not in json.loads(props.get("meta", "{}"))


def test_admrul_amendment_fallback_uses_doc_target_prefix():
    """행정규칙 개정문 fallback ID 는 law: 로 오염되지 않고 doc_target prefix 를 쓴다."""
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data["amendment_text"] = "행정처분 가중처분 기준을 정비한다."
    data["revision_reason"] = "반복 위반 기준을 명확히 하기 위함."
    data.pop("amendment_provision_id", None)

    amend = [o for o in map_admrul_data(data, ADMRUL_FIXTURE) if o.unit_type == "AMENDMENT"]

    assert len(amend) == 1
    assert amend[0].collection_type == "admrul"
    assert amend[0].provision_id == "admrul:표본행정규칙#AMENDMENT"


def test_admrul_addendum_and_appendix_fallback_ids_use_doc_target_prefix():
    """행정규칙 부칙은 문서당 #ADDENDUM 하나, 별표 fallback 은 doc_target prefix 를 쓴다."""
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data["addenda"][0].pop("provision_id", None)
    data["appendices"][0].pop("provision_id", None)

    objects = map_admrul_data(data, ADMRUL_FIXTURE)
    addendum = next(o for o in objects if o.unit_type == "ADDENDUM")
    appendix = next(o for o in objects if o.unit_type == "APPENDIX")

    assert addendum.provision_id == "admrul:표본행정규칙#ADDENDUM"
    assert appendix.provision_id.startswith("admrul:표본행정규칙#APPENDIX-")


def test_schlpub_targets_get_schlpub_domain_and_schlpubrul_prefix():
    """학칙/공단/공공기관은 domain=schlpub(컬렉션과 1:1), fallback ID prefix 는 SchlPubRul."""
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data["doc_target"] = "school"
    data["law_id"] = "school-1"
    data["adm_uid"] = "school:school-1"
    data["amendment_text"] = "학칙 개정문"

    amend = [o for o in map_admrul_data(data, ADMRUL_FIXTURE) if o.unit_type == "AMENDMENT"]

    assert amend[0].collection_type == "schlpub"
    assert amend[0].provision_id == "SchlPubRul:표본행정규칙#AMENDMENT"


def test_fallback_id_prefix_accepts_korean_doc_target_and_unescapes_name():
    """fallback ID 도 수집기 compact 규칙처럼 한글 target 과 HTML entity 를 정규화한다."""
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data["doc_target"] = "학칙공단"
    data["law_name"] = "수목원&#8228;정원 관리 규정"
    data["law_id"] = "school-entity"
    data["adm_uid"] = "school:school-entity"
    data["amendment_text"] = "개정문"
    data.pop("amendment_provision_id", None)

    amend = next(o for o in map_admrul_data(data, ADMRUL_FIXTURE) if o.unit_type == "AMENDMENT")

    assert amend.provision_id == "SchlPubRul:수목원정원관리규정#AMENDMENT"


def test_addenda_are_combined_into_one_document_level_chunk():
    """부칙은 개별 hash ID 로 나누지 않고 문서당 #ADDENDUM 청크 하나로 합친다."""
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    data["addenda"].append({"promulgation_date": "20250101", "promulgation_no": "2", "content": "두 번째 부칙"})

    addenda = [o for o in map_law_data(data, FIXTURE) if o.unit_type == "ADDENDUM"]

    assert len(addenda) == 1
    assert addenda[0].provision_id == "law:표본법#ADDENDUM"
    assert "[부칙 1 / 공포 20240101 제1호]" in addenda[0].content
    assert "[부칙 2 / 공포 20250101 제2호]" in addenda[0].content
    assert "부칙 원문" in addenda[0].content and "두 번째 부칙" in addenda[0].content


def test_future_addendum_heading_keeps_future_metadata():
    """시행예정 부칙은 합쳐진 ADDENDUM 청크 안에서도 미래 메타가 읽혀야 한다."""
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    data["addenda"] = [{
        "promulgation_date": "20251230",
        "promulgation_no": "21243",
        "enforcement_date": "20260701",
        "is_future": True,
        "content": "미래 부칙",
    }]

    addendum = next(o for o in map_law_data(data, FIXTURE) if o.unit_type == "ADDENDUM")

    assert "[부칙 1 / 시행예정 시행 20260701 공포 20251230 제21243호]" in addendum.content


def test_relation_refs_preserve_target_path_when_present():
    """relation meta 는 reference_id 외에 target git 경로가 있으면 함께 보존한다."""
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    rel = data["body"]["articles"][0]["relations"][0]
    rel["source_admrul_seq"] = "2100000247732"
    rel["target_git_path"] = "다른법/법률/다른법.json"
    rel["target_source_repository"] = "law_data"

    article = map_law_data(data, FIXTURE)[0]
    meta = json.loads(article.properties()["meta"])

    assert meta["relation_refs"][0]["source_admrul_seq"] == "2100000247732"
    assert meta["relation_refs"][0]["target_git_path"] == "다른법/법률/다른법.json"
    assert meta["relation_refs"][0]["target_source_repository"] == "law_data"


def test_relation_refs_carry_predicted_path_without_enrich():
    """enrich 를 안 돌려도 원문 경로가 실린다 — 수집기가 모든 관계에 붙이는 **추정** 경로.

    확정 경로(target_git_path)는 전체 인덱스를 가진 enrich 단계에서만 생긴다. 예전엔 확정 키만
    읽어서, enrich 없이 색인하면 relation 에 경로가 통째로 빠졌다(실측 12,893건 전부).
    """
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    rel = data["body"]["articles"][0]["relations"][0]
    rel["target_repo"] = "LAW"
    rel["target_git_path_guess"] = "다른법/법률/다른법.json"

    meta = json.loads(map_law_data(data, FIXTURE)[0].properties()["meta"])
    ref = meta["relation_refs"][0]
    assert ref["target_repo"] == "LAW"
    assert ref["target_git_path_guess"] == "다른법/법률/다른법.json"


def test_search_text_excludes_aliases_and_keeps_chapter_path():
    """검색 텍스트에 약칭·별칭은 **넣지 않고**, 장 경로 전체는 넣는다.

    벡터는 search_text 로 만들어진다. 별칭을 섞으면 같은 조문이라도 별칭 유무에 따라 텍스트가
    달라져 이전에 계산해 둔 벡터를 재사용할 수 없다(실측: 재수집분 재사용률이 43%까지 떨어졌다).
    별칭 키워드 검색은 `law_abbr` 속성이 담당하고, relation 대상 해석은 payload 의
    `name_aliases` 를 쓰므로 여기서 빠져도 영향이 없다.

    장은 nearest 만 넣으면 "제2절 해고" 가 어느 장 밑인지 사라져 경로 전체를 넣는다.
    chapter 필드값 자체는 nearest 를 유지한다.
    """
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    data["law_abbr"] = "표본법약칭"
    data["name_aliases"] = ["표본법약칭", "SAMPLE ACT"]
    data["body"]["articles"][0]["chapter"] = "제2절 해고"
    data["body"]["articles"][0]["chapter_path"] = "제1장 총칙 > 제2절 해고"

    article = map_law_data(data, FIXTURE)[0]
    lines = article.search_text.splitlines()
    assert lines[0] == "표본법"
    assert "표본법약칭" not in lines                            # 약칭은 search_text 에 없다
    assert "SAMPLE ACT" not in lines
    assert article.law_abbr == "표본법약칭"                      # 속성으로는 남는다
    assert "제1장 총칙 > 제2절 해고" in lines
    assert article.chapter == "제2절 해고"                      # 필드는 nearest 유지


def test_search_text_falls_back_to_nearest_chapter():
    """chapter_path 가 없는 옛 payload 는 종전대로 nearest 장을 쓴다."""
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    data["body"]["articles"][0]["chapter"] = "제1장 총칙"
    data["body"]["articles"][0].pop("chapter_path", None)
    assert "제1장 총칙" in map_law_data(data, FIXTURE)[0].search_text.splitlines()


def test_file_kind_on_attachment(tmp_path):
    """FILE 청크에 file_kind 가 붙는다(문서전체=document_file / 별표=attachment)."""
    from law_indexer.mapper import build_attachment_provisions
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    chunks = [{"content": "원문 텍스트", "chunk_index": 0, "page_no": 1}]
    doc = build_attachment_provisions(
        data, tmp_path / "표본.json", "law", None, chunks,
        provision_id=None, unit_type="FILE", file_name="문서전체.hwp", file_url=None,
        source_file_path=str(tmp_path / "문서전체.hwp"),
    )
    assert json.loads(doc[0].properties()["meta"])["file_kind"] == "document_file"
    byl = build_attachment_provisions(
        data, tmp_path / "표본.json", "law", None, chunks,
        provision_id="law:표본법#BYL0001", unit_type="APPENDIX", file_name="별표1.hwp", file_url=None,
        source_file_path=str(tmp_path / "별표1.hwp"),
        appendix={"no": "0001", "branch": "00", "kind": "별표", "title": "기준표"},
    )
    assert json.loads(byl[0].properties()["meta"])["file_kind"] == "attachment"



def test_split_prose_respects_hang_ho_boundaries():
    """조문이 max_chars 넘으면 항(①②)·호(1.) 경계에서 나뉘고 마커 중간은 안 잘린다."""
    from law_indexer.chunking import split_prose
    content = ("제1조(목적) 이 법은 다음 사항을 규정한다.\n"
               "① 첫째 항의 내용. " + "가나다라마바사 " * 40 + "\n"
               "② 둘째 항의 내용. " + "아자차카타파하 " * 40 + "\n"
               "③ 셋째 항의 내용. " + "라마바사아자차 " * 40)
    parts = split_prose(content, max_chars=400)
    assert len(parts) > 1
    # 각 조각은 항 마커로 시작하거나(경계 분할) 첫 조각(제1조) — 마커 중간 절단 없음
    for p in parts[1:]:
        assert p.lstrip()[0] in "①②③", f"항 경계 아님: {p[:20]!r}"


def test_split_table_like_keeps_rows_and_header():
    """표는 행 단위로만 나뉘고 각 조각에 앞 2줄(제목/헤더)이 반복된다."""
    from law_indexer.chunking import split_table_like
    header = "■ 별표 제1호 요율표\n구분 | 금액"
    rows = "\n".join(f"항목{i} | {i*1000}원" for i in range(60))
    parts = split_table_like(header + "\n" + rows, max_chars=300)
    assert len(parts) > 1
    for p in parts:
        assert p.startswith("■ 별표 제1호 요율표"), "헤더 반복 안 됨"
        # 각 줄이 '|' 있는 온전한 행(셀 중간 안 잘림)
        for line in p.splitlines()[2:]:
            assert "|" in line


def test_ordinance_delegations_summarized_into_article_meta(tmp_path):
    """조례 위임(payload 최상위)은 조문별 요약 [{link_text,count}] 로 meta 에 실린다.

    개별 조례(수백 개)는 나열하지 않는다 — B안(요약만). 위임 없는 조문·후속 청크에는 없다."""
    import json as _json
    payload = {
        "law_id": "L1", "version_uid": "L1:1:20240101", "law_name": "표본법", "law_type": "법률",
        "body": {"articles": [
            {"article_no": "제7조", "article_title": "구역", "content": "조례로 정한다",
             "provision_id": "law:표본법#JO0007"},
            {"article_no": "제8조", "article_title": "기타", "content": "본문",
             "provision_id": "law:표본법#JO0008"},
        ]},
        "ordinance_delegations": [
            {"delegation_type": "위임자치법규", "source_article_no": "제7조",
             "link_text": "그 지방자치단체의 조례로", "ordinance_count": 638,
             "ordinances": [{"target_law_name": "논산시 조례", "target_mst": "1"}]},
        ],
    }
    path = tmp_path / "표본법.json"
    path.write_text(_json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    objs = map_law_data(payload, path)
    a7 = [o for o in objs if o.unit_no == "제7조"][0]
    meta = _json.loads(a7.properties()["meta"])
    assert meta["ordinance_delegations"] == [{"link_text": "그 지방자치단체의 조례로", "count": 638}]
    # 개별 조례 목록은 싣지 않는다
    assert "논산시" not in a7.properties()["meta"]
    a8 = [o for o in objs if o.unit_no == "제8조"][0]
    assert "ordinance_delegations" not in _json.loads(a8.properties().get("meta") or "{}")


def test_domain_is_three_way_per_repo(tmp_path):
    """domain 은 컬렉션·레포와 1:1(law/admrul/schlpub) — 학칙공단이 admrul 로 접히지 않는다."""
    import json as _json
    base = {"law_id": "A1", "adm_uid": "school:1", "law_name": "표본학칙", "law_type": "학칙",
            "revision_date": "20240101", "doc_target": "school",
            "body": {"articles": [{"article_no": "제1조", "article_title": "목적",
                                    "content": "본문", "provision_id": "SchlPubRul:표본학칙#JO0001"}]}}
    path = tmp_path / "표본학칙.json"
    path.write_text(_json.dumps(base, ensure_ascii=False), encoding="utf-8")
    objs = map_admrul_data(base, path)
    assert objs[0].properties()["domain"] == "schlpub"
    base2 = dict(base, doc_target="admrul", adm_uid="admrul:2", law_name="표본고시", law_type="고시")
    base2["body"] = {"articles": [{"article_no": "제1조", "article_title": "목적",
                                    "content": "본문", "provision_id": "admrul:표본고시#JO0001"}]}
    objs2 = map_admrul_data(base2, path)
    assert objs2[0].properties()["domain"] == "admrul"


def test_relation_refs_carry_same_name_candidates():
    """동명 후보 정보(target_candidates/target_candidate_paths)가 meta.relation_refs 로 실려야 한다.

    enrich 가 payload 에 남겨도 여기서 안 실으면 검색 소비자가 동명 상황을 알 수 없다(실측 누락)."""
    from law_indexer.mapper import _relation_refs

    unit = {"relations": [{
        "reference_id": "admrul:동명고시:admrul_200#JO0001",
        "relation_type": "citation",
        "target_git_path": "동명고시/admrul_200/동명고시.json",
        "target_candidates": 2,
        "target_candidate_paths": [
            {"git_path": "동명고시/admrul_100/동명고시.json", "source_repository": "ADMRUL",
             "provision_id": "admrul:동명고시:admrul_100#JO0001", "is_current": True, "enforcement_date": "20200101"},
            {"git_path": "동명고시/admrul_200/동명고시.json", "source_repository": "ADMRUL",
             "provision_id": "admrul:동명고시:admrul_200#JO0001", "is_current": True, "enforcement_date": "20250101"},
        ],
    }]}
    refs = _relation_refs(unit)
    assert refs and refs[0]["target_candidates"] == 2
    assert len(refs[0]["target_candidate_paths"]) == 2
    assert refs[0]["target_candidate_paths"][0]["provision_id"].endswith("admrul_100#JO0001")
