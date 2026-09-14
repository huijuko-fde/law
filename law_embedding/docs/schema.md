# Weaviate 스키마

컬렉션은 3개 — 데이터 레포당 하나 — 이고 전부 같은 스키마를 사용한다(클래스 이름만 다름).

| source | 기본 컬렉션 |
| --- | --- |
| `law` | `LegalProvisionIndex` |
| `admrul` | `AdmrulProvisionIndex` |
| `schlpub` | `SchlPubRulProvisionIndex` |

컬렉션은 self-provided vector 방식이다. Weaviate 내부 vectorizer는 쓰지 않고, `law_embedding`이 만든 벡터를 직접 넣는다. 벡터 인덱스는 HNSW · cosine이다.

속성은 **35개**이고 그중 `indexFilterable=true`가 **18개**, `indexSearchable=true`가 `search_text` 하나다. inverted index는 BM25 `b=0.75` · `k1=1.2`이고 `indexNullState`·`indexPropertyLength`·`indexTimestamps`는 모두 끈다.

정의의 유일한 출처는 코드 상수 `weaviate_store.SCHEMA`이고, 납품용 정의서(루트 `genos_vdb_schema_law.json` · `genos_vdb_schema_admrul.json` · `genos_vdb_schema_schlpubrul.json`)도 거기서 뽑는다. 아래 표는 그 내용을 읽기 쉽게 옮긴 것이므로, 값이 어긋나면 코드와 JSON 쪽이 맞다.

## 1. 필드 구성

스키마는 네 층으로 나뉜다.

| 구분 | 설명 |
| --- | --- |
| 필터 필드 (18) | relation(조문이 다른 조문·법을 가리키는 참조) 탐색, 삭제, 날짜/문서 필터에 자주 쓰는 값. `indexFilterable=true` · BM25는 걸지 않는다. 텍스트 필터는 전부 `field` 토크나이저 = 정확일치용 |
| 키워드 검색 필드 (1) | `search_text` 하나가 하이브리드 검색의 BM25 절반을 담당한다 |
| 표시 필드 (15) | 답변, 디버깅, 출처 표시에 필요한 값. 검색·필터 인덱스를 모두 끈다 |
| `meta` (1) | 자주 필터하지 않는 부가 정보를 JSON 문자열 하나로 묶는다. where 필터 대상이 아니다 |

본문·제목·장은 이미 `search_text`에 합성돼 들어가므로 개별 속성에 BM25를 중복으로 걸지 않는다.

## 2. 필터 필드

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `chunk_id` | text | 청크 식별자. UUID 생성의 기준 |
| `provision_id` | text | 조문/부칙/별표/첨부가 속한 조문 식별자 |
| `law_id` | text | 문서 식별자. 삭제와 재색인의 기준 |
| `version_uid` | text | 문서의 버전 식별자. **payload 최상위 `version_uid` = `{law_id}:{MST}:{시행일}`** (실측 `014808:285793:20260511`). 행정규칙류 payload에는 이 필드가 없어 색인기가 `{adm_uid}:{개정일}`(개정일 없으면 시행일)로 합성한다 — 실측 `admrul:29464:20260630`. 개정되면 값이 바뀐다. 주의: 수집기 **DB**의 `document_version.version_uid`는 `{doc_uid}:{MST}:{시행일}`로 접두가 다르다(같은 이름, 다른 값) — 색인에 들어오는 건 payload 쪽이다 |
| `file_id` | text | FILE 청크 묶음 식별자 |
| `unit_type` | text | `ARTICLE`, `ADDENDUM`, `APPENDIX`, `AMENDMENT`, `FILE` |
| `is_current` | boolean | 현행 여부 |
| `is_future` | boolean | 시행예정 여부(`is_current`와 함께 현행/미래를 가른다) |
| `enforcement_date` | date | 시행일. 시점 질의("2024년 기준")를 위해 **범위 필터**(`indexRangeFilters`)를 켠 유일한 속성 |
| `reference_ids` | text[] | relation 대상 `provision_id` 목록 |
| `parent_provision_id` | text | 상위 조문 식별자 |
| `ministry` | text | 소관부처. 원본 `basic_info.소관부처`에서 부처명만 뽑은 값 |
| `domain` | text | `law` / `admrul` / `schlpub` — 데이터 레포·컬렉션과 1:1 (법령/행정규칙/학칙공단) |
| `law_name` | text | 문서명. "이 법 안에서만 검색" 용 정확일치 필터 |
| `law_type` | text | 법률/대통령령/부령/고시 등 |
| `source_type` | text | `JSON` 본문 / `FILE` 첨부 |
| `revision_type` | text | 제정/일부개정/전부개정/타법개정 |
| `git_path` | text | 원본 JSON 경로(데이터 레포 상대경로 — 연혁 조회·같은 문서 청크 모으기. `field` 토크나이저 정확일치) |

## 3. 키워드 검색 필드

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `search_text` | text | 임베딩에 사용한 텍스트이자 BM25 대상. 문서명+종류+장 경로+조번호+제목+`content`를 줄바꿈으로 합친 값(합성 규칙은 [indexer.md](indexer.md) §5) |

토크나이저는 `kagome_kr`(한국어 형태소)다. Weaviate에 해당 모듈이 없어 컬렉션 생성이 거부되면 `trigram`으로 한 번 더 시도한다 — 검색 품질은 떨어지지만 하이브리드 검색 자체는 동작한다. 기본값은 `SEARCH_TEXT_TOKENIZATION`으로 바꿀 수 있다.

약칭·별칭은 `search_text`에 넣지 않는다(벡터 재사용 때문 — [indexer.md](indexer.md) §5). 약칭은 `law_abbr`에 **표시용으로만** 남는다. `law_abbr`은 현재 filterable·searchable을 모두 끈 표시 필드라 약칭으로 BM25 검색이나 exact 필터를 걸 수는 없다 — 약칭 질의는 벡터 유사도에 기댄다.

## 4. 표시 필드

검색·필터 인덱스를 모두 끈 값들이다. 키워드 검색은 `search_text`가 전부 커버한다.

| 필드 | 설명 |
| --- | --- |
| `content` | 실제 청크 본문 |
| `law_abbr` | 약칭 |
| `unit_no`, `unit_title`, `chapter` | 조문/부칙/별표 번호와 제목, 장(가장 가까운 한 단계) |
| `source_url` | 원문 URL |
| `mst` | 법령 마스터번호(payload 최상위 `mst`) — 법령 식별 보조값 |
| `adm_uid` | 행정규칙 저장 키(예: `admrul:67925`) — 행정규칙 식별 보조값(법령엔 없음) |
| `file_name`, `file_url`, `page_no` | FILE 청크 출처 |
| `chunk_index`, `chunk_count` | 한 단위 안 청크 순서 |
| `promulgation_date`, `revision_date` | 공포일/개정일 |

## 5. meta 내부 키

`meta`는 JSON 문자열이다. 자주 필터하지 않지만 잃으면 안 되는 정보를 보존한다.

주요 키:

| 키 | 설명 |
| --- | --- |
| `source_repository`, `source_file_path`, `source_relative_path` | Git/파일 출처 추적 |
| `git_commit`, `content_hash`, `file_hash` | 재현성과 변경 추적 |
| `relation_refs` | relation 상세 정보. `reference_ids`는 필터용, `relation_refs`는 표시/디버깅용. 참조마다 `reference_id`·`target_mst`·`relation_type`·`source_clause`·`line_text`·`link_text`·`target_article_title`·`target_url`·`resolve_method`를 담고, payload에 있으면 경로 키(`target_git_path`·`target_source_repository` = 수집기 enrich가 확정한 문서 경로, `target_git_file_path` = 대상이 별표/별지 파일일 때 레포 기준 파일 전체 경로, `target_repo`·`target_git_path_guess` = 추정 폴백)와 `target_doc_target`·`target_doc_kind`·`source_admrul_seq`, 그리고 동명 대상 정보(`target_candidates` 후보 수, `target_candidate_paths` 후보 전원 목록 — 대표를 못 정한 동률 관계는 확정 경로 없이 목록만 온다)와 `target_missing_unit`(대상 문서엔 그 조문/별표가 없음 표시)을 함께 싣는다. 확정 경로 우선, 없으면 추정으로 폴백해 relation 하나로 대상 원문 파일까지 닿는다. 추정이 빗나가면 그 경로에 파일이 없을 뿐 다른 문서를 가리키지는 않는다 |
| `file_kind` | `attachment` 또는 `document_file` |
| `start_page`, `end_page`, `chunk_bboxes`, `media_files` | 전처리기 페이지/좌표/미디어 메타 |
| `image_urls` | 조문 본문 인라인 이미지 URL 목록(`article.images[]`) |
| `guardrail_categories` | 전처리기 가드레일 분류 결과 |
| `n_char`, `n_word`, `n_line`, `parser_reg_date` | 전처리기 통계 |
| `scheduled` | 조문별 미래 시행 예고 |
| `ordinance_delegations` | 조례 위임 요약 `[{link_text, count}]` — 법령 조문 전용. 조문당 수백 건이라 개별 조례는 나열하지 않는다(전체 목록은 원문 JSON) |
| `future_enforcement_dates` | 문서 단위 시행예정일 목록 |
| `repealed`, `repeal_scheduled`, `repealed_at` | 폐지 상태 |
| `promulgation_no` | 공포번호/발령번호 |

## 6. payload에서 무엇이 어떻게 들어가나?

Weaviate 객체는 payload 전체를 그대로 저장하지 않는다. 검색에 필요한 단위로 청킹한 뒤, 자주 쓰는 값은 top-level 필드로 올리고 나머지는 `meta`에 보존한다.

| payload 영역 | 저장 방식 |
| --- | --- |
| `body.articles[]` | 조문별 JSON 청크. 긴 조문은 여러 청크로 나뉘지만 같은 `provision_id`를 유지한다. |
| `addenda[]` | 부칙 청크. `unit_type=ADDENDUM`. 여러 부칙 항목을 **문서당 하나**로 합친다(`provision_id={문서}#ADDENDUM` — [indexer.md](indexer.md) §4) |
| `appendices[]` 텍스트 | 별표/별지/서식 텍스트 청크. `unit_type=APPENDIX` |
| `appendices[].is_file_only=true` | 원문 파일을 전처리한 FILE 청크. 원본 파일 메타는 `file_name`, `file_url`, `file_id`, `meta`에 저장. (`is_file_only` 자체는 저장 필드가 아니다 — `source_type=FILE`로 대체된다) |
| `amendment_text` + `revision_reason` | 개정문·개정이유를 합쳐 만든 청크 1개. `unit_type=AMENDMENT`. `provision_id`는 payload의 `amendment_provision_id`를 쓰고, 없으면 `{접두}:{공백 제거한 문서명}#AMENDMENT`로 만든다(접두는 `law`/`admrul`/`SchlPubRul`). meta가 아니라 검색 대상 청크다 |
| `attachments[]` | 문서 본문이 파일에만 있는 경우에만 전처리 대상. 본문 텍스트가 있으면 중복 방지를 위해 자동 전처리하지 않음 |
| relation | 대상 `provision_id` 목록은 `reference_ids`, 상세 relation 객체는 `meta.relation_refs` |
| 시행예정/미래법 | `is_future`(top-level bool)·`enforcement_date`(top-level date)로 구분해 같은 컬렉션에 저장한다. 둘 다 meta가 아니라 top-level 필드다 |

payload에서 모든 원본 필드를 top-level로 올리지는 않는다. Weaviate 필드는 검색/필터/출처 표시에 필요한 값 중심이고, 원본 payload 전체가 필요하면 data repo JSON 또는 package mirror repo를 봐야 한다.

## 7. relation 조회 방식

relation 탐색은 path 문자열을 추측하지 않는다.

1. 검색 hit의 `reference_ids`를 읽는다.
2. 각 값은 대상 조문의 `provision_id`다.
3. Weaviate에서 `provision_id == reference_id`로 exact fetch한다.
4. fetch된 대상 hit의 `git_path`, `content`, `unit_no`, `unit_title`을 사용한다.

`reference_id` 문자열만으로 파일 경로를 조립하면 안 된다. `provision_id` 접두(`law`/`admrul`/`SchlPubRul`/`ordin`)는 어느 **레포**인지까지만 알려주고, 레포 안 폴더는 문서명·문서종·동명충돌 여부에 따라 달라진다. 대상 레코드를 다시 조회해 그 레코드의 `git_path`(레포 상대경로)를 쓰는 것이 맞다.

첨부 실제 파일 경로가 필요하면 `meta.source_relative_path`를 우선 사용한다.

## 8. 모델 변경 주의

현재 모델은 `BAAI/bge-m3`(**1024차원**)이다. 예전에는 `Snowflake/snowflake-arctic-embed-m-v2.0`(768차원)이었다.

한 컬렉션에는 같은 차원의 벡터만 들어갈 수 있다(self-provided vector는 첫 적재 차원으로 잠긴다 — 적재 직전에 색인기가 한 번 더 대조한다). 그래서 모델만 바꾸고 재색인하면 `컬렉션 벡터 차원 불일치: 저장됨 768, 입력 1024`로 **전건이 거부된다**(조용히 섞이지는 않는다). `EMBEDDING_MODEL` 또는 벡터 차원이 바뀌면:

```bash
uv run python -m law_indexer create-collection --source law --recreate
uv run python -m law_indexer index --source law
```

처럼 컬렉션을 다시 만들고 재색인한다.
