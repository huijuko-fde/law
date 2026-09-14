# 색인기

이 문서는 `law_indexer index`가 data repo의 JSON을 읽어 Weaviate 객체로 만드는 흐름을 설명한다.

`temporal_law`의 수집기가 payload를 만든다면, `law_embedding`의 색인기는 그 payload를 검색 가능한 청크로 바꾼다.

## 1. 색인 대상

색인기는 세 source(`law`/`admrul`/`schlpub`)를 처리한다 — 데이터 레포 3개와 1:1 이다.

| source | 입력 repo | 기본 컬렉션 |
| --- | --- | --- |
| `law` | `LAW` | `LegalProvisionIndex` |
| `admrul` | `ADMRUL` | `AdmrulProvisionIndex` |
| `schlpub` | `SCHLPUBRUL` | `SchlPubRulProvisionIndex` |

**레포 3개 = 컬렉션 3개, source 1:1.** 학칙·공단정관·공공기관(`school`/`pi`/`public`)은
`--source schlpub` 로 자기 컬렉션에 들어간다(`SCHLPUB_REPO_PATH`를 비우면 `ADMRUL` 레포 폴백).
`--input`/`--paths-file`로 대상을 콕 집으면 그 경로를 품고 있는 레포가 기준 레포가 된다
(`git_path`가 레포 상대경로로 기록되므로 이 판정이 맞아야 한다). 다른 source 레포 밑 경로를
지정하면 엉뚱한 컬렉션에 넣지 않도록 즉시 에러다.

`--source all`(하위호환 `both`)을 주면 law → admrul → schlpub 순으로 처리한다. Weaviate 인증
key가 컬렉션별로 다른 환경을 고려해 source마다 Weaviate 연결을 따로 연다.

**증분 소비 라우팅** — 수집기 package/change-set 의 source 는 `law`/`admrul` 둘뿐이고 학칙공단
문서는 admrul 에 `doc_target`(school/pi/public)으로 섞여 온다. 소비자가 **문서 단위**로
schlpub 컬렉션에 갈라 넣는다(delete 는 law_id 접두 `school:`/`pi:`/`public:` 으로 판별).

## 2. data repo 구조

```text
data/
├── LAW/
│   └── {법령명}/{법률|시행령|시행규칙}/{문서명}.json
├── ADMRUL/
│   └── {문서명}/{문서명}.json                    ← 고시·훈령·예규
└── SCHLPUBRUL/
    └── {문서명}/{문서명}.json                    ← 학칙·공단정관·공공기관
```

레포가 문서종별로 갈려 있어 레포 안에 문서종 래퍼 폴더가 없다. 같은 레포에서 문서명이 겹칠
때만 `{문서명}/{doc_target}/` 또는 `{문서명}/{doc_target}_{문서ID}/` 로 한 단계 내려간다.

문서 JSON 옆에는 파일 단위를 담은 하위 폴더가 함께 놓인다. 색인기가 참조하는 폴더는 별표류 폴더(`별표`·`별지`·`서식`·`부록`·`별첨`·`별도`·`양식`·`붙임`·`기타`·`부속서`·`부표`·`도식`·`부도`)와 `첨부파일`·`원문`·`본문이미지`이고(파일-only 별표나 원문 파일이 여기 있다), 특히 행정규칙 조문 안 `[그림]`은 `본문이미지/{article_no}_{순번}.gif|png|jpg` 파일로 들어 있다(순번 순서가 본문 등장 순서와 1:1로 맞는다 — §6에서 이 규칙으로 이미지를 찾는다).

색인 대상:

- `.json`
- `appendices[].is_file_only=true`인 별표/별지/서식 파일
- 본문 텍스트가 없고 `attachments[]`만 있는 행정규칙류 원문 파일
- 행정규칙 조문 본문 안 `[그림]`에 대응하는 본문이미지

색인하지 않는 것:

- `.md`
- `_manifest.json`
- 본문 텍스트가 이미 있는 문서의 최상위 `attachments[]`
- 삭제 안내문뿐인 별표/별지/서식

초기 적재는 DB를 읽지 않는다. `LAW_REPO_PATH`, `ADMRUL_REPO_PATH`, `SCHLPUB_REPO_PATH` 아래의 JSON 파일을 실제 파일시스템에서 순회한다. 그래서 초기 적재 전에 data repo를 clone/pull하거나, 수집기 Git export 결과를 같은 구조로 배치해야 한다.

DB dump만 받아 초기 적재하는 경로는 아직 구현되어 있지 않다. 이 방식을 쓰려면 다음이 추가로 필요하다(`document_version`·`file_asset`은 수집기 `temporal_law`의 Postgres 테이블 이름이다).

- DB에서 `document_version.payload`를 읽어 source별 JSON payload로 공급하는 loader
- `file_asset`이 가리키는 원본 파일을 어디서 읽을지에 대한 규칙
- 첨부파일 원본이 DB 밖에 있을 경우 MinIO/NFS/파일 경로 연결
- DB 기반 초기 색인과 package 증분 소비의 중복 삭제/upsert 기준 정리

## 3. 실행 흐름

```text
JSON 파일 순회
  -> payload 읽기
  -> 기대 청크가 이미 전부 있으면 skip (--skip-existing, §7)
  -> 행정규칙 [그림] 본문이미지 OCR 보강
  -> 조문/부칙/별표/개정문 청크 생성
  -> 파일 전용 별표/별지/서식 전처리
  -> 문서 전체가 파일뿐인 문서 전처리
  -> 버퍼에 적재(문서 경계를 넘어 모음)
  ── flush ──
  -> search_text 임베딩 (벡터 캐시 적중분은 건너뜀 — §7)
  -> Weaviate upsert
  -> 문서별 고아 청크 정리(FILE·JSON)
```

임베딩은 문서마다 즉시 호출하지 않고 **문서 경계를 넘어 버퍼에 모았다가 한 번에** 흘린다(`INDEX_EMBED_FLUSH`, 기본 256청크). 원격 임베딩은 호출당 고정비가 커서 문서당 소배치(평균 20청크 안팎) 호출이 전체 색인 시간을 지배했기 때문이다. 성공/실패 집계와 고아 청크 정리는 flush 시점에 **문서 단위로** 한다.

실패 단위:

- JSON 파일을 읽지 못하면 그 파일만 실패로 기록하고 다음 파일을 처리한다.
- 조문/부칙/별표 mapper가 실패하면 그 문서만 실패로 기록한다.
- 특정 첨부파일 전처리가 실패해도 본문 조문 청크는 계속 적재한다.
- 임베딩/upsert가 실패한 문서는 기대 청크가 다 차지 않으므로 `--skip-existing`에 걸리지 않고 다음 실행에서 다시 시도된다. 실패 문서 경로는 `--failed-out` 파일로도 남아 그대로 `--paths-file` 재처리 입력이 된다.
- 청크가 하나라도 실패한 문서는 고아 청크 정리를 건너뛴다(부분 실패 시 옛 청크 보존).

## 4. 청크 단위

| payload 위치 | unit_type | 설명 |
| --- | --- | --- |
| `body.articles[]` | `ARTICLE` | 조문 |
| `addenda[]` | `ADDENDUM` | 부칙 — **문서당 하나로 접는다**(아래) |
| `appendices[]` | `APPENDIX` | 텍스트 별표/별지/서식 |
| `amendment_text`, `revision_reason` | `AMENDMENT` | 개정문·개정이유 |
| 전처리된 첨부 파일 | `APPENDIX` 또는 `FILE` | 파일-only 별표/별지/서식 또는 문서 전체 원문 |

긴 본문은 `CHUNK_MAX_CHARS`(기본 **2,500자**)를 넘을 때만 나눈다. 이 값은 예전 모델
(arctic-embed-l-v2.0-ko)의 학습 길이 1,300토큰이 상한이라 정해진 것이었지만, 현재 모델
`BAAI/bge-m3`는 8,192토큰까지 받으므로 지금은 **모델 제약이 아니라 검색 단위 선택**이다 —
청크가 커질수록 조문 하나를 정확히 집어내는 힘이 떨어진다. 바꾸면 청크 경계가 달라져
`chunk_id`가 전부 바뀌므로 전량 재색인이 필요하다. 자르는 순서는 **항(①②…) → 호(1. 2. …) → 문단(빈 줄) → 줄**로, 위 경계에서 나뉘면 아래로 내려가지 않는다. 항·호는 마커 '앞'에서 자르므로 조각을 그대로 이어붙이면 원문이 복원된다.
쪼개진 조각도 같은 `provision_id`를 유지하므로 검색 후 조문 단위 복원이 가능하다.

**부칙은 문서당 청크 하나다.** payload는 과거 부칙을 여러 항목으로 주지만, 각 항목 앞에 `[부칙 N / 시행일·공포일·공포번호]` 머리말을 붙여 한 덩이로 합친 뒤 `provision_id` 하나로 넣는다(payload의 `addendum_provision_id`, 없으면 `{접두}:{공백 제거한 문서명}#ADDENDUM`). 개별 부칙마다 id를 만들면 재수집 때 순서·본문 미세 차이로 id가 흔들리고, relation도 문서 부칙 전체를 가리키는 경우가 많아서다. 합친 본문이 최대 길이를 넘으면 위 규칙대로 여러 청크로 나뉜다.

sample 30문서 실측(청크 6,364개, 2026-08-24 기준). `ADDENDUM`이 문서 수보다 많은 것은 합친 부칙 본문이 최대 길이를 넘어 여러 조각으로 나뉘기 때문이다:

| unit_type | 청크 수 | 2,500자 초과로 남은 것 |
| --- | ---: | ---: |
| `ARTICLE` | 3,752 | 42 (1.1%) |
| `APPENDIX` | 2,469 | 0 |
| `ADDENDUM` | 119 | 3 (2.5%) |
| `AMENDMENT` | 24 | 0 |

`content` 길이는 중앙값 623자·평균 1,099자다. 즉 **대부분의 조문은 쪼개지지 않고 1청크로 들어간다**
(여러 조각으로 나뉜 단위는 347개). 어떤 경계로도 안 나뉘는 조문(표가 통째인 조문 등)은 자르지 않고
그대로 둔다 — 위 '초과' 열이 그것이고, 억지로 자르면 표가 반토막 나기 때문이다.

여기서 잠깐 색인기가 다루는 식별자들을 정리하면 이렇다. 수집기는 문서(법령·고시) 하나에 `law_id`(행정규칙류는 `doc_id`)를 붙이고, 도메인·target까지 합친 문서 통합키가 `doc_uid`(`{doc_domain}:{doc_target}:{doc_id}`)다. 같은 문서라도 개정될 때마다 버전이 생기므로, 버전 하나를 가리키는 키가 `version_uid`다. 색인에 들어오는 값은 **payload 최상위의 `{law_id}:{MST}:{시행일}`**(`MST`는 법제처가 버전마다 매기는 번호)이고, 행정규칙류 payload에는 이 필드가 없어 색인기가 `{adm_uid}:{개정일}`(개정일이 없으면 시행일)로 합성한다. (수집기 DB의 `document_version.version_uid`는 `{doc_uid}:{MST}:{시행일}`로 접두가 하나 더 붙는 별개 값이다 — 이름만 같다.) payload 안에서 조문·부칙·별표 한 단위를 가리키는 값이 `provision_id`이고, 첨부는 `file_id`로 가른다. 색인기는 이 값들을 재료로 청크마다 `chunk_id`를 결정적으로 계산한다(`stable_id`) — 즉 `version_uid` + `provision_id`(또는 `file_id`) + 단위 정보 → `chunk_id` → 같은 청크는 언제 다시 돌려도 같은 UUID로 교체된다(§7 멱등성).

| 식별자 | 무엇을 가리키나 | 만드는 쪽 |
| --- | --- | --- |
| `law_id` / `doc_id` | 문서(법·고시) 하나 | 수집기 payload |
| `doc_uid` | `{doc_domain}:{doc_target}:{doc_id}` 문서 통합키 | 수집기 payload |
| `version_uid` | `{law_id}:{MST}:{시행일}` 문서의 한 버전(행정규칙은 `{adm_uid}:{개정일}`로 합성) | 수집기 payload |
| `provision_id` | 조문·부칙·별표 한 단위 | 수집기 payload |
| `file_id` | 첨부파일 하나 | 색인기(`stable_id`). package 생산자가 `file_id`를 실어 보내면 그 값을 그대로 쓴다 |
| `chunk_id` | Weaviate 객체(청크) 하나 | 색인기(`stable_id`) |

## 5. 임베딩 대상

Weaviate에 저장하는 본문은 `content`이고, 임베딩 대상은 `search_text`다.

`search_text`는 다음 값을 줄바꿈으로 합친다.

```text
law_name
law_type
chapter_path   (조문 전용. 없으면 chapter)
unit_no
unit_title
content
```

장 문맥은 **장 경로 전체**(`chapter_path`, 실측 예 `제4장 등록절차 > 제1절 통칙`)를 쓴다. 저장 필드 `chapter`는 종전대로 가장 가까운 한 단계(`제1절 통칙`)만 담지만, 그 값만 넣으면 어느 장 밑인지가 사라지기 때문에 검색 텍스트에만 경로를 넣는다.

**약칭·별칭(`law_abbr`, 영문명·한자명)은 `search_text`에 넣지 않는다.** 벡터가 `search_text`로 만들어지는 탓에 별칭을 섞으면 같은 조문이라도 별칭 유무에 따라 텍스트가 달라져 이전에 계산해 둔 벡터를 재사용할 수 없다(실측 재사용률 43%까지 하락). 약칭은 `law_abbr` 속성에 표시용으로 남는다 — 다만 그 속성은 검색·필터 인덱스가 꺼져 있어 약칭으로 BM25 검색을 걸 수는 없다([schema.md](schema.md) §3). relation 대상 해석은 payload의 `name_aliases`를 쓰므로 여기와 무관하다.

첨부파일 청크는 파일 자체 텍스트만 넣지 않고 문서명, 법령 종류, 별표/원문 문맥, 페이지 정보를 함께 넣는다. 파일 안에 문서명이 없더라도 검색에서 문맥을 잃지 않기 위해서다.

## 6. 첨부파일 전처리

색인기는 전처리기를 직접 구현하지 않고 `DOC_PARSER_*` 설정으로 외부 전처리 API를 호출한다.

전처리는 payload 안에 이미 텍스트가 있는 조문을 다시 파싱하려는 기능이 아니다. 파일에만 본문이 있거나, 행정규칙 조문 일부가 이미지로 빠져 있는 경우에 검색 가능한 텍스트를 보강하는 단계다.

전처리하는 대상:

| 대상 | 조건 |
| --- | --- |
| 별표/별지/서식 파일 | `appendices[].is_file_only=true` |
| 행정규칙 원문 파일 | 본문에 의미 있는 텍스트가 없고 `attachments[]`가 있는 경우 |
| 행정규칙류 본문이미지 | 조문 content에 `[그림]`이 있고 `본문이미지/{article_no}_{n}.gif|png|jpg`가 있는 경우. admrul과 schlpub(학칙·공단정관·공공기관) 모두 해당 |

전처리하지 않는 대상:

| 대상 | 이유 |
| --- | --- |
| 법령/행정규칙 조문 본문 | payload의 `body.articles[]` 텍스트를 바로 청킹한다. |
| 부칙, 개정문, 텍스트 별표/별지 | payload에 구조화 텍스트가 있으므로 외부 파서가 필요 없다. |
| 본문 텍스트가 있는 문서의 최상위 `attachments[]` | 본문과 중복될 수 있어 자동 전처리하지 않는다. |
| 삭제 안내문뿐인 별표/별지/서식 | 검색 가치가 낮고 실제 본문이 아니므로 제외한다. |

법령류 본문이미지는 전처리하지 않는다. 법령류의 본문 이미지는 대체로 수식/표 조각이고 조문 본문에 텍스트가 이미 들어 있는 경우가 많아 중복과 비용이 크기 때문이다. 행정규칙류에서도 짧은 변이 `MIN_ARTICLE_IMAGE_SIDE`(기본 16px)보다 작은 이미지는 문서가 아니라 인라인 수식/기호 글리프로 보고 호출 자체를 건너뛴다 — 그 자리는 `[그림]` 마커가 그대로 남고 성공·실패 어느 쪽으로도 세지 않는다.

`DOC_PARSER_BASE_URL`이 비었거나 `INDEX_PREPROCESS_FILES=false`면 전처리 대상 파일은 **건너뛴 수(`attachments_skipped`)로 집계**되고 오류로 남지 않는다. `[그림]` OCR 단계도 통째로 생략되어 마커가 원문 그대로 남는다. 이미 텍스트가 있는 조문/부칙 청크는 계속 적재된다.

이 스위치는 "벌크는 전처리 끄고 한 번, 대상 문서만 켜서 다시" 라는 2단계 운영을 위한 것이다. 대상 문서 목록은 `preprocess-targets`가 원본 JSON에서 결정적으로 뽑는다(`[그림]` 마커 · `is_file_only` 별표 · 문서 전체가 파일인 문서). Weaviate를 조회하지 않으므로 적재 전·후 어느 시점에 돌려도 같은 결과가 나오고, 결과 파일은 그대로 `index --paths-file` 입력이 된다.

전처리기는 두 종류의 endpoint를 나눠 쓸 수 있다.

| endpoint | env | 사용 대상 |
| --- | --- | --- |
| 첨부용 전처리기 | `DOC_PARSER_ENDPOINT_PATH` | 별표/별지/서식 파일, 문서 전체 원문 파일 |
| 이미지/지능형 전처리기 | `DOC_PARSER_IMAGE_ENDPOINT_PATH` | 행정규칙류 조문 안 `[그림]`에 대응하는 본문이미지 |

문서 파일(hwp/hwpx/pdf/docx 등)은 항상 첨부용 endpoint로 보낸다. 이미지만 `DOC_PARSER_IMAGE_ENDPOINT_PATH`(적재용/intelligent)를 사용한다. 이 값을 비우면 이미지도 첨부용 endpoint로 간다. 판별은 확장자로 한다(`.gif`·`.png`·`.jpg`·`.jpeg`·`.bmp`·`.webp`·`.tif`·`.tiff`). 전처리기가 `.gif`를 받지 못해 호출 직전에 PNG로 변환한다.

이미지 OCR은 장당 21~42초(실측)라 그림이 많은 문서에서 순차 호출이 병목이 된다. `DOC_PARSER_IMAGE_CONCURRENCY`(기본 1 = 순차)를 올리면 **같은 문서 안의 이미지들**을 동시에 호출한다. 호출은 서로 독립이고 결과는 자리 index로 되쓰므로 `[그림]` 치환 순서는 그대로 유지된다.

재시도는 일시적 네트워크 오류(timeout·연결 실패·5xx·429)에만 한다. 결정적 오류(`code != 0`, 4xx, 빈 결과, 응답 파싱 실패)는 같은 입력이면 결과가 같으므로 즉시 실패로 낸다.

전처리 호출 방식:

| 방식 | 설정 | 설명 |
| --- | --- | --- |
| path | `DOC_PARSER_UPLOAD=false` | 전처리기에게 파일 경로를 넘긴다. 색인기와 전처리기가 같은 파일을 볼 수 있어야 한다. |
| multipart | `DOC_PARSER_UPLOAD=true` | 파일 bytes를 업로드한다. 전처리기가 별도 컨테이너/API로 떠 있으면 이 방식이 안전하다. |

전처리기 원시 응답 예시는 다음 형태다.

```json
{
  "code": 0,
  "errMsg": "success",
  "data": [
    {
      "text": "전처리된 본문",
      "n_char": 328,
      "n_word": 50,
      "n_line": 43,
      "i_page": 1,
      "e_page": 1,
      "i_chunk_on_doc": 0,
      "n_chunk_of_doc": 1,
      "reg_date": "2026-08-04T18:09:01Z",
      "chunk_bboxes": null,
      "media_files": null,
      "guardrail_categories": null
    }
  ]
}
```

색인기는 이 응답을 바로 저장하지 않고 다음처럼 정규화한다.

| 전처리기 응답 | FILE 청크 필드 |
| --- | --- |
| `text` | `content` |
| `i_page` | `page_no`, `start_page` |
| `e_page` | `end_page` |
| `i_chunk_on_doc` | `chunk_index` |
| `reg_date` | `parser_reg_date` |
| `chunk_bboxes`, `media_files`, `guardrail_categories` | 같은 이름의 메타 필드 |
| `n_char`, `n_word`, `n_line` | 같은 이름의 통계 필드 |

이렇게 만든 FILE 청크도 조문 청크와 같은 컬렉션에 들어간다. 별도 파일 전용 컬렉션은 없다.

## 7. 멱등성과 이어받기

`chunk_id`는 payload 식별자와 단위 정보를 기반으로 결정적으로 만든다. 같은 청크는 같은 UUID로 들어가므로 재실행해도 같은 객체를 교체한다.

`--skip-existing`은 **기대 청크가 전부 들어 있는 문서만** 건너뛴다. 기대 `chunk_id`는 매핑이 결정적으로 만드는 값이라(`version_uid`·단위·순번 기반, 본문 내용에 의존하지 않음) 임베딩 없이 산출할 수 있고, 그것을 UUID 일괄조회로 실제 컬렉션과 대조한다.

- 기대 청크가 전부 있는 문서: skip
- 실패했거나 **부분만 들어간** 문서: 다시 처리

예전 정의(청크가 하나라도 있으면 skip)는 업서트 부분 실패로 반쯤 들어간 문서가 영영 메워지지 않았다(실측: 민법 제1~394조 400청크 누락).

세부 규칙:

- JSON 청크만 대조한다. FILE 청크는 전처리를 돌려야 개수가 나오므로 판정에 넣지 않는다.
- '기대 ⊆ 실제'면 skip이다. 초과분(2단계 OCR로 분할이 늘어난 청크, FILE 청크)은 건드리지 않는다 — 재색인하면 OCR 결과를 `[그림]` 마커본으로 되돌릴 위험이 있다.
- 매핑 실패이거나 JSON 청크가 0인 문서(파일 전용 문서)는 skip하지 않고 본 처리로 넘긴다.
- `--files-only`에는 적용하지 않는다. 본문 청크가 이미 있는 채로 첨부만 채우는 실행이기 때문이다.

고아 청크 정리는 두 갈래다. 파일 청크는 전처리 결과 청크 수가 줄어들 수 있으므로 같은 `file_id`의 이전 청크 중 이번 결과에 없는 것을 지운다. JSON 청크도 OCR 재색인으로 조문 본문이 바뀌면 분할 개수가 달라질 수 있어, 같은 `law_id`+`version_uid`의 `source_type=JSON` 청크 중 이번 결과에 없는 것을 지운다.

**벡터 재사용 캐시.** `--vector-cache <parquet>`(`scripts/migrate_collection export` 산출물 — uuid·vector·props 컬럼)를 주면 같은 청크 UUID이고 `search_text`가 그대로일 때 임베딩 호출 없이 옛 벡터를 재사용한다. `search_text`가 달라졌으면(개정·매핑 규칙 변화) 캐시 미스로 새로 임베딩한다. 속성과 `meta`는 캐시 적중과 무관하게 항상 새로 쓰므로 relation 확정 같은 meta 갱신은 그대로 반영된다. parquet는 배치 단위로 스트리밍해 읽는다(전량을 파이썬 리스트로 펼치면 수십만 청크에서 메모리가 터진다).

## 8. 실행 예시

명령·옵션·ENV의 정본은 [README](../README.md) §4·§5다. 여기서는 위 설명에 대응하는 최소 예시만 둔다.

```bash
uv run python -m law_indexer create-collection --source all
uv run python -m law_indexer index --source law --skip-existing
uv run python -m law_indexer index --source admrul --skip-existing
uv run python -m law_indexer index --source schlpub --skip-existing
```

특정 폴더만:

```bash
uv run python -m law_indexer index --source law --input ../data/LAW/근로기준법 --recursive
```

2단계 전처리(§6):

```bash
uv run python -m law_indexer preprocess-targets --source admrul --out preprocess_targets.txt
uv run python -m law_indexer index --source admrul --paths-file preprocess_targets.txt
```
