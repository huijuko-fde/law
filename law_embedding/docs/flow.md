# 흐름도

## 1. 초기/전체 색인

```mermaid
flowchart TD
    START([index 실행]) --> SOURCE{source}
    SOURCE -->|law| LAW_REPO[LAW]
    SOURCE -->|admrul| ADMRUL_REPO[ADMRUL]
    SOURCE -->|schlpub| SCHLPUB_REPO[SCHLPUBRUL]
    SOURCE -->|all / both| ALL[세 repo 순회]
    SOURCE -.->|DB dump| DBTODO[미구현<br/>DB payload/file loader 필요]

    LAW_REPO --> JSON[JSON 파일 검색]
    ADMRUL_REPO --> JSON
    SCHLPUB_REPO --> JSON
    ALL --> JSON

    JSON --> SKIP{skip-existing<br/>기대 청크가 전부 있음?}
    SKIP -->|있음| NEXT[다음 문서]
    SKIP -->|없음| LOAD[payload 로드]

    LOAD --> IMG{행정규칙류 그림 표시?<br/>전처리 on}
    IMG -->|예| OCR[이미지/지능형 전처리<br/>DOC_PARSER_IMAGE_ENDPOINT_PATH]
    IMG -->|아니오| MAP
    OCR --> MAP[조문/부칙/별표/개정문 매핑]

    MAP --> FILES{파일 전용 단위?}
    FILES -->|is_file_only 별표/별지| PRE[첨부용 전처리<br/>DOC_PARSER_ENDPOINT_PATH]
    FILES -->|문서 전체 파일| PRE
    FILES -->|없음| BUF
    PRE --> FILE_CHUNK[FILE 청크 생성]
    FILE_CHUNK --> BUF[버퍼에 적재<br/>INDEX_EMBED_FLUSH]
    BUF --> NEXT
    BUF -.->|버퍼가 차면 flush| CACHE{벡터 캐시 적중?}
    CACHE -->|예| UPSERT
    CACHE -->|아니오| EMBED[search_text 임베딩]
    EMBED --> UPSERT[Weaviate upsert]
    UPSERT --> CLEAN[문서별 고아 청크 정리<br/>FILE·JSON]
```

이 흐름은 초기/전체 색인이다. data repo의 JSON을 하나씩 열어 조문·부칙·별표로 청킹하고, 파일에만 본문이 있는 단위만 전처리를 거쳐 FILE 청크로 만든 뒤, 임베딩 대상 텍스트(`search_text` — 저장용 본문 `content`에 법령명·조문번호 같은 문맥을 붙인 값)를 벡터로 만들어 Weaviate에 upsert한다.

임베딩·upsert는 문서마다 즉시 하지 않고 문서 경계를 넘어 버퍼에 모았다가 한 번에 흘린다. 실패 집계와 고아 청크 정리는 flush 시점에 문서 단위로 한다. `--vector-cache`를 주면 안 바뀐 청크는 임베딩을 건너뛰고 옛 벡터를 재사용한다([indexer.md](indexer.md) §7).

## 2. package 증분 소비

```mermaid
flowchart TD
    START([consume-folder 또는 index-changeset]) --> HEADER{첫 줄 package_header?}
    HEADER -->|아니오| LEGACY[1세대 changeset 처리]
    HEADER -->|예| SOURCE[source 결정]

    SOURCE --> CLAIM[package claim<br/>.processing]
    CLAIM --> BUFFER[law_id별 record 버퍼링]
    BUFFER --> ROUTE[문서별 컬렉션 라우팅<br/>doc_target school/pi/public 은 schlpub]

    ROUTE --> DOC{document record}
    DOC -->|있음| MAP[문서 payload 매핑]
    DOC -->|없음| EXTRA

    ROUTE --> EXTRA{추가 record}
    EXTRA -->|preprocessed_chunk| PRECHUNK[FILE 청크 생성]
    EXTRA -->|file content_b64| DECODE[base64 decode]
    EXTRA -->|file transfer_name| INBOX[inbox 파일 조회]
    EXTRA -->|pending_attachment| PENDING[보류 기록]
    EXTRA -->|delete| DELETE[삭제 표시]
    EXTRA -->|rename| RENAME[개명 통지 - 참조 meta·mirror 폴더 갱신]

    DECODE --> PARSER[첨부용 전처리]
    INBOX --> PARSER
    PARSER --> FILECHUNK[FILE 청크 생성]

    MAP --> MIRROR{PACKAGE_MIRROR_REPO}
    MIRROR -->|true| REPO[내부망 data repo 갱신]
    MIRROR -->|false| UPSERT
    REPO --> UPSERT
    PRECHUNK --> UPSERT
    FILECHUNK --> UPSERT

    UPSERT[새 청크 upsert] --> STALE[전부 성공 시에만<br/>옛 version_uid 청크 삭제]
    DELETE --> DELETEVDB[law_id 청크 삭제]
    STALE --> RESULT{package 결과}
    DELETEVDB --> RESULT
    PENDING --> RESULT
    RESULT -->|성공| DONE[processed 또는 delete]
    RESULT -->|일시 오류| RETRY[원래 이름으로 복구<br/>다음 sweep 재시도]
    RESULT -->|영구 오류/재시도 초과| FAILED[failed 격리]
```

증분 소비는 package(JSONL) 하나를 원자적으로 잡아 처리한다. `claim`은 package 파일 이름을 `.processing`으로 바꿔 "내가 처리 중"임을 표시하는 것(다른 소비자나 재실행과 겹치지 않게)이고, `sweep`은 폴더를 반복해서 훑어 아직 처리 안 된 package를 집는 순회를 뜻한다. 문서를 새로 upsert한 뒤에는 같은 문서의 옛 `version_uid`(=문서+버전 식별키) 청크를 지워 한 버전만 남긴다 — 단, 새 청크가 **전부** 들어갔을 때만 지운다.

## 3. 수집기와 임베딩기 역할

```mermaid
flowchart LR
    subgraph COLLECTOR[temporal_law]
        API[법제처 API]
        PAYLOAD[payload 생성]
        PACKAGE[JSONL package 생성]
    end

    subgraph EMBEDDING[law_embedding]
        CONSUME[package 소비]
        CHUNK[청킹/매핑]
        PRE[선택: 내부 전처리]
        EMB[임베딩]
        VDB[(Weaviate)]
    end

    API --> PAYLOAD
    PAYLOAD --> PACKAGE
    PACKAGE --> CONSUME
    CONSUME --> CHUNK
    CONSUME --> PRE
    PRE --> CHUNK
    CHUNK --> EMB
    EMB --> VDB
```

수집기(`temporal_law`)가 payload와 package를 만들고, 임베딩기(`law_embedding`)는 그 package를 소비해 청킹·(필요 시) 전처리·임베딩만 한다. package를 무엇으로 채울지 고르는 규칙과 `manifest`(수집기가 어떤 문서·버전을 내보냈는지 적어 두는 목록) 관리는 수집기 몫이고, 자세한 계약은 [package.md](package.md)와 수집기 문서를 본다.

## 4. 읽는 법

- 데이터 레포 3개(LAW·ADMRUL·SCHLPUBRUL)와 컬렉션 3개(Legal·Admrul·SchlPubRul ProvisionIndex)는 1:1이다. 초기/전체 색인은 `--source`가 곧 레포이자 컬렉션이고, 증분은 package source가 `law`/`admrul` 둘뿐이라 소비자가 문서 단위로 schlpub을 갈라낸다.
- 초기/전체 색인은 현재 data repo의 JSON 파일을 순회한다. DB dump만 받아 바로 색인하는 경로는 아직 없고, 구현하려면 DB payload loader와 file_asset 원본 파일 위치 규칙이 필요하다.
- 전처리기는 두 갈래다. 별표/별지/서식/원문 파일은 첨부용 endpoint를 쓰고, 행정규칙 본문 `[그림]`은 이미지/지능형 endpoint를 쓴다. 법령 본문 `[그림]`은 OCR하지 않는다.
- package 소비는 `law_id`별로 record를 모은 뒤 Weaviate에 반영한다. 일부 파일 전처리 실패는 pending으로 남길 수 있지만, Weaviate upsert 같은 문서 단위 오류는 package 재시도/failed 흐름을 탄다.
- `law_embedding`은 package를 만드는 쪽이 아니다. package 생성 규칙과 manifest 모드는 `temporal_law` 문서를 보고, 여기서는 도착한 package를 어떻게 소비하는지만 다룬다.
