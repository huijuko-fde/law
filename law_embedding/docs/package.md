# 증분 package 소비

이 문서는 `law_indexer index-changeset`과 `law_indexer consume-folder`가 수집기 package를 어떻게 소비하는지 설명한다.

package는 `temporal_law`와 `law_embedding` 사이의 전달 계약이다. 분리망에서는 임베딩기가 수집기 DB나 DMZ 파일 경로를 직접 보지 않고 package만 본다.

`temporal_law` 문서가 “package를 어떻게 만드는가”를 설명한다면, 이 문서는 “도착한 package를 어떻게 소비해 Weaviate와 내부망 repo에 반영하는가”를 설명한다. 생산자 설정 전체를 다시 설명하지 않고 소비자 동작에 집중한다.

## 1. 진입점

package 하나를 직접 소비:

```bash
uv run python -m law_indexer index-changeset --input /mnt/handoff/packages/law-20260814-010000-3f9ac1.jsonl
```

폴더에 도착한 package를 순차 소비:

```bash
uv run python -m law_indexer consume-folder --dir /mnt/handoff/packages
```

`consume-folder`는 운영용에 가깝다. package를 `.processing`으로 rename해 원자적으로 claim하고, 성공/실패/재시도를 분리한다(§6).

package 파일 이름은 `{source}-{생성시각}-{무작위 6자리}.jsonl`이고 그 값이 곧 `package_header.package_id`다. 무작위 접미는 같은 초에 만들어진 package끼리 파일명이 겹쳐 서로 덮어쓰는 것을 막으려는 것이므로, 순서나 내용은 파일명이 아니라 header로 판단한다.

## 2. package record

| record_type | 처리 |
| --- | --- |
| `package_header` | source와 package_id를 읽는다. source로 기본 컬렉션을 정하고, 문서별 라우팅은 §3에서 다시 가른다. |
| `document` | payload를 mapper로 보내 조문/부칙/별표/개정문 청크를 만든다. `op="delete"`면 `delete`와 같이 취급한다. |
| `normalized_chunk` | 생산자가 미리 만든 정규화 청크를 청크 객체로 만든다. |
| `preprocessed_chunk` | 수집기 쪽 전처리 결과를 FILE 청크로 만든다. |
| `file` | 원본 파일을 내부망 전처리기로 보낸 뒤 FILE 청크로 만든다. |
| `pending_attachment` | 처리 보류 정보를 기록한다. 색인 객체는 만들지 않는다. |
| `delete` | 해당 `law_id` 청크를 삭제한다. |
| `rename` | 문서 이름이 바뀌었다는 수집기 통지(`old_name`/`new_name`/`law_id`/`doc_target`, 있으면 `old_dir`/`new_dir`). 문서 flush가 끝난 뒤 ① 개명 문서 자신의 옛 이름 청크 잔존분 삭제 ② 옛 이름 문서가 아직 살아 있으면(동명) 참조 패치를 건너뛰고 `rename_ambiguous`로 보고 ③ 세 컬렉션에서 그 문서를 참조하던 청크의 `reference_ids`·meta를 새 이름으로 patch(벡터 보존, 인용 원문 `line_text`/`link_text`는 보존) ④ mirror의 옛 문서 폴더 정리(새 폴더 있으면 삭제, 없으면 이동). 멱등이라 재발송을 다시 받아도 안전하다. 배경은 엄브렐라 `teach/rename_mirror_issue.md`. |
| `package_footer` | record_count를 검증한다(rename 포함). |

`mapper`(`map_law_data`/`map_admrul_data`)는 payload(JSON)를 색인 청크(`LegalProvision`)로 바꾸는 매핑이다. `document` payload가 이 mapper를 거쳐 조문·부칙·별표·개정문 청크가 된다.

`document` record는 payload 말고도 `git_path`(데이터 레포 상대경로 — mirror 위치 기준이자 청크의 `git_path`)와 `commit_message`(mirror push 시 수집기와 같은 커밋 메시지)를 함께 실어 온다.

payload 안 `relations[]`에는 수집기 enrich가 붙인 대상 확정 정보가 그대로 들어 있다(`target_git_path`·`target_source_repository`·`target_git_file_path`, 추정 폴백 `target_repo`·`target_git_path_guess`). 소비자는 이 값을 그대로 청크의 `meta.relation_refs`로 옮긴다 — 자세한 키 목록은 [schema.md](schema.md) §5. enrich가 동명 후보 문서를 여러 개 남긴 경우의 `target_candidates`(후보 수)·`target_candidate_paths`(후보 전원 목록)와 `target_missing_unit` 표시도 함께 `relation_refs`로 옮긴다 — 대표를 못 정한(동률) 관계는 확정 경로 없이 목록만 오므로, 소비자는 목록에서 고르면 된다.

## 3. 소비 흐름

```text
package 읽기
  -> source 결정
  -> law_id별 record 버퍼링
  -> 문서마다 컬렉션 라우팅(아래)
  -> document payload가 있으면 내부망 repo mirror 선택 수행
  -> preprocessed_chunk/file/normalized_chunk를 같은 law_id 버퍼에 합침
  -> 새 청크 upsert
  -> 전부 성공했을 때만 오래된 version_uid 청크 삭제
  -> 성공 package 이동 또는 삭제
```

여기서 `version_uid`는 문서의 버전 식별자다 — 개정되면 값이 바뀌므로, 옛 `version_uid` 청크만 지우면 개정 전 청크가 정리된다.

중요한 점은 **upsert 먼저, 옛 청크 삭제는 나중**이라는 점이다. 새 버전 적재가 실패하면 기존 청크가 검색에서 사라지지 않게 하기 위해서다. 게다가 upsert는 부분 실패에 예외를 던지지 않고 `{success, failed}`만 돌려주므로, **`failed == 0`일 때만** 옛 버전 청크를 지운다. 하나라도 실패했는데 옛 청크를 지우면 새 청크가 일부 빠진 채 옛 것까지 없어져 그 문서에 구멍이 난다.

폐지 문서처럼 `delete`만 있고 재적재할 내용이 없으면 `law_id` 단위로 삭제한다. `delete`가 있어도 재적재할 내용이 함께 왔으면 개정으로 보고 위의 upsert 경로를 탄다.

**컬렉션 라우팅.** 수집기 package의 source는 `law`/`admrul` 둘뿐인데 컬렉션은 3개다. 학칙·공단정관·공공기관 문서는 admrul package에 `doc_target`(school/pi/public)으로 섞여 오므로, 소비자가 **문서 단위**로 schlpub 컬렉션에 갈라 넣는다(payload가 없는 `delete` record는 `law_id` 접두 `school:`/`pi:`/`public:`으로 판별). genos처럼 컬렉션마다 Weaviate API 키가 다른 환경을 위해, 다른 컬렉션 문서가 나오면 그때 그 키로 연결을 새로 연다.

package 안에 여러 문서가 있으면 `law_id`별로 버퍼를 나눠 처리한다.

- `document`, `file`, `preprocessed_chunk`, `delete`가 package 안에서 흩어져 있어도 같은 `law_id`면 한 버퍼로 모은다.
- 한 package 안의 100개 중 99개가 성공하고 1개가 실패하면, 성공한 99개는 이미 Weaviate에 반영될 수 있다.
- 다만 package 전체에는 오류가 남으므로 성공 archive/delete 처리하지 않는다.
- 재시도 시 package 전체를 다시 읽을 수 있다. 같은 `law_id` upsert/delete는 멱등이어야 하므로 중복 재처리로 데이터가 망가지지 않게 구성되어 있다.

## 4. file record 처리

첨부·파일 본문이 어떤 record_type으로 오느냐에 따라 임베딩기가 전처리를 하는지 갈린다. 이미 텍스트로 온 것(`document`·`normalized_chunk`·`preprocessed_chunk`)은 청킹만 하고, 원본 파일(`file`)로 온 것만 내부망 전처리기(Doc Parser)에 보낸다.

| package record | 임베딩기 동작 | 전처리 |
| --- | --- | --- |
| `document` | payload를 읽어 조문/부칙/텍스트 별표를 청킹한다. | 안 함 |
| `normalized_chunk` | 생산자가 미리 정규화·청킹한 본문 텍스트를 JSON 청크로 만든다. | 안 함 |
| `preprocessed_chunk` | 이미 전처리된 텍스트를 FILE 청크로 만든다. | 안 함 |
| `file` | 원본 파일을 내부망 전처리기에 보내 FILE 청크로 만든다. | 필요(아래) |
| `pending_attachment` | 보류 정보만 남긴다. | 안 함 |
| `delete` | `law_id` 기준 기존 청크를 삭제한다. | 안 함 |

### `file` record의 파일 확보

`file` record는 파일 바이트를 실어 오는 방식이 둘이고, 소비자는 `content_b64`를 먼저 본다.

| 방식 | record 필드 | 파일 확보 | 필요한 env |
| --- | --- | --- | --- |
| base64(자기완결) | `content_b64` | base64를 임시 파일로 decode | `DOC_PARSER_BASE_URL` |
| 옆채널 파일 | `transfer_name` | `PACKAGE_FILE_INBOX_DIR/transfer_name` 파일을 찾음 | `PACKAGE_FILE_INBOX_DIR`, `DOC_PARSER_BASE_URL` |
| 둘 다 없음 | — | 확보 불가 → pending | — |

**옆채널 파일**은 파일 바이트를 JSONL 밖(공유폴더/NFS 등)으로 따로 보내는 전달 방식이고, record는 `transfer_name`으로 inbox 안 파일 이름을 가리킨다. 이렇게 package를 만드는 생산자(수집기) 모드 이름이 `file_transfer`다(생산자 설정 — 자세힌 temporal_law 문서).

record에 `sha256`이 있으면 전처리 전에 대조한다. 값이 다르면 전송 중 손상으로 보고 전처리하지 않고 pending으로 남긴다 — 깨진 파일을 그대로 색인하면 재시도해도 같은 `file_id` 아래 오염된 청크가 남는다.

확보한 파일은 전처리기를 거쳐 `preprocessed_chunk`와 같은 내부 형식으로 합쳐지고, 최종적으로 `map_attachment_data()`를 지나 `source_type=FILE`(`unit_type=FILE` 또는 `APPENDIX`) Weaviate 객체가 된다.

### 내부 전처리를 켤지

`file` record를 실제로 전처리기에 보낼지는 env `PACKAGE_PREPROCESS_FILES`로 정하고, 미지정이면 소비자 자신의 `DOC_PARSER_BASE_URL` 유무로 자동 유도한다 — 주소가 있으면 on, 없으면 첨부는 pending. 즉 임베딩기 쪽에 전처리기가 안 붙어 있으면 `file` record는 색인되지 않고 보류된다.

그래서 무엇을 보낼지는 생산자(수집기)가 망 구성에 맞춰 정한다. 전처리 결과(`preprocessed_chunk`)를 담아 보내면 소비자 전처리기가 없어도 FILE 청크를 만들 수 있고, 원본 파일(`file`)로 보내면 소비자 쪽 전처리기 연결이 있어야 파일 본문까지 적재된다. (생산자 첨부 모드 `preprocess`/`base64`/`file_transfer`는 생산자 설정 — 자세힌 temporal_law 문서.)

전처리 성공 후:

- 청크는 FILE 객체로 Weaviate에 들어간다.
- `PACKAGE_MIRROR_REPO=true`면 내부망 repo에도 원본 파일을 저장한다.
- `PACKAGE_DELETE_CONSUMED_FILES=true`면 성공한 inbox 파일을 삭제한다.

### pending과 실패는 다르다

`file` 처리가 안 되는 경우는 결과가 갈린다.

- **전처리기 자체가 없을 때**(내부 전처리 off = `DOC_PARSER_BASE_URL` 빈 값): 파일을 pending으로 남기되 **오류를 기록하지 않는다.** package는 정상 소비로 끝난다.
- **전처리기는 있는데 실패**(`DocParserError`)하거나 **inbox에서 파일을 못 찾을 때**: 그 파일만 pending으로 두되 **`totals["errors"]`에 오류를 남긴다.** 오류가 남은 package는 `consume-folder`가 `processed/`로 넘기지 않고, 재시도하다가 최종적으로 `failed/`로 격리한다(§6).

두 경우 모두 같은 package의 다른 record(문서 본문 `document` 등)는 그대로 계속 처리된다(소비는 record 단위로 진행). 다만 오류가 남았다면 그 package가 "성공 처리"된 것은 아니다.

## 5. repo mirror

`PACKAGE_MIRROR_REPO=true`면 package 소비 중 내부망 data repo를 갱신한다. 위치 기준은 생산자가 실어 준 `git_path`(데이터 레포 상대경로)로, 소비자가 그 경로에 원본 위치를 그대로 재현한다.

| record | mirror 동작 |
| --- | --- |
| `document` | `git_path` 위치에 payload JSON 저장(tmp에 쓴 뒤 replace) |
| `file` | 문서 디렉터리 아래 초기 export와 같은 서브폴더에 저장. 파일명 앞머리로 별표류 폴더(`별표`·`별지`·`서식`·`별첨`·`부속서` 등)를 판별하고, 안 되면 `unit_type`으로 `별표`(APPENDIX)·`첨부파일`(FILE), 본문이미지 파일명 형식이면 `본문이미지`, 그래도 판별이 안 되면 `첨부미러/` |

레포는 3개(LAW/ADMRUL/SCHLPUBRUL)이므로 mirror도 문서의 `doc_target`에 맞는 레포를 골라 쓴다. 한 package에 admrul과 학칙공단 문서가 섞여 와도 각자 제 레포로 간다. 문서 → 파일 순서로 처리되므로, `document`에서 정한 레포를 그 문서의 파일과 커밋에 이어 쓴다.

`file` record의 원본 파일은 **전처리보다 먼저** 레포에 복사한다. 전처리가 실패해도 원본은 남는다.

`PACKAGE_MIRROR_PUSH=true`면 변경 후 내부 Git 서버로 commit/push한다(문서 단위 commit, package 끝에 한 번 push). 커밋 메시지는 `document` record가 실어 온 수집기 메시지를 그대로 쓴다. 이때 origin은 내부망 Git 서버를 가리켜야 한다.

mirror는 Weaviate 적재와 별개의 선택이다.

| 설정 | 결과 |
| --- | --- |
| `PACKAGE_MIRROR_REPO=false` | package는 Weaviate 적재에만 사용하고 내부망 data repo는 만들지 않는다. |
| `PACKAGE_MIRROR_REPO=true` | package의 `git_path` 기준으로 내부망 data repo 구조를 갱신한다. |
| `PACKAGE_MIRROR_PUSH=true` | 갱신 후 내부 Git 서버로 commit/push한다. |

수집기가 `manifest` 모드라서 DMZ에 payload/file을 남기지 않아도, package 안에 `document.payload`, `git_path`, `file` record가 있으면 소비자가 내부망 repo를 만들 수 있다.

## 6. consume-folder 안전장치

`consume-folder`는 단순 for-loop가 아니라 운영 중 재시도를 고려한다.

| 장치 | 설명 |
| --- | --- |
| 폴더 존재 검증 | 없는/오타 경로면 "0건 성공"으로 조용히 끝내지 않고 즉시 오류를 낸다. |
| 원자적 claim | 소비 전 `package.jsonl.processing`으로 rename한다. |
| stale 복구 | 이전 실행이 죽어 남은 `.processing` 파일을 다음 실행 시작에 되돌린다. |
| transient 재시도 | Weaviate 연결 실패, timeout, 서버 오류, OOM 등은 원래 이름으로 되돌리고 다음 sweep에서 재시도한다. 횟수는 옆의 `<package>.attempts` 파일로 세고, 기본 5회를 넘으면 `failed/`로 격리한다. |
| 영구 오류 격리 | JSON 파싱 실패, source 미결정, footer count 불일치 등은 `failed/`로 보낸다. 오류 내역은 `<package>.errors.json`으로 함께 남긴다. |
| 잘린 package 감지 | 생산자는 항상 footer를 쓴다. header는 있는데 footer가 없으면 전송 중 잘린 것으로 보고 오류로 남긴다(그냥 통과시키면 남은 문서가 조용히 누락된다). |
| 성공 처리 | 성공 package는 `processed/`로 이동하거나 설정에 따라 삭제한다. |

한 package 실패가 다음 package 소비를 막지 않고, 재실행은 멱등이다.

위 재시도·격리는 package 파일 단위다. package 안의 일부만 실패하는 경우는 따로 갈린다.

| 실패 | 처리 |
| --- | --- |
| 특정 파일 전처리 실패 | package 전체를 막지 않고 해당 파일을 pending으로 남길 수 있다. |
| 특정 law_id upsert 실패 | package에는 오류가 남아 재시도/격리 대상이 된다. 성공한 다른 law_id는 이미 반영될 수 있다. |

## 7. 관련 env

ENV 목록의 정본은 [README](../README.md) §5.5·§5.4다. 여기서는 위 소비 규칙과 직접 맞물리는 것만 다시 적는다.

| env | 설명 |
| --- | --- |
| `PACKAGE_PREPROCESS_FILES` | `file` record를 내부 전처리기로 돌릴지. 미지정이면 `DOC_PARSER_BASE_URL` 유무로 자동 유도(있으면 on, 없으면 pending) |
| `PACKAGE_FILE_INBOX_DIR` | 옆채널 `transfer_name` 파일을 찾을 폴더 |
| `DOC_PARSER_BASE_URL` | 내부망 전처리기 주소(비면 내부 전처리 off) |
| `PACKAGE_MIRROR_REPO` | 내부망 data repo 갱신 여부 |
| `PACKAGE_MIRROR_PUSH` | mirror 후 commit/push 여부 |
| `PACKAGE_STORE_ORIGINAL` | repo 외 별도 원본 저장 여부(`PACKAGE_ORIGINAL_DIR`과 **둘 다** 설정해야 동작) |
| `PACKAGE_ORIGINAL_DIR` | 별도 원본(JSON/파일) 저장 루트 |
| `PACKAGE_DELETE_CONSUMED_FILES` | 전처리 성공 파일 삭제 여부 |
| `PACKAGE_DELETE_CONSUMED_PACKAGE` | 성공 package 삭제 여부 |
