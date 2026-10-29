# GenOS 법 목록 관리 화면 연동

적재 대상 목록의 출처는 GenOS 관리자 화면 **데이터 > 법 목록 관리**(`/data/law-scope`) 입니다.
이전의 프롬프트 748(`/prompt/runtime`) 연동은 대체되었습니다.

## 구성

1. `src/law_indexer/` 의 `worker.py`, `pipeline.py`, `law_scope.py` 를 코드스페이스의 같은 경로에 둡니다.
   `prompt_scope.py` 는 더 이상 쓰지 않습니다.
2. 코드서빙 배포 환경변수에 `LAW_SCOPE_ENABLED=true` 를 추가합니다.
   `LLMOPS_ADMIN_API_URL` 은 GenOS 가 주입하는 내부 주소를 쓰고, 없으면
   `http://llmops-admin-api-service:8080` 입니다. 로그인 토큰은 쓰지 않습니다.
3. 목록은 화면에서 법령명 / 소관부처를 추가하고 **저장**하면 반영됩니다.
   저장 위치는 `genos_config.key='law_scope'` 이고 값은 아래 형식입니다.

```json
{"law_names":["가사소송수수료규칙","가사소송규칙"],"ministries":[]}
```

### admin-api 조회 API

`GET /data/law-scope` 는 **무인증**입니다(`/prompt/runtime` 운영 본문 조회와 같은 규약).
색인 워커처럼 로그인 토큰이 없는 클러스터 내부 소비처가 읽어야 하기 때문입니다.
저장(`POST /data/law-scope/update`)은 그대로 인증을 요구합니다.

## 동작

`InitialIndexWorkflow` 의 `initial_index` Activity 가 실행될 때마다 목록을 새로 조회합니다.
캐시나 개인 로그인 토큰은 쓰지 않습니다. 실행 중 바꾼 목록은 다음 Activity 실행/재시도부터
반영됩니다. 조회 실패, 빈 목록, 잘못된 응답이면 적재를 시작하지 않습니다.
`LAW_SCOPE_ENABLED` 를 켜지 않으면 기존 무필터 동작(전량 적재)이 유지되므로 반드시 켜세요.

JSON 의 `law_name` 또는 `basic_info` 의 `소관부처`/`소관부처명` 이 일치하면 적재합니다(OR).

비교 규칙은 **수집기(temporal_law `collector/common.compact()`)와 같아야 합니다** — 공백(일반·전각),
가운뎃점 5종(`ㆍ·․‧・`), 전각 영숫자를 정규화하고 HTML 엔티티를 복원한 뒤 비교합니다. 양쪽이 어긋나면
수집기는 담고 색인기는 버리는 조용한 누락이 납니다. 수집기 쪽 규칙이 바뀌면 `law_scope.compact()` 도
같이 바꿔야 합니다.

소관부처는 한 문자열에 묶여 오는 경우가 있어(실측 `경찰청,법무부` — 법제처 데이터의 약 8%)
콤마·세미콜론으로 나눠 각각 비교합니다. 배열과 `{"content": ...}` 형태도 평탄화합니다.

목록에 적었는데 한 번도 안 걸린 항목은 적재 후 경고로 남습니다(대개 오타이거나 구명칭 —
예: 안전행정부). 결과에는 `scope_matched`, `scope_skipped`, `scope_law_names`, `scope_ministries`,
`scope_names_unmatched`, `scope_ministries_unmatched` 가 포함됩니다.

목록 저장 자체가 작업을 자동 실행하지는 않습니다. 기존 Temporal 실행 시 이 필터가 적용됩니다.
목록의 문서를 모두 처리하려면 요청의 `limit` 을 생략하세요(`limit` 은 필터 전 파일 탐색 수 제한입니다).

이 필터는 `InitialIndexWorkflow` 전용입니다. CLI `index` 직접 실행과 `ConsumePackageWorkflow`
에는 적용되지 않으며, 기존 청크 삭제도 수행하지 않습니다.

## 검증

`tests/test_law_scope.py` 18개 테스트 — 파싱(600개 목록·OR·응답 오류 5종), 정규화(compact 규칙
대조·공백/가운뎃점 차이·콤마 묶음 5종·배열/중첩 dict), 미매칭 경고(원본 표기 유지·이름 적중 시에도
부처 집계), 조회(스위치 OFF 3종·매번 새 조회·토큰 미사용·기본 내부 주소·HTTP 실패 중단),
그리고 실제 `index_documents` 를 돌리는 분기 3종(제외 시 청크 0·미매칭 집계·콤마 묶음 회귀).
운영 서버 연결·적재는 미검증입니다.
