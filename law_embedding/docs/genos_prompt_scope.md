# 748번 운영 프롬프트 연결

이 폴더는 기존 law_embedding 전체가 아니라 덮어쓸 변경 파일 3개입니다.
원본 로컬 레포 및 원격 코드스페이스/배포본은 수정하지 않았습니다.

1. src/law_indexer/의 worker.py, pipeline.py, prompt_scope.py를 코드스페이스의 동일 경로에 복사합니다. 기존 main.py와 worker_workflows.py는 원본 버전을 유지합니다. 이전 law-pipeline-17 토큰 방식 파일은 사용하지 않습니다.
2. 코드서빙 배포 환경변수에 LAW_SCOPE_PROMPT_ID=748을 추가합니다. LLMOPS_ADMIN_API_URL은 GenOS가 주입하는 내부 주소를 사용하며 없으면 http://llmops-admin-api-service:8080입니다. 로그인 토큰은 사용하지 않습니다.
3. 프롬프트 748의 운영 본문을 아래 JSON 형식으로 지정합니다. 마크다운 코드 블록이나 설명 문장은 본문에 넣지 않습니다.

{"law_names":["가사소송수수료규칙","가사소송규칙"],"ministries":[]}

4. 파일 변경을 커밋하고 새 커밋으로 한 번 재배포합니다. 이후 목록은 프롬프트 화면에서 수정하고 운영 버전으로 적용하면 됩니다.

## 동작
InitialIndexWorkflow의 initial_index Activity가 실행될 때마다 운영 본문을 새로 조회합니다. 캐시나 개인 로그인 토큰은 사용하지 않습니다. 실행 중 변경한 목록은 다음 Activity 실행/재시도부터 반영됩니다. 조회 실패, 운영 미지정, 빈 목록, 잘못된 JSON이면 적재를 시작하지 않습니다. 환경변수 LAW_SCOPE_PROMPT_ID를 아예 지정하지 않으면 기존 무필터 동작이 유지되므로 반드시 748을 지정하세요.

JSON의 law_name 또는 basic_info의 소관부처/소관부처명이 정확히 일치하면 적재합니다. 공백은 양끝만 제거합니다. scope_matched, scope_skipped, scope_prompt_id, scope_revision_id를 결과에 포함합니다.

목록 저장 자체가 작업을 자동 실행하지는 않습니다. 기존 Temporal 실행 시 이 필터가 적용됩니다. 두 문서를 모두 처리하려면 요청의 limit를 생략하세요(기존 limit는 필터 전 파일 탐색 수 제한입니다).

이 변경은 InitialIndexWorkflow 전용입니다. CLI index 직접 실행, ConsumePackageWorkflow에는 적용되지 않으며, 기존 청크 삭제도 수행하지 않습니다.

검증: tests/test_prompt_scope.py 7개 테스트(600개 목록, OR 필터, 응답 오류, 매번 새 조회, 토큰 미사용, 실제 index_documents의 제외 분기) 및 변경 파일 구문 검사. 운영 서버 연결/적재 미검증.
