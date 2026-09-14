# law_embedding

법령·행정규칙·학칙공단 데이터를 청킹·임베딩하여 Weaviate에 적재하는 코드입니다.
수집기는 포함하지 않으며, 수집기가 만든 data repo 또는 JSONL package를 입력으로 사용합니다.

## 빠른 시작

```bash
uv sync
cp .env.example .env
uv run python -m law_indexer health
uv run python -m law_indexer create-collection --source all
```

전체 색인:

```bash
uv run python -m law_indexer index --source all --skip-existing
```

JSONL package 증분 소비:

```bash
uv run python -m law_indexer consume-folder --dir /mnt/handoff/packages
```

검색 확인:

```bash
uv run python -m law_indexer search --source law --query "연차 유급휴가" --limit 5
```

## GenOS 목록 연동

`InitialIndexWorkflow`는 `LAW_SCOPE_PROMPT_ID`가 설정된 경우 GenOS 운영 프롬프트에서 적재 대상 목록을 읽습니다.

```text
LAW_SCOPE_PROMPT_ID=748
```

프롬프트 본문 형식:

```json
{"law_names":["가사소송규칙"],"ministries":[]}
```

- `law_names` 또는 `ministries` 중 하나라도 정확히 일치하면 적재합니다.
- 두 목록이 모두 있으면 OR 조건입니다.
- 프롬프트 조회 실패·빈 목록·잘못된 형식이면 적재를 중단합니다.
- `LAW_SCOPE_PROMPT_ID`를 설정하지 않으면 기존처럼 전체 적재합니다.
- 목록 변경은 다음 Workflow Activity 실행부터 반영됩니다.

## 주요 환경변수

Weaviate 접속에는 `WEAVIATE_HTTP_HOST`, `WEAVIATE_HTTP_PORT`, `WEAVIATE_GRPC_HOST`, `WEAVIATE_GRPC_PORT`, `WEAVIATE_API_KEY`를 사용합니다.
컬렉션은 `LAW_COLLECTION`, `ADMRUL_COLLECTION`, `SCHLPUB_COLLECTION`으로 지정합니다.

임베딩은 `EMBEDDING_BACKEND=local|remote`로 선택합니다. 원격 모드는 `EMBEDDING_API_URL`과 `EMBEDDING_API_KEY`를 사용합니다. 색인과 검색은 같은 모델과 벡터 차원을 사용해야 합니다.

Temporal 워커를 실행할 때는 `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TLS`, `INDEX_TASK_QUEUE`를 설정합니다.

전체 설정 예시는 [.env.example](.env.example)를 참고하세요.

## 디렉터리

- `src/law_indexer/`: 색인기·Temporal 워커·Weaviate 저장소
- `tests/`: 단위 및 통합 테스트
- `docs/schema.md`: Weaviate 스키마
- `docs/genos_prompt_scope.md`: GenOS 프롬프트 목록 연동 상세
- `genos_vdb_schema_*.json`: 컬렉션 정의서
