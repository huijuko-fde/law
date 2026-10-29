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

`InitialIndexWorkflow`는 `LAW_SCOPE_ENABLED=true`인 경우 GenOS 관리자 화면 **데이터 > 법 목록 관리**(`/data/law-scope`)에서 적재 대상 목록을 읽습니다.

```text
LAW_SCOPE_ENABLED=true
LLMOPS_ADMIN_API_URL=http://llmops-admin-api-service:8080
```

화면이 저장하는 목록 형식(`genos_config.key='law_scope'`):

```json
{"law_names":["가사소송규칙"],"ministries":[]}
```

- `law_names` 또는 `ministries` 중 하나라도 일치하면 적재합니다.
- 두 목록이 모두 있으면 OR 조건입니다.
- 비교는 수집기(temporal_law)와 같은 정규화를 씁니다 — 공백·가운뎃점·전각 차이는 무시합니다.
- 소관부처가 `경찰청,법무부`처럼 묶여 오면 각 부처로 나눠 비교합니다.
- 목록에 적었는데 한 번도 안 걸린 항목은 경고로 남습니다(오타·구명칭 감지).
- 조회 실패·빈 목록·잘못된 응답이면 적재를 중단합니다.
- `LAW_SCOPE_ENABLED`를 켜지 않으면 기존처럼 전체 적재합니다.
- 목록 변경은 다음 Workflow Activity 실행부터 반영됩니다.
- 조회(`GET /data/law-scope`)는 무인증이라 로그인 토큰이 필요 없습니다.

## ZIP 수신

상대 측이 보내는 법령 ZIP 을 기존 JSONL package 로 바꿔(1단계 `zip_intake.py`) 기존 package 소비 경로로 적재합니다(2단계 `zip_consume.py`, `consume-zip` 명령).

### 적재 (2단계: `consume-zip`)

```bash
uv run python -m law_indexer consume-zip --dir /mnt/handoff/zip
#   --package-dir  변환 package 를 두고 소비할 폴더(기본 <dir>/packages) — consume-folder --dir 와 같은 규칙
#   --nack         consume-folder --nack 과 같다
```

- 폴더 바로 아래 `*.zip` 만 **파일명 순서**로 처리합니다. 점으로 시작하는 파일·임시 확장자·manifest·기타 파일은 보지 않습니다.
- ZIP 마다: SHA-256 → 이력 확인 → manifest 대조 → 검증(`read_zip`) → package 변환(`write_packages`) → **기존 `consume_folder` 로 바로 소비** → 이력 확정 → `done/`·`failed/` 로 이동(원본은 지우지 않습니다).
- 바로 소비하는 이유: 같은 문서가 여러 ZIP 에 있으면 나중 ZIP 이 최종이어야 하는데, 이는 앞 ZIP 이 실제로 적재됐는지 알아야 지킬 수 있습니다. 앞 ZIP 의 package 가 일시장애로 재시도 대기면 그 ZIP 은 수신 폴더에 남고(`loading`) 뒤 ZIP 은 다음 실행으로 미룹니다. 이 명령을 주기적으로 돌리면 됩니다(같은 폴더를 두 실행이 동시에 처리하지 않도록 잠급니다).
- package 폴더는 그대로 consume-folder 폴더입니다 — `processed/`·`failed/`(`.errors.json`)·`nack/`·재시도(`.attempts`, 5회 후 격리) 규칙이 같습니다.
- 첨부는 **file_transfer** 로 넘깁니다(첨부를 폴더에 풀고 record 에는 `transfer_name` 만). 폴더는 `PACKAGE_FILE_INBOX_DIR`, 비어 있으면 `<package-dir>/files` 이고 소비도 그 폴더를 봅니다. 소비가 끝난 첨부를 지우려면 `PACKAGE_DELETE_CONSUMED_FILES=true`.
- 문서 1건 오류는 그 문서(같은 `law_id` 전체)만 빼고 나머지를 적재합니다(부분완료). exit 코드: 0 = 모두 완료(재시도 대기 포함), 2 = 부분완료·실패 있음, 1 = 하드 에러.

```text
<dir>/
├── 0001_batch-….zip             수신 ZIP
├── 0001_batch-….manifest.json   (선택) manifest
├── done/        완료·부분완료 ZIP (+ manifest)
├── failed/      실패 ZIP (+ manifest)
├── duplicate/   이미 처리한 SHA-256 과 같은 ZIP(이름이 달라도)
├── state/       <sha256>.json 처리 이력, .lock
└── packages/    consume-folder 폴더(processed/ · failed/ · nack/ · .zip-work/<sha256> 변환 작업)
    └── files/   첨부(PACKAGE_FILE_INBOX_DIR 미설정 시)
```

처리 이력 `state/<sha256>.json` — ZIP 단위, tmp→fsync→rename 으로 기록하고 단계를 먼저 기록한 뒤 움직이므로 중간에 죽어도 다음 실행이 이어 갑니다.

| status | 뜻 | ZIP 위치 |
|---|---|---|
| `done` | 완료 | `done/` |
| `partial` | 부분완료 — 거부 문서·ZIP 단위 오류(고아 첨부 등)를 빼고 적재 | `done/` |
| `failed` | 실패 — `reason`: `zip_corrupt`·`manifest_mismatch`·`manifest_invalid`·`all_rejected`(실을 문서 없음)·`load_failed`(consume-folder 가 package 격리) | `failed/` |
| `loading` | package 게시됨·적재(재시도) 대기 — 다음 실행이 이어 감 | 수신 폴더 |
| `converting` | 변환 중 죽음 — 다음 실행이 작업 폴더를 버리고 처음부터 | 수신 폴더 |

기록 항목: `zip_name`·`size_bytes`·`attempt`·시각, `summary`(문서·적재·삭제·거부·중복 수, `files`·`pending_attachments`, `zip_errors`), `packages`(이름·건수·적재 결과), `issues`(`Issue` 전체: level·code·message·path·law_id), `archive`(옮긴 위치), `duplicates`(다시 온 같은 ZIP), `history`(재처리 전 상태).

재처리:

- **실패 ZIP** — `failed/` 의 ZIP(과 manifest)을 수신 폴더로 다시 옮기면 처음부터 다시 처리합니다(같은 SHA-256 이라도 실패 이력은 막지 않고 `history` 에 남습니다). manifest 불일치면 상대에게 다시 받은 manifest 와 함께 넣습니다.
- **`load_failed`** — `packages/failed/*.errors.json` 으로 원인을 고친 뒤 ZIP 을 다시 넣습니다. 이때 격리된 package 는 `requeue_failed_packages.py` 로 되돌리지 말고 지웁니다(같은 내용이 두 번·순서가 바뀌어 들어갑니다).
- **완료 ZIP 을 일부러 다시 적재** — `state/<sha256>.json` 을 지우고 넣습니다.
- 순서 주의: 실패 ZIP 을 나중에 재처리하면 그 사이 처리된 뒤 ZIP 보다 옛 내용이 최종이 될 수 있습니다. 같은 문서를 가진 뒤 ZIP 이 있으면 그 ZIP 도 이력을 지우고 이어서 다시 넣습니다.
- ZIP 수신에는 nack 을 읽을 생산자가 없습니다 — `nack/` 은 기록으로만 남습니다.

manifest — ZIP 옆 `<stem>.manifest.json`(수집기 zipbatch 형식, `<이름>.zip.manifest.json` 도 받음):

- `sha256`(필수) 를 실제 ZIP 과, `documents`·`repealed` 를 ZIP 안 문서 JSON 수·폐지 수와 대조합니다. 하나라도 다르거나 manifest 를 못 읽으면 실패(`manifest_mismatch`/`manifest_invalid`)로 아무것도 적재하지 않습니다. `filename` 이 ZIP 이름과 다르면(순번을 붙여 이름만 바꾼 경우) 경고만 합니다.
- 없으면 경고(`manifest_missing`)만 하고 처리합니다.
- 내부 수집기 zipbatch 는 한 문서의 같은 이름·다른 내용 첨부를 `{stem}__{sha12}{ext}` 로 바꿔 싣고 그 사실을 manifest `changes[].attachment_files`(url·path·sha256)에만 남깁니다. manifest 가 있으면 이 목록으로 원래 이름·URL 에 되돌려 연결합니다.

상대 측 전달 요구사항:

1. ZIP 은 임시 이름(점으로 시작하거나 `.zip` 이 아닌 확장자)으로 다 쓴 뒤 `*.zip` 으로 이름을 바꿉니다.
2. manifest 는 ZIP 보다 먼저(늦어도 동시에) 둡니다. ZIP 이 먼저 보이면 manifest 없이 처리될 수 있습니다.
3. 처리 순서는 파일명 정렬입니다 — 순번은 고정 자릿수로 앞에 붙입니다(`0001_…`, `0002_…`). manifest 이름도 같이 바꿉니다.
4. 같은 ZIP 재전송은 안전합니다(SHA-256 중복 방지). 내용을 고쳤으면 새 ZIP 으로 보냅니다.
5. 한 ZIP 안에서 같은 `law_id` 는 한 버전만 보냅니다(아래 다중 버전 정책).
6. 폐지는 `repealed: true` 와 `law_id`. `law_id` 가 없으면 법령만 `doc_uid: "law:eflaw:<law_id>"` 로 받습니다.

### 변환 (1단계: 해석·검증·package 변환)

```python
from pathlib import Path
from law_indexer.zip_intake import read_zip, write_packages

result = read_zip(Path("batch-20261015-030000-xxxx.zip"))   # ZIP 이 깨졌으면 ZipIntakeError
print(result.summary())                                      # 문서·거부·중복·오류·경고 건수
write_packages(result, Path("/mnt/handoff/packages"))        # source 별 <source>-<시각>-<접미>.jsonl
```

- 최상위 폴더 `law/`·`admrul/` 이 package source 입니다(학칙·공단정관·공공기관도 `admrul/`). 그 아래 폴더 구조는 보지 않습니다.
- 문서는 JSON 필드(`law_id`·`version_uid`·`doc_target`·`repealed`)로 식별합니다. `doc_target` 이 최상위 폴더와 모순되면 거부합니다.
- 첨부는 가장 가까운 상위 폴더의 문서 JSON 중 그 파일명을 첨부 목록(`appendices`·`attachments`·조문 `images`)에 가진 문서에 붙습니다. 그런 문서가 없거나 둘 이상이면 오류입니다.
- 첨부 중 package 에 싣는 것은 수집기와 같습니다 — `is_file_only` 별표, 본문 없는 문서의 원문, `[그림]` 본문이미지. ZIP 에 없으면 `pending_attachment` 로 싣습니다.
- 같은 `law_id`·같은 `version_uid` 인데 내용이 다르면 충돌로 거부하고, 완전히 같으면 하나만 싣습니다. 하나라도 거부되면 그 `law_id` 전체를 뺍니다.
- **다중 버전 정책(임시)**: 한 ZIP 에 같은 `law_id` 가 서로 다른 `version_uid`(현행 + 시행예정 등)로 오면 그 `law_id` 전체를 거부합니다(`multi_version_unsupported`). 적재기는 `law_id` 당 한 버전만 보관하므로(소비자는 package 의 마지막 `document` 만 남기고 나머지 버전 청크를 지웁니다) 현행·시행예정본을 함께 보관할지 기획이 정해질 때까지 어느 한 버전을 골라 싣지 않습니다. 소비자 동작은 바꾸지 않았습니다.
- `repealed: true` 는 `delete` record 가 됩니다. 본문 없는 폐지 JSON(수집기 `MANIFEST_ONLY`)처럼 `law_id` 가 없으면 `doc_uid` 가 `law:eflaw:<law_id>`(수집기 `make_doc_uid`, 법령)일 때만 거기서 `law_id` 를 꺼냅니다(`law_id_from_doc_uid` 경고). 행정규칙류 `doc_uid` 의 id 는 수집기 사본마다 다르고(DMZ: 연도 뺀 이름 키, 내부: 행정규칙ID) `law_id` 와 같다고 볼 수 없어 기존처럼 거부합니다.
- 첨부는 기본 `content_b64` 인라인입니다. `file_stage_dir` 를 주면 그 폴더에 풀고 `transfer_name` 을 싣습니다(`PACKAGE_FILE_INBOX_DIR` 와 같은 폴더여야 합니다). `consume-zip` 은 항상 이 방식입니다.
- `read_zip(path, manifest=…)` 로 manifest 를 주면 이름이 바뀐(`{stem}__{sha12}`) 첨부를 원래 이름에 연결하고, 같은 이름 파일이 여럿이면 manifest 의 URL 로 가릅니다.
- 오류·경고는 `result.errors`/`warnings`(ZIP 단위)와 `document.errors`/`warnings`(문서 단위)에 `Issue.code` 로 남습니다.

## 운영 가동 — 코드서빙 800 + Temporal (LAW-28)

상대 측이 보낸 ZIP 이 사람 손 없이 적재되는 경로입니다. 코드서빙 800 파드 하나가 **수신기(`law_indexer.ingest_app`)와 Temporal 워커**를 같이 돌립니다. Temporal Schedule `law-inbox` 가 `LawInboxWorkflow`(변환 activity → consume-zip activity → 판정)를 주기로 띄우고, 업로드가 오면 수신기가 그 Schedule 을 즉시 trigger 합니다. 실행 이력·결과·실패 사유는 Temporal 서버에 남아 `/temporal` UI 에서 보입니다.

```text
상대 측 ──POST /api/v1/law/ingest (file + manifest)──▶ <LAW_ROOT>/<수신시각>_<이름>.zip + .manifest.json
                                                           │  Schedule law-inbox: 업로드 즉시 trigger + LAW_SCHEDULE_CRON
                                                           ▼
   LawInboxWorkflow:  convert activity ─▶ <LAW_ROOT>/converted/*.zip ─▶ consume-zip activity ─▶ Weaviate ─▶ 판정
                      (원본 쌍 → done/ 또는 failed/)        (state/ done/ failed/ duplicate/ packages/)   (멈춤이면 실패)
```

- 멈춤 사유(아래)가 있으면 워크플로가 실패하고 Schedule 이 **일시정지**됩니다(`pause_on_failure`). 일시정지 = HALT. 수신은 계속 받되 trigger 하지 않습니다.
- 겹침은 `overlap=SKIP` — 돌고 있으면 건너뛰고 다음 주기가 이어 받습니다. 소비가 일시장애 대기(`waiting`)면 워크플로 안에서 15분 자고 다시 소비합니다(최대 5회).
- `LAW_SCHEDULER=temporal` 이 운영 모드입니다(못 붙으면 기동 실패 — 조용히 로컬로 떨어지지 않습니다). `auto` 는 TEMPORAL_* 가 있으면 temporal, 못 붙으면 경고 후 로컬 러너(`auto_ingest.Runner`, 이벤트 + 주기). `local` 은 Temporal 없는 개발·검증용.

### 수신 규약 — 상대 측에 안내한 "재전송하면 갱신" 과 같다

- 같은 ZIP 을 다시 보내면 200 `duplicate` — 저장·적재하지 않습니다(바이트 SHA-256 기준).
- 내용이나 파일명을 고쳐 다시 보내면 새 수신입니다. 수신 시각 접두가 붙어 **도착 순서 = 처리 순서**이고, 그 법령은 갱신됩니다 — 같은 버전(version_uid)이면 제자리 덮어쓰기 + 이번 결과에 없는 청크 정리, 새 버전이면 교체.
- **옛 버전은 건너뜁니다.** 저장된 것보다 (시행일, MST — 행정규칙은 개정일, 일련번호) 가 작으면 적재하지 않고 `version_skipped` 로 기록합니다(실패가 아닙니다). 최신판을 일부러 되돌릴 때만 `consume-zip --allow-older-version`(수동).
- manifest(`docs[].mst` 포함)는 **같은 요청의 `manifest` 필드**로 보냅니다. 없으면 422 — law 의 MST 가 manifest 에만 있어 변환할 수 없습니다. 받은 ZIP 은 `rejected/` 에 남깁니다. manifest 의 `sha256`·`bytes` 가 받은 바이트와 다르면 역시 422(전송 손상).
- 한 ZIP 20MB 이하(게이트웨이 본문 한도 실측 ~25MB).

### 멈춤(HALT)

변환 실패 ZIP · 적재 실패(`failed`) · 부분 완료(`partial`) · 품질 가드 · 잠금 충돌 · manifest 없이 유예 초과가 하나라도 있으면 워크플로가 실패하고 Schedule 이 일시정지됩니다(로컬 모드에서는 `<LAW_ROOT>/HALT.json`). 수신은 계속 받아 쌓습니다. 순서 규칙("나중 ZIP 이 최종") 때문에 실패 ZIP 을 두고 뒤를 계속 적재하면 재처리 때 옛 내용이 최신을 덮으므로 멈추는 쪽이 안전합니다. 사람이 원인을 고친 뒤 `POST /api/v1/pipeline/resume`(헤더 `X-Law-Ops-Token`) 로 풉니다 — unpause + 즉시 한 번. 실패분 재투입은 `POST /api/v1/pipeline/requeue?zip=<이름>&stage=convert|consume`(파드가 옮깁니다 — 파드 소유 폴더를 손으로 만질 필요 없음).

- 변환 실패분: `<LAW_ROOT>/failed/<이름>.zip`·`.manifest.json`·`.convert-report.json`. 고친 뒤 원본 쌍을 `<LAW_ROOT>/` 로 되돌립니다.
- 적재 실패·부분 완료: 위 「ZIP 수신」의 재처리 절차 그대로(`converted/` 가 consume-zip 폴더).
- 품질 가드: 적재분은 되돌리지 않습니다. 필터 점검 후 필요하면 해당 ZIP 을 `converted/state/<sha256>.json` 을 지우고 재투입.

### 보존·감시

- `LAW_RETENTION_DAYS`(기본 30): 적재가 끝난 산출물(`done/`·`converted/done/`·`duplicate/`·`packages/processed/`·`rejected/`·`ops/`·`runs/`)을 매 실행 끝에 지웁니다 — `/nfs-root` 는 플랫폼 전체가 공유하는 PVC 라 법령 파이프라인이 채우면 안 됩니다. 실패분(`failed/`)과 처리 이력(`converted/state/`)은 지우지 않습니다. 0 이면 영구 보존.
- `LAW_MANIFEST_GRACE_SEC`(기본 3600): manifest 없이 이 시간 넘게 놓인 ZIP 은 멈춤 사유 `manifest_missing:<이름>` 이 됩니다(수신기는 manifest 를 먼저 두므로 자동 경로엔 생기지 않고, 손으로 넣었거나 옛 수신기 잔존물입니다). 치우거나 manifest 를 놓고 resume.
- `LAW_EXPECT_INTERVAL_HOURS`(기본 0=끔): 마지막 수신 뒤 이 시간이 지나도록 ZIP 이 안 오면 `status.attention` 에 `no_receipt_for_Nh` 를 올립니다(멈추진 않습니다).

### 확인 창구 (GenOS 에 알림 기능이 없어 사람이 보러 간다)

- `GET /api/v1/pipeline/status` — `mode`·`state`(idle/running/halted)·`halt`(일시정지 사유)·`attention`·`schedule`(cron·다음 실행·실행 수)·`last_run`(결과 또는 멈춤 사유)·`last_received_at`·수신 대기·실패 수·디스크
- `GET /api/v1/pipeline/runs?limit=20` — 최근 워크플로 실행(Temporal visibility, 리텐션 7일). 상세는 `/temporal` UI 의 `LawInboxWorkflow`
- 코드서빙 800 로그의 `[law-ingest] …` 한 줄 기록, 코드스페이스에서 `<LAW_ROOT>` 직접 열람
- 상대 측이 받는 응답의 `pipeline.halted`

### 배포

1. `scripts/make_codeserving_bundle.sh ../../_bundle_cs800` → `main.py` + `src/law_indexer` + `lib/`(cp312 wheel 해제본, pip 미사용)
2. zip 으로 묶어 `POST …/800/api/v1/law/ingest?kind=ops` 로 올리고(20MB 초과면 split) 코드스페이스에서 풀어 Gitea `llmops/800` 내용을 통째로 교체 → push
3. 관리자 UI: 리비전 추가(새 커밋 해시) → 환경설정 탭 → 복제본 **1**·HA 끔 → 배포 → 가중치 100 → **구 리비전 수동 중지**(안 멈추면 두 파드가 같은 폴더를 봅니다 — `lease.json` 이 2차로 막지만 수칙이 1차)

환경변수: UI 에는 `LAW_ROOT`·`LAW_OPS_TOKEN`·`LAW_ENV_FILE`·`LAW_SCHEDULER=temporal`·`TEMPORAL_ADDRESS`·`TEMPORAL_NAMESPACE` 를 넣고 나머지는 `LAW_ENV_FILE` 이 가리키는 NFS 의 `.env`(운영 .env 사본)에서 읽는 것을 권합니다(UI 값이 우선). 전체 목록: `LAW_ROOT`(NFS, 예 `/nfs-root/DEV/volume/326/law-inbox`) · `LAW_SCHEDULER`(temporal) · `LAW_SCHEDULE_CRON`(`0 * * * *`, Asia/Seoul) · `INDEX_TASK_QUEUE`(law-embedding) · `TEMPORAL_ADDRESS`(예 `temporal-frontend.temporal.svc.cluster.local:7233`, 또는 코드서빙이 주입하는 `TEMPORAL_HOST`/`TEMPORAL_PORT`) · `TEMPORAL_NAMESPACE`(llmops) · `LAW_SWEEP_INTERVAL_SEC`(600, 로컬 모드만) · `LAW_OPS_TOKEN` · `LAW_HALT_ON_PARTIAL`(true) · `LAW_RETENTION_DAYS`(30) · `LAW_MANIFEST_GRACE_SEC`(3600) · `LAW_EXPECT_INTERVAL_HOURS`(0) + 운영 `.env` 의 `WEAVIATE_*`·`LAW_COLLECTION`·`EMBEDDING_*`·`DOC_PARSER_*`(`DOC_PARSER_SHARED_HOST_DIR` = `DOC_PARSER_SHARED_CONTAINER_DIR` = `<LAW_ROOT>/shared` — 800 과 355 가 같은 `/nfs-root` 를 본다) · `LAW_SCOPE_ENABLED`.

수동·검증: `python -m law_indexer auto-ingest --root <LAW_ROOT> --once` (멈춤 사유가 있으면 exit 2). 라이브 폴더에 `consume-zip` 을 따로 돌리지 않습니다(러너와 겹침).

### 새 사이트 배포 절차

코드는 바뀌지 않습니다. 바뀌는 것은 그 사이트의 자원 ID·주소·키이고, 전부 NFS 의 `.env` 한 장과 UI 환경변수 몇 줄에 모여 있습니다.

**0. 그 사이트 GenOS 에 먼저 있어야 하는 것**

| 자원 | 만드는 법 | 나오는 값 |
|---|---|---|
| VDB(Weaviate 컬렉션) | 자동적재(VT0001) + 수동 스키마 ON 에 `genos_vdb_schema_law.json`(`scripts/dump_vdb_schema.py`) 붙여넣기 | `LAW_COLLECTION`, `WEAVIATE_API_KEY`(쓰기 키) |
| 임베딩 모델서빙 | bge-m3 — 질의 쪽(law_agent)과 **같은 모델** | `EMBEDDING_API_URL`, `EMBEDDING_API_KEY` |
| 전처리기 코드서빙 | genos 전처리기 패키지 | `DOC_PARSER_BASE_URL`, `DOC_PARSER_API_KEY` |
| 코드스페이스 | NFS 볼륨이 같이 생김 → 그 안에 `law-inbox/` 폴더 | `LAW_ROOT`(파드 쪽 경로 `/nfs-root/DEV/volume/<id>/law-inbox`) |
| Temporal | 코드스페이스에서 `python3 -c "import socket;socket.create_connection(('temporal-frontend.temporal.svc.cluster.local',7233),3);print('OK')"` | 있으면 `LAW_SCHEDULER=temporal`, 없으면 `local` |

**1. 코드 준비(개발 PC)** — `scripts/make_codeserving_bundle.sh ../../_bundle_cs800` → `main.py + src/law_indexer + lib/`. 그 사이트 `template-code-serving` 의 파이썬이 3.12 가 아니면 스크립트의 `--python-version`·`--abi` 를 맞춰 다시 받습니다. zip 으로 묶고 20MB 를 넘으면 `split -b 19m`.

**2. 수신 코드서빙** — 관리자 UI 에서 코드서빙 신규(내부 Gitea 저장소가 같이 생김) → 코드스페이스 VS Code 탐색기에 번들 zip 을 끌어다 놓고 풀어 그 저장소 내용으로 통째 교체 → `git push`(Password 프롬프트에는 GenOS 토큰) → 리비전 추가(그 커밋, 이미지 `template-code-serving`, START/BUILD 비움, 복제본 1, HA 끔).

**3. 설정** — 코드스페이스에서 `law-inbox/.env` 작성(`.env.example` 을 그 사이트 값으로: `WEAVIATE_*`·`LAW_COLLECTION`·`EMBEDDING_*`·`DOC_PARSER_*`, `DOC_PARSER_SHARED_HOST_DIR`=`DOC_PARSER_SHARED_CONTAINER_DIR`=`<LAW_ROOT>/shared`), `mkdir -p law-inbox/shared && chmod 777 law-inbox/shared`. UI 환경변수:

```
LAW_ROOT=/nfs-root/DEV/volume/<id>/law-inbox
LAW_ENV_FILE=/nfs-root/DEV/volume/<id>/law-inbox/.env
LAW_OPS_TOKEN=<비밀값>
LAW_SCHEDULER=temporal            # Temporal 이 없는 사이트는 local
TEMPORAL_ADDRESS=temporal-frontend.temporal.svc.cluster.local:7233
TEMPORAL_NAMESPACE=llmops
```

배포 → 승인 → 가중치 100 → 이전 리비전 중지 → 상대 측에 줄 AuthKey 발급.

**4. 배포하면 자동으로 일어나는 것** — 파드가 뜨면 `.env` 를 읽고 → Temporal 에 붙어 → Schedule `law-inbox` 가 없으면 만들고(`LAW_SCHEDULE_CRON`, Asia/Seoul) → 이 파드가 워커로 큐 `law-embedding` 을 폴링 → 시작 직후 한 번 돌고 → `/health` 200. 확인: `GET …/api/v1/pipeline/status` 에 `mode: temporal` 과 `schedule.cron`, `/temporal` UI 에 `LawInboxWorkflow`.

**5. 배치가 도는 세 가지 길** — ① 상대 측 `POST /api/v1/law/ingest`(ZIP+manifest) → 저장 → Schedule trigger → 즉시 1회 ② cron ③ `POST /api/v1/pipeline/run`. 한 번 = 변환 activity → consume-zip activity(일시장애면 15분 뒤 재시도, 최대 5회) → 판정. 멈춤 사유 → 워크플로 Failed → Schedule 일시정지 → `status: halted` → 원인 조치 → `POST /api/v1/pipeline/resume`. 실패 ZIP 재투입 `POST /api/v1/pipeline/requeue?zip=…&stage=convert|consume`.

**6. 첫 데이터 = 초기 적재** — 초기 적재 전용 명령(`index`) 대신 이 경로로 넣습니다. 법령 몇 건짜리 batch 1개 → `status` 의 `quality`(버린 청크·압축 수)와 검색 결과 확인 → 나머지 batch(20MB 이하, manifest 동봉) → 법령 수·청크 수 대조. ZIP 단위로 격리·재시도·멱등이라 잘못된 ZIP 만 다시 넣으면 됩니다.

**7. 상대 측에 줄 것** — 엔드포인트 URL, AuthKey, 전송 API 가이드(manifest 같은 요청 동봉·20MB·재전송 규칙).

**8. 매일** — `status` 한 번 또는 `/temporal` 화면. `halted` 면 5번.

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
- `docs/genos_law_scope.md`: GenOS 법 목록 관리 화면 연동 상세
- `genos_vdb_schema_law.json`: 컬렉션 정의서(통합 1종)
