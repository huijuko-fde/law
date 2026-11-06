"""법령 ZIP 수신 + 자동 적재 — 코드서빙 800 의 앱 (LAW-39 수신기 + LAW-28 스케줄러).

엔드포인트
  GET  /health                     200. Temporal 워커 task 가 죽었으면 503(코드서빙이 파드를 다시 띄우게)
  GET  /api/v1/ping
  POST /api/v1/law/ingest          multipart: file(ZIP) + manifest(JSON). 같은 요청에 둘 다. ?kind=ops 는 X-Law-Ops-Token
  GET  /api/v1/files               수신 폴더 목록(확인용)
  GET  /api/v1/pipeline/status     스케줄 상태·마지막 실행·멈춤 여부
  GET  /api/v1/pipeline/runs       실행 기록 최근 N건
  POST /api/v1/pipeline/run        즉시 한 번 돌린다(X-Law-Ops-Token)
  POST /api/v1/pipeline/resume     멈춤 해제 + 즉시 한 번(X-Law-Ops-Token)
  POST /api/v1/pipeline/requeue    실패분 재투입 ?zip=<이름>&stage=convert|consume (X-Law-Ops-Token)

실행 주체(LAW_SCHEDULER)
  temporal  **기본 운영 모드.** 이 프로세스가 Temporal 워커를 겸하고(task queue INDEX_TASK_QUEUE), Schedule
            `law-inbox`(LAW_SCHEDULE_CRON) 가 LawInboxWorkflow 를 띄운다. 업로드가 오면 그 Schedule 을 trigger 해
            즉시 한 번 돈다. 실행 이력·결과·실패 사유는 Temporal 서버에 남는다(`/temporal` UI). 멈춤 = Schedule
            일시정지(pause_on_failure), 해제 = unpause + trigger. Temporal 에 못 붙으면 기동 실패.
  auto      TEMPORAL_HOST/TEMPORAL_ADDRESS 가 있으면 temporal, 못 붙으면 경고 후 local.
  local     Temporal 없는 환경(개발·검증) — 프로세스 안 러너(auto_ingest.Runner)가 이벤트+주기로 돈다.

수신 규약 — 상대 측에 안내한 "재전송하면 갱신" 과 맞춘다.
  · 같은 바이트(SHA-256)를 **같은 manifest 로** 다시 보내면 200 duplicate — 저장도 적재도 하지 않는다(멈춤이
    아니면 스케줄은 한 번 깨운다). manifest 만 고쳐(mst·dir 정정) 다시 보내면 새 수신이다.
  · 같은 이름이라도 내용이 다르면 새 수신으로 받는다. 수신 시각 접두 `YYYYMMDD-HHMMSS-ffffff_` 를 ZIP 과
    manifest 에 같이 붙이므로 처리 순서 = 도착 순서. 옛 버전이 최신을 덮는 것은 적재기의 버전 가드가
    막는다(package._older_than_stored).
  · manifest 가 없으면 422 — law 의 MST 가 manifest 에만 있어 변환할 수 없다. ZIP 은 rejected/ 에 남긴다.
  · ZIP 은 `.part` 로 받아 다 쓴 뒤 manifest 를 먼저 놓고 rename — 반쯤 쓴 파일을 집지 않는다.

env: LAW_ROOT(필수)  LAW_ENV_FILE(NFS 의 .env — 나머지 키를 여기서 읽는다, UI 값이 우선)
     LAW_SCHEDULER(auto|temporal|local)  LAW_SCHEDULE_CRON("0 * * * *", Asia/Seoul)  INDEX_TASK_QUEUE(law-embedding)
     TEMPORAL_HOST/TEMPORAL_PORT/TEMPORAL_NAMESPACE(genos 코드서빙이 주입) 또는 TEMPORAL_ADDRESS
     GENOS_LOGIN_ID/GENOS_LOGIN_PASSWORD(GenOS 관리자 계정 — gRPC 게이트(llmops-temporal-grpc-gateway:7233)를
     거칠 때 필수. 로그인해 받은 12시간짜리 JWT 를 만료 전에 갱신) 또는 TEMPORAL_AUTH_TOKEN(정적 JWT, 검증용)
     LAW_OPS_TOKEN(비우면 run/resume/requeue 막힘)  LAW_HALT_ON_PARTIAL(true)  LAW_RETENTION_DAYS(30, 0=영구)
     LAW_MANIFEST_GRACE_SEC(3600)  LAW_EXPECT_INTERVAL_HOURS(0=결손 감지 끔)  LAW_SWEEP_INTERVAL_SEC(600, local 전용)
베이스 규약: repo 루트 main.py 가 이 모듈의 app 을 re-export 하면 entrypoint 가 `uvicorn main:app` 으로 띄운다.
"""
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from .auto_ingest import (IngestPaths, Runner, _age_sec, attention, build_run_fn, inbox_status,
                          last_receipt, read_json)
from .auto_ingest import requeue as requeue_files

log = logging.getLogger("law_embedding.ingest")

ATTACH = (".hwp", ".hwpx", ".pdf", ".doc", ".docx", ".png", ".jpg", ".jpeg", ".gif")
_UNSAFE = re.compile(r"[\\/\x00-\x1f]")        # 경로 구분자·제어문자만 막는다(한글 이름은 그대로)


def _setup_logging() -> None:
    """uvicorn 은 root 에 핸들러를 달지 않아 이 앱의 INFO(`[law-ingest] accepted …`, sweep 결과)가 버려졌다 —
    폐쇄망에서 'ZIP 이 들어갔나' 를 로그로 볼 수 없었다. root 에 핸들러가 없을 때만 단다(단독 워커의
    basicConfig 와 겹치지 않는다). uvicorn 자체 로거는 propagate=False 라 영향 없다."""
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("law_embedding").setLevel(logging.INFO)


def _versions() -> dict:
    """기동 로그 한 줄 — 베이스 이미지의 파이썬·temporalio·protobuf 와 번들 lib/ 가 섞이는 구조라 실제로 어느
    것이 로딩됐는지 현장에서 바로 보게 한다(protobuf 런타임이 lib 의 gencode 보다 낮으면 weaviate 가 안 뜬다)."""
    import platform
    out = {"python": platform.python_version(), "machine": platform.machine()}
    for mod in ("temporalio", "weaviate", "google.protobuf", "grpc", "fastapi"):
        try:
            m = __import__(mod, fromlist=["__version__"])
            out[mod] = f"{getattr(m, '__version__', '?')} @ {Path(getattr(m, '__file__', '?')).parent}"
        except Exception as exc:                        # 없는 모듈도 한 줄로 — 기동 실패 원인 추적용
            out[mod] = f"import 실패: {type(exc).__name__}: {exc}"
    return out


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _stem(filename: Optional[str]) -> str:
    stem = _UNSAFE.sub("_", Path(filename or "").stem).strip(" .")
    return stem or "upload"


def _zip_facts(path: Path) -> dict:
    with zipfile.ZipFile(path) as zf:
        bad = zf.testzip()
        names = zf.namelist()
        tops = sorted({n.split("/")[0] for n in names if n.strip("/")})
        return {"valid": bad is None, "corrupt_entry": bad, "entries": len(names),
                "json": sum(1 for n in names if n.lower().endswith(".json")),
                "attachments": sum(1 for n in names if n.lower().endswith(ATTACH)),
                "top_level_count": len(tops), "top_level_sample": tops[:10]}


class Receiver:
    """수신 폴더에 ZIP+manifest 를 규약대로 놓는다. FastAPI 와 무관해 테스트는 이걸 직접 부른다."""

    def __init__(self, paths: IngestPaths):
        self.paths = paths

    @staticmethod
    def stamp() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")

    @staticmethod
    def manifest_sha(manifest_bytes: Optional[bytes]) -> Optional[str]:
        return hashlib.sha256(manifest_bytes).hexdigest() if manifest_bytes is not None else None

    def seen(self, sha256: str, manifest_sha256: Optional[str] = None) -> Optional[dict]:
        """같은 수신을 이미 받았는가(원장 received.jsonl) — ZIP 바이트와 manifest 가 **둘 다** 같을 때만.
        manifest 만 고쳐 다시 보낸 것(mst·dir 정정)은 새 수신이다 — 법령 MST 가 manifest 에만 있어 그걸 고친
        재전송이 duplicate 로 막히면 영영 처리되지 않는다. 거부된 수신은 세지 않는다."""
        try:
            lines = self.paths.ledger.read_text(encoding="utf-8").splitlines()
        except OSError:
            return None
        for line in lines:
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if entry.get("sha256") != sha256 or entry.get("status") != "accepted":
                continue
            if manifest_sha256 and entry.get("manifest_sha256") not in (None, manifest_sha256):
                continue                                 # 같은 ZIP, 다른 manifest — 새 수신
            return entry
        return None

    def _ledger(self, entry: dict) -> None:
        with open(self.paths.ledger, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def _reject(self, part: Path, stamp: str, stem: str, sha256: str, size: int, original: str,
                error: str, **extra) -> Tuple[int, dict]:
        """받은 바이트는 버리지 않는다 — rejected/ 에 두고 422."""
        self.paths.rejected.mkdir(parents=True, exist_ok=True)
        kept = self.paths.rejected / f"{stamp}_{stem}.zip"
        os.replace(part, kept)
        self._ledger({"stamp": stamp, "original": original, "stored": kept.name, "sha256": sha256,
                      "bytes": size, "received_at": _now(), "status": "rejected", "error": error})
        log.warning("[law-ingest] rejected %s: %s", original, error)
        return 422, {"received": False, "error": error, "sha256": sha256, "size_bytes": size,
                     "kept_as": kept.name, **extra}

    def accept(self, part: Path, stamp: str, stem: str, original: str, sha256: str, size: int,
               manifest_bytes: Optional[bytes]) -> Tuple[int, dict]:
        """다 받은 `.part` 를 검증해 수신 폴더에 확정한다. 반환: (HTTP 상태, 응답 본문)."""
        dup = self.seen(sha256, self.manifest_sha(manifest_bytes))
        if dup:
            part.unlink(missing_ok=True)
            return 200, {"received": True, "duplicate": True, "stored": False, "filename": original,
                         "sha256": sha256, "size_bytes": size, "received_at": _now(),
                         "stored_as": dup.get("stored"), "first_received_at": dup.get("received_at"),
                         "first_stored_as": dup.get("stored")}
        try:
            facts = _zip_facts(part)
        except zipfile.BadZipFile:
            return self._reject(part, stamp, stem, sha256, size, original, "ZIP 으로 열리지 않음",
                                zip={"valid": False})
        if not facts["valid"]:
            return self._reject(part, stamp, stem, sha256, size, original,
                                f"손상된 항목: {facts['corrupt_entry']}", zip=facts)
        if manifest_bytes is None:
            return self._reject(part, stamp, stem, sha256, size, original,
                                "manifest 가 없습니다 — 같은 요청의 'manifest' 필드로 함께 보내주세요", zip=facts)
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8-sig"))
            if not isinstance(manifest, dict):
                raise ValueError("JSON 객체가 아님")
        except ValueError as exc:
            return self._reject(part, stamp, stem, sha256, size, original, f"manifest JSON 오류: {exc}", zip=facts)
        if manifest.get("sha256") and manifest["sha256"] != sha256:
            return self._reject(part, stamp, stem, sha256, size, original,
                                "manifest 의 sha256 이 받은 바이트와 다릅니다 — 전송 중 손상, 다시 보내주세요",
                                zip=facts, manifest_sha256=manifest["sha256"])
        if manifest.get("bytes") and int(manifest["bytes"]) != size:
            return self._reject(part, stamp, stem, sha256, size, original,
                                "manifest 의 bytes 가 받은 크기와 다릅니다", zip=facts)
        stored = f"{stamp}_{stem}"
        manifest_path = self.paths.inbox / f"{stored}.manifest.json"
        tmp = manifest_path.with_name(f".{manifest_path.name}.tmp")
        tmp.write_bytes(manifest_bytes)
        os.replace(tmp, manifest_path)                      # manifest 먼저
        os.replace(part, self.paths.inbox / f"{stored}.zip")  # 그 다음 ZIP — 이 순간부터 처리 대상
        received_at = _now()
        # 투라인 운영 manifest 는 documents 가 건수가 아니라 문서 목록(파일 목록까지, 수십 KB)이다 — 원장엔 건수만.
        docs = manifest.get("docs") or (manifest["documents"] if isinstance(manifest.get("documents"), list) else [])
        doc_count = manifest.get("documents") if isinstance(manifest.get("documents"), int) else \
            (manifest.get("documentCount") if isinstance(manifest.get("documentCount"), int) else len(docs))
        self._ledger({"stamp": stamp, "original": original, "stored": f"{stored}.zip", "sha256": sha256,
                      "manifest_sha256": self.manifest_sha(manifest_bytes),
                      "bytes": size, "received_at": received_at, "status": "accepted",
                      "documents": doc_count})
        log.info("[law-ingest] accepted %s as %s.zip sha=%s bytes=%d docs=%s", original, stored, sha256[:12],
                 size, doc_count)
        return 200, {"received": True, "duplicate": False, "stored": True, "filename": original,
                     "stored_as": f"{stored}.zip", "size_bytes": size, "sha256": sha256,
                     "received_at": received_at, "zip": facts,
                     "manifest": {"documents": doc_count, "docs": len(docs)}}


# ── GenOS 관리자 토큰 — Temporal gRPC 게이트(ext_authz) 통과용 ──────────────────────────────
# GenOS 의 Temporal gRPC(:7233)는 frontend 직결이 아니라 앞단 Envoy 게이트(llmops-temporal-grpc-gateway)를
# 거치고, 게이트는 요청마다 `authorization: Bearer <GenOS 관리자 JWT>` 를 admin-api 로 검증한다
# (helm/temporal README 「접근 제어」). JWT 는 12시간짜리라 정적 토큰이면 워커가 반나절 뒤 끊긴다 —
# 관리자 계정으로 로그인해 받고 만료 30분 전에 다시 받는다. frontend 로 바로 붙으면 TCP 는 열리지만 첫 RPC 에서
# "connection closed" 로 끊긴다(2026-11-06 실측).
DEFAULT_ADMIN_API_URL = "http://llmops-admin-api-service:8080"      # law_scope 와 같은 내부 주소


def _exp_text(token: Optional[str]) -> Optional[str]:
    exp = _jwt_exp(token or "")
    return datetime.fromtimestamp(exp, timezone.utc).isoformat(timespec="minutes") if exp else None


def _jwt_exp(token: str) -> Optional[float]:
    """JWT payload 의 exp(epoch 초). 검증 없이 읽기만 — 갱신 시점을 잡는 용도."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except Exception:
        return None


class GenosLogin:
    """GenOS 관리자 로그인(POST /auth/login) → access_token."""

    def __init__(self, user_id: str, password: str, base: Optional[str] = None):
        self.user_id, self.password = user_id, password
        self.base = (base or os.getenv("LLMOPS_ADMIN_API_URL") or DEFAULT_ADMIN_API_URL).rstrip("/")

    async def fetch(self) -> str:
        import httpx
        async with httpx.AsyncClient(timeout=30) as http:
            r = await http.post(f"{self.base}/auth/login", json={"user_id": self.user_id, "password": self.password})
        if r.status_code != 200:
            raise RuntimeError(f"GenOS 로그인 실패 HTTP {r.status_code} ({self.base}/auth/login, id={self.user_id}): {r.text[:200]}")
        token = ((r.json() or {}).get("data") or {}).get("access_token")
        if not token:
            raise RuntimeError(f"GenOS 로그인 응답에 access_token 이 없습니다: {r.text[:200]}")
        return token


# ── 스케줄러 — 같은 인터페이스 둘: Temporal(운영) / 로컬 러너(Temporal 없는 환경) ───────────────

class LocalScheduler:
    """프로세스 안 러너(auto_ingest.Runner). 멈춤 = HALT.json."""

    mode = "local"

    def __init__(self, paths: IngestPaths, runner: Runner):
        self.paths, self.runner = paths, runner

    async def start(self) -> None:
        self.runner.start()
        self.runner.trigger("startup")                  # 재배포·재기동 사이에 도착한 분을 이어 받는다

    async def stop(self) -> None:
        self.runner.stop()

    async def halted(self) -> bool:
        return self.runner.halted() is not None

    async def trigger(self, reason: str) -> bool:
        if self.runner.halted():
            return False
        self.runner.trigger(reason)
        return True

    async def status(self) -> dict:
        return self.runner.status()

    async def runs(self, limit: int) -> List[dict]:
        files = sorted((p for p in self.paths.runs.glob("*.json") if p.name != "latest.json"), reverse=True)
        return [r for r in (read_json(p) for p in files[:limit]) if r]

    async def resume(self) -> bool:
        return self.runner.resume(by="ops")

    async def requeue(self, name: str, stage: str) -> List[str]:
        return self.runner.requeue(name, stage)


class TemporalScheduler:
    """Temporal 워커 + Schedule. 멈춤 = Schedule 일시정지(pause_on_failure)."""

    mode = "temporal"

    def __init__(self, paths: IngestPaths, *, address: str, namespace: str, task_queue: str, cron: str,
                 req, workers: int = 1, tls: bool = False, grace_sec: int = 3600, expect_hours: float = 0,
                 auth_token: Optional[str] = None, login: Optional[GenosLogin] = None):
        self.paths, self.address, self.namespace, self.task_queue = paths, address, namespace, task_queue
        self.cron, self.req, self.workers, self.tls = cron, req, workers, tls
        self.grace_sec, self.expect_hours = grace_sec, expect_hours
        self.auth_token, self.login = auth_token, login
        self.client = self.worker = self.executor = self._task = self._refresh = None
        self.worker_error: Optional[str] = None
        self.create_namespace = _flag("LAW_TEMPORAL_CREATE_NAMESPACE", True)

    def _metadata(self) -> dict:
        return {"authorization": f"Bearer {self.auth_token}"} if self.auth_token else {}

    async def start(self) -> None:
        from temporalio.client import Client
        from . import temporal_schedule as TS
        from .worker import build_worker
        if self.login is not None:
            self.auth_token = await self.login.fetch()
            log.info("[law-ingest] genos login ok (id=%s, token exp=%s)", self.login.user_id, _exp_text(self.auth_token))
        self.client = await Client.connect(self.address, namespace=self.namespace, tls=self.tls,
                                           rpc_metadata=self._metadata())
        if self.create_namespace:
            await self._ensure_namespace()
        await TS.ensure_schedule(self.client, task_queue=self.task_queue, req=self.req, cron=self.cron)
        self.executor = ThreadPoolExecutor(max_workers=self.workers)
        self.worker = build_worker(self.client, self.task_queue, self.executor, self.workers)
        self._task = asyncio.create_task(self.worker.run(), name="law-temporal-worker")
        self._task.add_done_callback(self._worker_done)
        if self.login is not None:
            self._refresh = asyncio.create_task(self._refresh_token_loop(), name="law-genos-token")
        log.info("[law-ingest] temporal worker up: %s ns=%s queue=%s cron=%r auth=%s",
                 self.address, self.namespace, self.task_queue, self.cron, self.auth_kind)
        await self.trigger("startup")

    @property
    def auth_kind(self) -> Optional[str]:
        return "login" if self.login is not None else ("token" if self.auth_token else None)

    async def _refresh_token_loop(self) -> None:
        """만료 30분 전에 다시 로그인해 클라이언트(워커가 같이 쓴다)의 메타데이터를 바꾼다. 실패하면 5분 뒤 다시."""
        while True:
            exp = _jwt_exp(self.auth_token or "")
            await asyncio.sleep(max(300, exp - time.time() - 1800) if exp else 6 * 3600)
            try:
                self.auth_token = await self.login.fetch()
                self.client.rpc_metadata = self._metadata()
                log.info("[law-ingest] genos token refreshed (exp=%s)", _exp_text(self.auth_token))
            except Exception as exc:
                log.error("[law-ingest] genos token refresh failed: %s — 5분 뒤 다시", exc)

    async def _ensure_namespace(self) -> None:
        """네임스페이스가 없으면 만든다 — Temporal UI 로는 못 만들고 CLI 도 없는 사이트가 많다(새 사이트 첫 기동).
        어떤 이유로든 실패하면 기동은 계속한다 — 그 뒤 create_schedule 이 진짜 오류를 낸다."""
        from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest, RegisterNamespaceRequest
        from temporalio.service import RPCError, RPCStatusCode
        svc = self.client.workflow_service
        try:
            await svc.describe_namespace(DescribeNamespaceRequest(namespace=self.namespace))
            return
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                log.warning("[law-ingest] describe_namespace(%s) 실패: %s", self.namespace, exc)
                return
        except Exception as exc:
            log.warning("[law-ingest] describe_namespace(%s) 실패: %s", self.namespace, exc)
            return
        try:
            from google.protobuf.duration_pb2 import Duration
            await svc.register_namespace(RegisterNamespaceRequest(
                namespace=self.namespace, description="법령 적재기(law_embedding) 수신 파이프라인",
                workflow_execution_retention_period=Duration(seconds=30 * 86400)))
        except Exception as exc:
            log.error("[law-ingest] 네임스페이스 %s 생성 실패: %s", self.namespace, exc)
            return
        log.warning("[law-ingest] 네임스페이스 %s 를 새로 만들었다(보존 30일) — 반영 대기", self.namespace)
        for _ in range(30):                              # 서버가 네임스페이스 캐시를 갱신할 때까지(보통 10초 안)
            try:
                await svc.describe_namespace(DescribeNamespaceRequest(namespace=self.namespace))
                await asyncio.sleep(10)
                return
            except RPCError:
                await asyncio.sleep(1)

    def _worker_done(self, task: "asyncio.Task") -> None:
        """워커 task 는 조용히 죽을 수 있다(SDK 가 로그만 남기고 예외를 task 에 담는다). 그러면 Schedule 은
        계속 워크플로를 띄우는데 아무도 집지 않아 첫 실행이 Running 에 고착되고 overlap=SKIP 이 뒤를 전부
        건너뛴다. 기록해 두고 /health 가 503 을 내게 한다."""
        if task.cancelled():
            self.worker_error = "cancelled"
            return
        exc = task.exception()
        self.worker_error = f"{type(exc).__name__}: {exc}" if exc else "stopped"
        if exc:
            log.error("[law-ingest] temporal worker died: %s — /health 503, 파드 재기동이 필요하다", self.worker_error)

    def healthy(self) -> bool:
        return self._task is not None and not self._task.done()

    async def stop(self) -> None:
        if self._refresh is not None:
            self._refresh.cancel()
        if self.worker is not None:
            await self.worker.shutdown()
        if self.executor is not None:
            self.executor.shutdown(wait=False)

    async def halted(self) -> bool:
        from . import temporal_schedule as TS
        return (await TS.describe(self.client))["paused"]

    async def trigger(self, reason: str) -> bool:
        from . import temporal_schedule as TS
        if await self.halted():
            log.info("[law-ingest] schedule paused — not triggering (%s)", reason)
            return False
        await TS.trigger(self.client)
        return True

    async def status(self) -> dict:
        from . import temporal_schedule as TS
        worker_alive = self.healthy()
        notes = attention(self.paths, self.grace_sec, self.expect_hours)
        if not worker_alive:
            notes.append(f"worker_not_running:{self.worker_error or 'not started'}")
        received = last_receipt(self.paths)
        base = {
            "mode": "temporal",
            "attention": notes,
            "last_received_at": received,
            "hours_since_last_receipt": round(_age_sec(received) / 3600, 1) if received else None,
            **inbox_status(self.paths),
            "temporal": {"address": self.address, "namespace": self.namespace, "task_queue": self.task_queue,
                         "worker_alive": worker_alive, "worker_error": self.worker_error,
                         "auth": self.auth_kind, "token_exp": _exp_text(self.auth_token) if self.auth_token else None},
        }
        try:
            d = await TS.describe(self.client)
            last = await TS.last_run(self.client)
        except Exception as exc:                        # Temporal 이 잠깐 안 보여도 폴더 기준 상태는 돌려준다
            base["temporal"]["error"] = f"{type(exc).__name__}: {exc}"
            return {**base, "state": "unknown", "halt": None, "schedule": None, "last_run": None}
        return {
            **base,
            "state": "halted" if d["paused"] else ("running" if d["running"] else "idle"),
            "halt": {"note": d["note"], "last_action_at": d["last_action_at"]} if d["paused"] else None,
            "schedule": d,
            "last_run": last,
        }

    async def runs(self, limit: int) -> List[dict]:
        from . import temporal_schedule as TS
        return await TS.recent_runs(self.client, limit)

    async def resume(self) -> bool:
        from . import temporal_schedule as TS
        if not await self.halted():
            return False
        await TS.unpause(self.client, note=f"resume by ops {_now()}")
        await TS.trigger(self.client)
        return True

    async def requeue(self, name: str, stage: str) -> List[str]:
        moved = requeue_files(self.paths, name, stage)
        await self.trigger("requeue")
        return moved


# ── 환경에서 조립 ──────────────────────────────────────────────────────────

def load_env_file(path: Optional[str]) -> int:
    """LAW_ENV_FILE 이 가리키는 .env 를 환경에 올린다(이미 있는 키는 덮지 않는다 — UI 환경변수가 우선).

    코드서빙은 저장소의 .env 를 읽지 않고(비밀값을 git 에 두지 않는다) 환경변수는 관리자 UI 에 한 줄씩
    넣어야 한다. 운영 .env(20여 줄)를 NFS 에 두고 이 키 하나로 가리키면 UI 에는 LAW_ROOT·LAW_OPS_TOKEN·
    LAW_ENV_FILE 셋만 넣으면 된다. 반환: 새로 올린 키 수."""
    if not path:
        return 0
    from dotenv import dotenv_values
    added = 0
    for key, value in (dotenv_values(path) or {}).items():
        if value is not None and key not in os.environ:
            os.environ[key] = value
            added += 1
    log.info("[law-ingest] env file %s: %d keys loaded", path, added)
    return added


def _flag(name: str, default: bool) -> bool:
    return os.getenv(name, "true" if default else "false").lower() not in ("0", "false", "no")


class _Prefs:
    def __init__(self):
        self.mode = (os.getenv("LAW_SCHEDULER") or "auto").lower()
        self.has_temporal = bool(os.getenv("TEMPORAL_ADDRESS") or os.getenv("TEMPORAL_HOST"))
        self.halt_on_partial = _flag("LAW_HALT_ON_PARTIAL", True)
        self.retention_days = int(os.getenv("LAW_RETENTION_DAYS", "30"))
        self.grace_sec = int(os.getenv("LAW_MANIFEST_GRACE_SEC", "3600"))
        self.expect_hours = float(os.getenv("LAW_EXPECT_INTERVAL_HOURS", "0"))
        self.interval_sec = int(os.getenv("LAW_SWEEP_INTERVAL_SEC", "600"))

    @property
    def want_temporal(self) -> bool:
        return self.mode == "temporal" or (self.mode == "auto" and self.has_temporal)

    @property
    def strict_temporal(self) -> bool:
        return self.mode == "temporal"


def _from_env():
    """env → (paths, settings, prefs). 스케줄러 조립은 startup 에서(Temporal 접속이 비동기라)."""
    env_file = os.getenv("LAW_ENV_FILE")
    if env_file and not Path(env_file).is_file():
        # 조용히 0키로 넘어가면 엉뚱한 지점(Weaviate 접속)에서 죽어 원인을 못 찾는다 — 여기서 바로 멈춘다.
        raise RuntimeError(f"LAW_ENV_FILE 이 가리키는 파일이 없습니다: {env_file} — NFS 경로와 볼륨 번호를 확인하세요")
    load_env_file(env_file)
    from .config import Settings
    root = os.getenv("LAW_ROOT")
    if not root:
        raise RuntimeError("LAW_ROOT 가 비어 있습니다 — 수신·처리 폴더(NFS)를 지정하세요")
    return IngestPaths(Path(root)).ensure(), Settings.from_env(), _Prefs()


def _temporal_scheduler(paths: IngestPaths, prefs: _Prefs) -> TemporalScheduler:
    from .worker import temporal_address
    from .worker_workflows import LawInboxRequest
    req = LawInboxRequest(root=str(paths.root), halt_on_partial=prefs.halt_on_partial,
                          manifest_grace_sec=prefs.grace_sec, retention_days=prefs.retention_days)
    return TemporalScheduler(
        paths, address=temporal_address(), namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
        task_queue=os.getenv("INDEX_TASK_QUEUE", "law-embedding"),
        cron=os.getenv("LAW_SCHEDULE_CRON", "0 * * * *"), req=req,
        workers=int(os.getenv("INDEX_ACTIVITY_WORKERS", "1")), tls=_flag("TEMPORAL_TLS", False),
        grace_sec=prefs.grace_sec, expect_hours=prefs.expect_hours,
        auth_token=(os.getenv("TEMPORAL_AUTH_TOKEN") or "").strip() or None,
        login=(GenosLogin(os.environ["GENOS_LOGIN_ID"].strip(), os.environ["GENOS_LOGIN_PASSWORD"])
               if os.getenv("GENOS_LOGIN_ID") and os.getenv("GENOS_LOGIN_PASSWORD") else None))


def _local_scheduler(paths: IngestPaths, settings, prefs: _Prefs) -> LocalScheduler:
    from .embedder import make_embedder
    run_fn = build_run_fn(paths, make_embedder(settings), settings, halt_on_partial=prefs.halt_on_partial)
    runner = Runner(paths, run_fn, interval_sec=prefs.interval_sec, retention_days=prefs.retention_days,
                    manifest_grace_sec=prefs.grace_sec, expect_interval_hours=prefs.expect_hours)
    return LocalScheduler(paths, runner)


async def _start_scheduler(paths: IngestPaths, settings, prefs: _Prefs):
    """Temporal 우선. strict(LAW_SCHEDULER=temporal)면 못 붙을 때 기동 실패, auto 면 경고 후 로컬."""
    from .embedder import make_embedder
    try:
        make_embedder(settings)                          # 모델·설정 오류를 기동 시점에 드러낸다
    except ModuleNotFoundError as exc:                   # .env.example 기본 EMBEDDING_BACKEND=local 을 그대로 둔 경우
        raise RuntimeError(
            f"임베딩 백엔드 '{settings.embedding_backend}' 를 쓸 수 없습니다({exc}) — 코드서빙에는 sentence-transformers 가 "
            "없으니 .env 에 EMBEDDING_BACKEND=remote 와 EMBEDDING_API_URL/EMBEDDING_API_KEY 를 두세요") from exc
    if prefs.want_temporal:
        sched = _temporal_scheduler(paths, prefs)
        try:
            await sched.start()
            return sched
        except Exception as exc:
            if prefs.strict_temporal:
                raise RuntimeError(f"Temporal 에 붙지 못했습니다({sched.address}, ns={sched.namespace}): {exc}") from exc
            log.error("[law-ingest] temporal unreachable (%s) — falling back to local runner", exc)
    sched = _local_scheduler(paths, settings, prefs)
    await sched.start()
    return sched


def create_app(paths: Optional[IngestPaths] = None, scheduler=None) -> FastAPI:
    """paths/scheduler 를 주면 그대로(테스트), 없으면 startup 때 env 로 만든다."""
    state = {"paths": paths, "sched": scheduler}
    _setup_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("[law-ingest] versions %s", _versions())
        if state["sched"] is None:
            paths_, settings, prefs = _from_env()
            state["paths"] = paths_
            state["sched"] = await _start_scheduler(paths_, settings, prefs)
        else:
            await state["sched"].start()
        log.info("[law-ingest] scheduler=%s root=%s", state["sched"].mode, state["paths"].root)
        try:
            yield
        finally:
            await state["sched"].stop()

    app = FastAPI(title="law-ingest", lifespan=lifespan)

    def sched():
        return state["sched"]

    def paths_() -> IngestPaths:
        return state["paths"]

    async def snapshot() -> dict:
        try:
            return {"mode": sched().mode, "halted": await sched().halted()}
        except Exception as exc:                        # Temporal 이 잠깐 안 보여도 수신 응답은 깨지지 않는다
            return {"mode": sched().mode, "halted": None, "error": f"{type(exc).__name__}: {exc}"}

    async def nudge(reason: str, body: dict) -> None:
        """저장이 끝난 뒤의 trigger — 실패해도 수신은 성공이다(파일은 이미 있고 다음 주기·재기동이 집어간다)."""
        try:
            body["triggered"] = await sched().trigger(reason)
        except Exception as exc:
            body["triggered"] = False
            body["trigger_error"] = f"{type(exc).__name__}: {exc}"
            log.error("[law-ingest] trigger(%s) failed after storing %s: %s", reason, body.get("stored_as"), exc)

    def ops(token: Optional[str]) -> None:
        expected = os.getenv("LAW_OPS_TOKEN")
        if not expected or not token or not hmac.compare_digest(token, expected):
            raise HTTPException(status_code=403, detail="X-Law-Ops-Token 이 필요합니다")

    @app.get("/health")
    async def health():
        s = sched()
        if s is not None and hasattr(s, "healthy") and not s.healthy():
            return JSONResponse({"status": "degraded", "error": getattr(s, "worker_error", None)}, status_code=503)
        return {"status": "ok"}

    @app.get("/api/v1/ping")
    async def ping():
        return {"pong": True, "ts": _now()}

    @app.get("/api/v1/files")
    async def files():
        inbox = paths_().inbox
        items = [{"name": p.name, "size": p.stat().st_size} for p in sorted(inbox.iterdir()) if p.is_file()]
        return {"inbox": str(inbox), "files": items}

    @app.post("/api/v1/law/ingest")
    async def ingest(file: UploadFile = File(...), manifest: Optional[UploadFile] = File(None),
                     kind: str = "law", x_law_ops_token: Optional[str] = Header(default=None)):
        receiver = Receiver(paths_())
        stamp, stem = receiver.stamp(), _stem(file.filename)
        if kind == "ops":                                   # 운영용 전달(배포 번들·lib) — 변환 대상이 아니다
            ops(x_law_ops_token)                            # 게이트웨이 키(상대 측)만으로는 ops/ 에 못 쓴다
            name = f"{stamp}_{_UNSAFE.sub('_', Path(file.filename or 'upload').name)}"
            part = paths_().ops / f".{name}.part"
        else:
            part = paths_().inbox / f".{stamp}_{stem}.zip.part"
        sha, size = hashlib.sha256(), 0
        try:
            with open(part, "wb") as out:                   # 통째로 메모리에 올리지 않는다
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    sha.update(chunk)
                    size += len(chunk)
                    out.write(chunk)
        except OSError as exc:                              # 비root UID 의 NFS 쓰기 권한 문제가 여기서 드러난다
            part.unlink(missing_ok=True)
            return JSONResponse({"received": False, "error": f"저장 실패: {exc}"}, status_code=500)
        if kind == "ops":
            final = paths_().ops / name
            os.replace(part, final)
            log.info("[law-ingest] ops upload %s bytes=%d sha=%s", final.name, size, sha.hexdigest()[:12])
            return {"received": True, "kind": "ops", "stored_as": final.name, "size_bytes": size,
                    "sha256": sha.hexdigest(), "received_at": _now()}
        manifest_bytes = await manifest.read() if manifest is not None else None
        try:
            status, body = receiver.accept(part, stamp, stem, file.filename or "upload.zip",
                                           sha.hexdigest(), size, manifest_bytes)
        except Exception as exc:                            # 암호화 ZIP·깨진 deflate·bytes 비숫자 등 — 500 과 .part 고아 대신 422
            log.warning("[law-ingest] accept failed for %s: %s", file.filename, exc)
            status, body = receiver._reject(part, stamp, stem, sha.hexdigest(), size, file.filename or "upload.zip",
                                            f"수신 검증 실패: {type(exc).__name__}: {exc}")
        if body.get("stored") or body.get("duplicate"):     # duplicate 도 깨운다 — 앞선 수신의 trigger 가 실패했을 수 있다
            await nudge("ingest", body)
        body["pipeline"] = await snapshot()
        return JSONResponse(body, status_code=status)

    @app.get("/api/v1/pipeline/status")
    async def pipeline_status():
        return await sched().status()

    @app.get("/api/v1/pipeline/runs")
    async def pipeline_runs(limit: int = 20):
        return {"runs": await sched().runs(max(1, min(limit, 200)))}

    @app.post("/api/v1/pipeline/run")
    async def pipeline_run(x_law_ops_token: Optional[str] = Header(default=None)):
        ops(x_law_ops_token)
        triggered = await sched().trigger("manual")
        return JSONResponse({"triggered": triggered, "pipeline": await snapshot()}, status_code=202)

    @app.post("/api/v1/pipeline/resume")
    async def pipeline_resume(x_law_ops_token: Optional[str] = Header(default=None)):
        ops(x_law_ops_token)
        cleared = await sched().resume()
        return {"cleared": cleared, "pipeline": await snapshot()}

    @app.post("/api/v1/pipeline/requeue")
    async def pipeline_requeue(zip: str, stage: str = "convert",
                               x_law_ops_token: Optional[str] = Header(default=None)):
        """실패분을 다시 줄에 세운다(?zip=<이름>&stage=convert|consume). 멈춤 중이면 resume 도 해야 돈다."""
        ops(x_law_ops_token)
        try:
            moved = await sched().requeue(zip, stage)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        return {"moved": moved, "pipeline": await snapshot()}

    return app


app = create_app()
