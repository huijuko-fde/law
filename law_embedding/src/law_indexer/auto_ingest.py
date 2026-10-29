"""수신 폴더 → 변환 → consume-zip 을 사람 없이 돌리는 러너 (LAW-28 1단계).

코드서빙 800 파드 하나 안에서 돈다(ingest_app 이 띄운다). 수신 엔드포인트가 ZIP 을 완성하면
trigger() 로 깨우고, 아니어도 interval 마다 한 번 돈다(잃은 이벤트·재시도 대기 이어가기). 한 번에
한 실행(스레드 하나). 실패·품질 가드가 나면 HALT.json 을 쓰고 멈춘다 — 사람이 풀기 전엔 ZIP 을 받아
쌓아 두기만 하고 처리하지 않는다. 순서 규칙("나중 ZIP 이 최종") 때문에 실패 ZIP 을 두고 뒤를 계속
적재하면 재처리 때 옛 내용이 최신을 덮는다.

run_once 는 변환·소비 두 단계를 함수 인자로 받는 순수한 조립이라 Temporal(2단계)로 옮길 때 그대로
activity 본체가 된다. 이 모듈은 표준 라이브러리만 쓴다(적재기 모듈은 build_run_fn 안에서만 import).

폴더 (LAW_ROOT 아래):
  *.zip + *.manifest.json      수신분(수신기가 manifest 먼저, ZIP 은 .part → rename)
  done/  failed/               변환이 끝난·실패한 원본 쌍(+ .convert-report.json)
  converted/                   consume-zip 폴더(그 밑 state/ done/ failed/ duplicate/ packages/)
  runs/<id>.json  runs/latest.json   실행 기록
  HALT.json                    멈춤 표시(사유·실행 id). 있으면 돌지 않는다
  lease.json                   실행 중인 인스턴스 — 다른 파드(구 리비전 등)가 같은 폴더를 돌리는 것을 막는다
"""
import json
import logging
import os
import shutil
import socket
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from .ingest_rules import HALT, OK, WAITING, decide, trim_report  # noqa: F401 — 판정 규칙은 ingest_rules 에

log = logging.getLogger("law_embedding.ingest")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _age_sec(stamp: Optional[str]) -> float:
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds()
    except (TypeError, ValueError):
        return float("inf")


def write_json(path: Path, data: dict) -> None:
    """tmp → replace. 반쯤 쓴 기록을 다른 쪽(status 조회·코드스페이스)이 읽지 않게."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


@dataclass(frozen=True)
class IngestPaths:
    root: Path

    @property
    def inbox(self) -> Path:
        return self.root

    @property
    def done(self) -> Path:
        return self.root / "done"

    @property
    def failed(self) -> Path:
        return self.root / "failed"

    @property
    def rejected(self) -> Path:
        return self.root / "rejected"

    @property
    def ops(self) -> Path:
        return self.root / "ops"           # 운영용 업로드(번들·lib) — 변환 대상이 아니다

    @property
    def converted(self) -> Path:
        return self.root / "converted"

    @property
    def runs(self) -> Path:
        return self.root / "runs"

    @property
    def latest(self) -> Path:
        return self.runs / "latest.json"

    @property
    def halt(self) -> Path:
        return self.root / "HALT.json"

    @property
    def lease(self) -> Path:
        return self.root / "lease.json"

    @property
    def ledger(self) -> Path:
        return self.root / "received.jsonl"

    def ensure(self) -> "IngestPaths":
        """폴더를 만들고 0777 로 연다 — 파드(UID 3000)와 코드스페이스 사용자가 다른 UID 로 같은 NFS 를 쓴다.
        사람이 실패분을 손으로 옮길 수 있어야 하고(README 재처리), 10/20 shared/ 와 같은 조치다."""
        for p in (self.root, self.done, self.failed, self.rejected, self.ops, self.converted, self.runs):
            p.mkdir(parents=True, exist_ok=True)
            try:
                p.chmod(0o777)
            except OSError:
                pass
        return self

    def pending_zips(self) -> List[Path]:
        return sorted(p for p in self.inbox.glob("*.zip") if p.is_file() and not p.name.startswith("."))

    def manifest_of(self, zip_path: Path) -> Path:
        return zip_path.with_name(zip_path.stem + ".manifest.json")

    def archives(self) -> List[Path]:
        """보존 기간이 지나면 지워도 되는 폴더들 — 적재가 끝난 산출물과 거부·운영 업로드."""
        return [self.done, self.rejected, self.ops, self.converted / "done", self.converted / "duplicate",
                self.converted / "packages" / "processed"]


def requeue(paths: IngestPaths, name: str, stage: str) -> List[str]:
    """실패분을 다시 줄에 세운다 — 파드가 옮기므로 사람이 파드 소유 폴더를 만질 필요가 없다.

    stage="convert": failed/<name>(+manifest) → 수신 폴더(변환부터 다시; 보고 파일은 지운다)
    stage="consume": converted/failed/<name>(+manifest) → converted/(consume-zip 이 처음부터 다시;
                     실패 이력은 막지 않는다 — zip_consume 참고)
    반환: 옮긴 파일 이름. 없으면 FileNotFoundError, stage 가 틀리면 ValueError."""
    name = Path(name).name
    if stage == "convert":
        src_dir, dst_dir = paths.failed, paths.inbox
    elif stage == "consume":
        src_dir, dst_dir = paths.converted / "failed", paths.converted
    else:
        raise ValueError(f"stage 는 convert|consume: {stage}")
    zip_path = src_dir / name
    if not zip_path.is_file() or not name.endswith(".zip"):
        raise FileNotFoundError(f"{stage} 실패분에 {name} 이 없습니다")
    moved = []
    for p in (zip_path, paths.manifest_of(zip_path)):
        if p.exists():
            os.replace(p, dst_dir / p.name)
            moved.append(p.name)
    report = src_dir / f"{zip_path.stem}.convert-report.json"
    if report.exists():
        report.unlink()
    log.info("[law-ingest] requeue stage=%s %s", stage, moved)
    return moved


def manifest_overdue(paths: IngestPaths, grace_sec: int, now: Optional[float] = None) -> List[str]:
    """manifest 없이 유예보다 오래 놓인 ZIP — 수신기는 manifest 를 먼저 두므로 자동 경로엔 없고, 사람이 손으로
    넣었거나 옛 수신기 시절 잔존물이다. 조용히 영원히 대기하지 않게 멈춤 사유로 올린다."""
    cutoff = (now or time.time()) - grace_sec
    return [p.name for p in paths.pending_zips()
            if not paths.manifest_of(p).exists() and p.stat().st_mtime < cutoff]


def last_receipt(paths: IngestPaths) -> Optional[str]:
    """원장(received.jsonl)의 마지막 accepted 수신 시각."""
    try:
        lines = paths.ledger.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get("status") == "accepted":
            return entry.get("received_at")
    return None


def attention(paths: IngestPaths, grace_sec: int, expect_hours: float) -> List[str]:
    """멈추지는 않지만 사람이 봐야 할 것 — 수신 결손(기대 간격 초과)·manifest 대기 초과."""
    notes = []
    last = last_receipt(paths)
    if expect_hours > 0 and last and _age_sec(last) > expect_hours * 3600:
        notes.append(f"no_receipt_for_{int(_age_sec(last) // 3600)}h")
    for name in manifest_overdue(paths, grace_sec):
        notes.append(f"manifest_missing:{name}")
    return notes


def inbox_status(paths: IngestPaths) -> dict:
    """폴더 기준 상태 — 스케줄러(로컬 러너·Temporal)와 무관하게 같은 숫자를 보여준다."""
    pending = paths.pending_zips()
    return {
        "inbox": {"pending": len(pending),
                  "pending_manifest": [p.name for p in pending if not paths.manifest_of(p).exists()]},
        "failed": {"convert": len(list(paths.failed.glob("*.zip"))),
                   "consume": len(list((paths.converted / "failed").glob("*.zip")))},
        "disk": disk(paths),
    }


def run_once(paths: IngestPaths, convert: Callable, consume: Callable, *, trigger: str = "manual",
             halt_on_partial: bool = True) -> dict:
    """변환 → 소비 → 판정 한 번. 예외도 기록(halt)으로 바꾼다 — 러너 스레드가 죽지 않는다.

    convert(inbox, outbox, done_dir=, failed_dir=) -> 보고 list (inbox_convert.convert_inbox 모양)
    consume(folder) -> 요약 dict (zip_consume.consume_zip_folder 모양)"""
    record = {"trigger": trigger, "started_at": _now()}
    try:
        reports = convert(paths.inbox, paths.converted, done_dir=paths.done, failed_dir=paths.failed)
        summary = consume(paths.converted)
        verdict, reasons = decide(reports, summary, halt_on_partial=halt_on_partial)
        record.update(convert=[trim_report(r) for r in reports], consume=summary, verdict=verdict, reasons=reasons)
    except Exception as exc:
        record.update(verdict=HALT, reasons=[f"exception:{type(exc).__name__}: {exc}"],
                      traceback=traceback.format_exc())
    record["finished_at"] = _now()
    return record


def build_run_fn(paths: IngestPaths, embedder, settings, *, halt_on_partial: bool = True,
                 allow_older_version: bool = False) -> Callable[[str], dict]:
    """실제 변환기·소비자를 묶은 run_fn(trigger) -> record. 적재 범위(law_scope)는 실행마다 새로 읽는다."""
    from .inbox_convert import convert_inbox
    from .law_scope import load_law_scope
    from .zip_consume import consume_zip_folder

    def consume(folder: Path) -> dict:
        return consume_zip_folder(embedder, settings, folder, scope=load_law_scope(),
                                  allow_older_version=allow_older_version)

    return lambda trigger: run_once(paths, convert_inbox, consume, trigger=trigger,
                                    halt_on_partial=halt_on_partial)


def retire(paths: IngestPaths, retention_days: int, now: Optional[float] = None) -> dict:
    """보존 기간이 지난 산출물을 지운다 — 공유 NFS(모든 코드서빙·RAG 문서가 같은 PVC)를 법령 파이프라인이
    채우지 않게. 0 이면 지우지 않는다. 실행 기록(runs/)도 같은 기간으로 정리한다(latest.json 은 남긴다)."""
    removed = {"files": 0, "bytes": 0}
    if retention_days <= 0:
        return removed
    cutoff = (now or time.time()) - retention_days * 86400
    targets = [p for d in paths.archives() if d.is_dir() for p in d.iterdir() if p.is_file()]
    targets += [p for p in paths.runs.glob("*.json") if p.name != "latest.json"]
    for p in targets:
        try:
            st = p.stat()
            if st.st_mtime < cutoff:
                p.unlink()
                removed["files"] += 1
                removed["bytes"] += st.st_size
        except OSError:
            continue
    return removed


def disk(paths: IngestPaths) -> dict:
    try:
        usage = shutil.disk_usage(paths.root)
        return {"total_gb": round(usage.total / 1e9, 1), "free_gb": round(usage.free / 1e9, 1),
                "used_pct": round(100 * (usage.total - usage.free) / usage.total, 1) if usage.total else None}
    except OSError:
        return {}


class Runner:
    """실행 스레드 하나. trigger() 또는 interval 로 깨어나 sweep 한다. HALT 가 있으면 돌지 않는다."""

    def __init__(self, paths: IngestPaths, run_fn: Callable[[str], dict], *, interval_sec: int = 600,
                 instance: Optional[str] = None, lease_ttl_sec: int = 900, retention_days: int = 30,
                 manifest_grace_sec: int = 3600, expect_interval_hours: float = 0):
        self.paths, self.run_fn = paths, run_fn
        self.interval, self.lease_ttl = interval_sec, lease_ttl_sec
        self.retention_days, self.manifest_grace, self.expect_hours = retention_days, manifest_grace_sec, expect_interval_hours
        self.instance = instance or f"{socket.gethostname()}:{os.getpid()}"
        self._wake, self._stop = threading.Event(), threading.Event()
        self._reason: Optional[str] = None
        self._thread: Optional[threading.Thread] = None
        self.running = False
        self.last_sweep_at: Optional[str] = None

    # ── 제어 ──
    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="law-ingest-runner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def trigger(self, reason: str) -> None:
        self._reason = reason
        self._wake.set()

    def halted(self) -> Optional[dict]:
        return read_json(self.paths.halt)

    def resume(self, by: str = "ops") -> bool:
        """HALT 를 풀고 바로 한 번 돈다. 풀 것이 없었으면 False."""
        if not self.paths.halt.exists():
            return False
        self.paths.halt.unlink()
        log.info("[law-ingest] halt cleared by=%s", by)
        self.trigger("resume")
        return True

    def requeue(self, name: str, stage: str) -> List[str]:
        moved = requeue(self.paths, name, stage)
        self.trigger("requeue")
        return moved

    def manifest_overdue(self, now: Optional[float] = None) -> List[str]:
        return manifest_overdue(self.paths, self.manifest_grace, now)

    def last_receipt(self) -> Optional[str]:
        return last_receipt(self.paths)

    def attention(self) -> List[str]:
        return attention(self.paths, self.manifest_grace, self.expect_hours)

    def status(self) -> dict:
        halt = self.halted()
        last = self.last_receipt()
        return {
            "mode": "local",
            "instance": self.instance,
            "state": "halted" if halt else ("running" if self.running else "idle"),
            "halt": halt,
            "attention": self.attention(),
            "interval_sec": self.interval,
            "retention_days": self.retention_days,
            "last_sweep_at": self.last_sweep_at,
            "last_run": read_json(self.paths.latest),
            "last_received_at": last,
            "hours_since_last_receipt": round(_age_sec(last) / 3600, 1) if last else None,
            **inbox_status(self.paths),
            "lease": read_json(self.paths.lease),
        }

    # ── 실행 ──
    def _loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.interval)
            self._wake.clear()
            if self._stop.is_set():
                break
            reason, self._reason = self._reason or "interval", None
            try:
                self.sweep(reason)
            except Exception:                        # sweep 자체가 던지면(기록 실패 등) 스레드는 살린다
                log.exception("[law-ingest] sweep crashed reason=%s", reason)

    def sweep(self, reason: str) -> Optional[dict]:
        """한 번 돈다. HALT·lease 때문에 안 돌면 None."""
        if self.halted():
            log.info("[law-ingest] halted — skip reason=%s", reason)
            return None
        if not self._acquire_lease():
            return None
        self.running = True
        refresher = threading.Thread(target=self._refresh_lease, daemon=True)
        refresher_stop = self._refresh_stop = threading.Event()
        refresher.start()
        log.info("[law-ingest] sweep start reason=%s instance=%s", reason, self.instance)
        try:
            record = self.run_fn(reason)
            overdue = self.manifest_overdue()
            if overdue:                               # manifest 없이 방치된 ZIP — 조용한 대기 대신 멈춤
                record["verdict"] = HALT
                record.setdefault("reasons", []).extend(f"manifest_missing:{n}" for n in overdue)
            record["retired"] = retire(self.paths, self.retention_days)
            record["attention"] = [n for n in self.attention() if not n.startswith("manifest_missing")]
            record["id"] = f"{record['started_at'].replace(':', '').replace('+00:00', 'Z')}_{reason}"
            record["instance"] = self.instance
            write_json(self.paths.runs / f"{record['id']}.json", record)
            write_json(self.paths.latest, record)
            if record.get("verdict") == HALT:
                write_json(self.paths.halt, {"at": _now(), "run_id": record["id"],
                                             "reasons": record.get("reasons") or []})
            for note in record["attention"]:
                log.warning("[law-ingest] attention %s", note)
            log.info("[law-ingest] sweep end verdict=%s reasons=%s retired=%s id=%s",
                     record.get("verdict"), record.get("reasons"), record["retired"], record["id"])
            return record
        finally:
            refresher_stop.set()
            self._release_lease()
            self.running = False
            self.last_sweep_at = _now()

    # ── lease ──
    def _acquire_lease(self) -> bool:
        current = read_json(self.paths.lease)
        if current and current.get("instance") != self.instance and _age_sec(current.get("at")) < self.lease_ttl:
            log.warning("[law-ingest] lease held by %s since %s — skip", current.get("instance"), current.get("at"))
            return False
        write_json(self.paths.lease, {"instance": self.instance, "at": _now()})
        return True

    def _refresh_lease(self) -> None:
        while not self._refresh_stop.wait(max(self.lease_ttl // 3, 1)):
            write_json(self.paths.lease, {"instance": self.instance, "at": _now()})

    def _release_lease(self) -> None:
        current = read_json(self.paths.lease)
        if current and current.get("instance") == self.instance:
            try:
                self.paths.lease.unlink()
            except OSError:
                pass
