"""ZIP 수신 2단계(LAW-41) — 수신 폴더의 법령 ZIP 을 파일명 순서로 package 로 바꿔 기존 소비 경로로 적재하고
ZIP 별 처리 이력을 남긴다.

ZIP 하나의 흐름:
  SHA-256 → 이력 확인(같은 SHA-256 을 이미 완료·부분완료했으면 이름이 달라도 duplicate/ 로 옮기고 끝)
  → manifest 대조(있으면 SHA-256·문서 수·폐지 수, 없으면 경고) → read_zip(문서 단위 검증)
  → write_packages(첨부는 file_transfer — record 에는 transfer_name 만) → consume_folder 로 적재
  → 이력 확정 → done/ 또는 failed/ 로 이동(원본은 지우지 않는다).

소비 로직은 새로 짜지 않는다. package 폴더(기본 <dir>/packages)는 그대로 consume-folder 폴더라 processed/·
failed/·nack/·재시도(.attempts)·scripts/requeue_failed_packages.py 가 같은 규칙으로 돈다. package 를 떨궈
두기만 하지 않고 바로 소비하는 이유는 순서다 — '같은 문서가 여러 ZIP 에 있으면 나중 ZIP 이 최종'은 앞
ZIP 이 실제로 적재됐는지 알아야 지킬 수 있다. package 이름은 변환 시각이라 같은 초에 만든 두 ZIP 의
package 순서가 보장되지 않고, 앞 package 가 일시장애 재시도로 밀리면 뒤 package 가 먼저 들어간다.
그래서 앞 ZIP 의 package 가 재시도 대기면 뒤 ZIP 은 다음 실행으로 미룬다.

이력(state/<sha256>.json)은 tmp→fsync→rename 으로 쓰고, 단계를 먼저 기록한 뒤 움직인다 — 중간에 죽어도
다음 실행이 기록된 단계부터 이어 간다.
  converting : 변환 중. 죽었으면 작업 폴더(<package 폴더>/.zip-work/<sha256>)를 버리고 처음부터.
  loading    : package 를 다 썼고 게시·적재 중. 죽었으면 남은 게시를 마치고 적재부터.
  done / partial / failed : 확정. 기록 후 ZIP 을 옮긴다(옮기기 전에 죽었으면 다음 실행이 옮기기만 마친다).
"""
import dataclasses
import fcntl
import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from .config import Settings
from .pipeline import consume_folder
from .zip_intake import (Issue, ZipIntakeError, ZipIntakeResult, flatten_nested_zips,
                         read_zip, write_packages)

DONE, PARTIAL, FAILED = "done", "partial", "failed"
CONVERTING, LOADING = "converting", "loading"
DUPLICATE = "duplicate"
# 이 상태의 SHA-256 이 다시 오면 처리하지 않는다. 실패(failed)는 다시 넣으면 처음부터 재처리한다.
_COMPLETED = (DONE, PARTIAL)
_SUMMARY_KEYS = {DONE: "done", PARTIAL: "partial", FAILED: "failed", DUPLICATE: "duplicate",
                 LOADING: "waiting"}
MANIFEST_SUFFIX = ".manifest.json"
_READ_CHUNK = 1024 * 1024


@dataclass
class _Run:
    """consume-zip 1회 실행의 폴더·소비 자원."""

    inbox: Path                     # ZIP 수신 폴더
    state: Path                     # 처리 이력(<sha256>.json)·잠금
    done: Path
    failed: Path
    duplicate: Path
    packages: Path                  # consume-folder 폴더
    files: Path                     # 첨부(file_transfer) 폴더 = 소비자 PACKAGE_FILE_INBOX_DIR
    embedder: object
    settings: Settings              # 소비용 — package_file_inbox_dir 를 files 로 맞춘 것
    nack_dir: Optional[Path]
    scope: object


def consume_zip_folder(embedder, settings: Settings, folder: Path, *,
                       package_dir: Optional[Path] = None, nack_dir: Optional[Path] = None,
                       scope=None) -> dict:
    """수신 폴더의 `*.zip` 을 파일명 순서로 처리한다. 반환: 실행 요약(ZIP 별 결과 포함).

    처리 대상은 폴더 바로 아래 `*.zip` 뿐이다(점으로 시작하는 임시 파일·manifest·기타는 보지 않는다).
    문서 단위 오류는 그 문서만 빼고 나머지를 적재한다(부분완료). ZIP 손상·manifest 불일치·실을 문서
    없음·적재 격리는 실패로 failed/ 에 둔다. 한 ZIP 의 실패가 다음 ZIP 을 막지는 않지만, 적재가 재시도
    대기인 ZIP 이 있으면 순서를 지키려고 거기서 멈춘다(다음 실행이 이어 간다)."""
    folder = Path(folder)
    if not folder.is_dir():
        raise FileNotFoundError(f"consume-zip 대상 폴더가 없습니다(경로 확인): {folder}")
    packages = Path(package_dir) if package_dir else folder / "packages"
    # 첨부는 이 폴더에 풀고 record 에는 transfer_name 만 싣는다(소비자가 package 를 통째로 메모리에
    # 버퍼링하므로 base64 인라인은 대용량 ZIP 에 맞지 않는다). 소비자도 같은 폴더를 봐야 한다.
    files = Path(settings.package_file_inbox_dir or (packages / "files"))
    run = _Run(inbox=folder, state=folder / "state", done=folder / "done", failed=folder / "failed",
               duplicate=folder / "duplicate", packages=packages, files=files, embedder=embedder,
               settings=dataclasses.replace(settings, package_file_inbox_dir=files),
               nack_dir=nack_dir, scope=scope)
    for path in (run.state, run.packages, run.files):
        path.mkdir(parents=True, exist_ok=True)

    summary = {"folder": str(folder), "package_dir": str(packages), "total": 0, "done": 0,
               "partial": 0, "failed": 0, "duplicate": 0, "waiting": 0, "deferred": 0, "zips": []}
    with _exclusive(run.state) as acquired:
        if not acquired:
            summary["skipped"] = "다른 consume-zip 실행이 이 폴더를 처리 중입니다"
            return summary
        zips = sorted(p for p in folder.glob("*.zip") if p.is_file() and not p.name.startswith("."))
        summary["total"] = len(zips)
        for index, zip_path in enumerate(zips):
            outcome = _process(run, zip_path)
            summary[_SUMMARY_KEYS[outcome["status"]]] += 1
            summary["zips"].append(outcome)
            if outcome["status"] == LOADING:
                summary["deferred"] = len(zips) - index - 1
                break
    return summary


# ── ZIP 하나 ────────────────────────────────────────────────────────────

def _process(run: _Run, zip_path: Path) -> dict:
    sha256 = _sha256(zip_path)
    record = _load_record(run, sha256)
    status = (record or {}).get("status")
    if status in _COMPLETED:
        return _skip_completed(run, zip_path, record)
    if status == LOADING:
        record["zip_name"] = zip_path.name             # 이어받기 — 이름을 바꿔 다시 넣었을 수 있다
    else:                                              # 처음 · 실패 후 재투입 · 변환 중 죽음
        record = _convert(run, zip_path, sha256, record)
        if record["status"] == FAILED:
            return _finish(run, zip_path, record)
    _publish(run, record)
    if record["packages"]:
        _load(run, record)
        states = {item["status"] for item in record["packages"]}
        if "pending" in states:
            _save(run, record)
            return _outcome(record)                    # loading 그대로 — 다음 실행이 이어 간다
        if "failed" in states:
            record.update(status=FAILED, reason="load_failed")
            record["issues"].append(dataclasses.asdict(Issue(
                "error", "load_failed",
                "package 적재가 실패해 consume-folder 가 격리했습니다(package 폴더 failed/ 의 .errors.json 참조)")))
            return _finish(run, zip_path, record)
    record["status"], record["reason"] = _final_status(record["summary"])
    return _finish(run, zip_path, record)


def _convert(run: _Run, zip_path: Path, sha256: str, previous: Optional[dict]) -> dict:
    """manifest 대조 → read_zip → write_packages(작업 폴더). 결과 record(status=loading 또는 failed)."""
    record = _new_record(zip_path, sha256, previous)
    work = _work_dir(run, sha256)
    shutil.rmtree(work, ignore_errors=True)             # 지난 실행이 변환 중에 죽었으면 반쯤 쓴 package 를 버린다
    manifest_path = find_manifest(zip_path)
    record["manifest"] = manifest_path.name if manifest_path else None
    _save(run, record)

    issues: List[Issue] = []
    manifest = None
    if manifest_path is None:
        issues.append(Issue("warning", "manifest_missing",
                            f"ZIP 옆에 manifest({zip_path.stem}{MANIFEST_SUFFIX})가 없어 대조하지 않았습니다"))
    else:
        manifest, problems = _read_manifest(manifest_path)
        issues += problems
        if manifest is not None:
            issues += _check_manifest_file(manifest, zip_path, sha256)
    if _errors(issues):
        return _fail(record, issues, _errors(issues)[0].code)
    # 첨부가 ZIP 이면(법령 사이트가 별표 묶음을 ZIP 으로 준다) _scan_members 가
    # unsupported_extension 으로 거부해 내용물이 통째로 유실된다. read_zip 에 넘기기 전에 펼친다.
    # manifest 대조·sha256 은 위에서 원본 기준으로 끝났고, 펼친 ZIP 은 work 안에 둬서
    # _publish 의 rmtree 가 함께 치운다.
    source_zip = zip_path
    try:
        flattened = flatten_nested_zips(zip_path, work / "flattened.zip")
        if flattened is not None:
            source_zip = flattened
            issues.append(Issue("warning", "nested_zip_flattened",
                                "첨부 ZIP 을 펼쳐 내용물을 적재 대상에 포함했습니다", path=zip_path.name))
    except Exception as exc:                            # 펼치기 실패는 치명적이지 않다 — 원본으로 간다
        issues.append(Issue("warning", "nested_zip_flatten_failed",
                            f"첨부 ZIP 펼치기 실패, 원본으로 진행합니다: {exc}", path=zip_path.name))
    try:
        result = read_zip(source_zip, manifest=manifest)
    except ZipIntakeError as exc:
        return _fail(record, issues + [Issue("error", "zip_corrupt", str(exc), path=zip_path.name)], "zip_corrupt")
    mismatches = _check_manifest_counts(manifest, result) if manifest is not None else []
    issues += mismatches + _result_issues(result)
    record["summary"] = _summary(result, [])
    if _errors(mismatches):
        return _fail(record, issues, "manifest_mismatch")
    try:
        written = write_packages(result, work, file_stage_dir=run.files)
    except ZipIntakeError as exc:                       # 검증과 변환 사이에 ZIP 이 바뀌었다
        shutil.rmtree(work, ignore_errors=True)
        return _fail(record, issues + [Issue("error", "zip_corrupt", str(exc), path=zip_path.name)], "zip_corrupt")
    record.update(
        status=LOADING, summary=_summary(result, written), issues=_dicts(issues),
        packages=[{"name": Path(w["path"]).name, "source": w["source"], "package_id": w["package_id"],
                   "documents": w["documents"], "deletes": w["deletes"], "files": w["files"],
                   "pending_attachments": w["pending_attachments"], "status": "pending"}
                  for w in written])
    _save(run, record)
    return record


def _publish(run: _Run, record: dict) -> None:
    """작업 폴더의 package 를 consume-folder 폴더로 옮긴다(이미 옮긴 것은 건너뛴다 — 재실행 안전).
    package 를 다 쓴 뒤에 옮기므로 소비자가 반쯤 변환한 ZIP 의 package 를 보는 일은 없다."""
    work = _work_dir(run, record["sha256"])
    for item in record["packages"]:
        src = work / item["name"]
        if src.exists():
            os.replace(src, run.packages / item["name"])
    shutil.rmtree(work, ignore_errors=True)


def _load(run: _Run, record: dict) -> None:
    """기존 consume_folder 로 package 폴더를 소비하고, 이 ZIP 의 package 마다 결과를 파일 위치로 정한다.

    sweep 요약만 보면 다른 소비자(스케줄된 consume-folder 등)가 먼저 가져간 package 를 놓친다 — 위치는
    누가 소비했든 같은 규칙(processed/·failed/·제자리=재시도 대기)을 따른다."""
    processed, failed = run.packages / "processed", run.packages / "failed"
    sweep = consume_folder(run.embedder, run.settings, run.packages, processed_dir=processed,
                           failed_dir=failed, nack_dir=run.nack_dir, scope=run.scope)
    by_name = {entry.get("package"): entry for entry in sweep.get("packages") or []}
    for item in record["packages"]:
        name = item["name"]
        if (run.packages / name).exists() or (run.packages / (name + ".processing")).exists():
            item["status"] = "pending"                   # 일시장애 재시도 대기(.attempts) — 다음 실행이 다시 소비
        elif (failed / name).exists():
            item["status"] = "failed"                    # consume-folder 가 격리(.errors.json · nack)
        else:
            item["status"] = "ok"                        # processed/ 로 옮겨졌거나 PACKAGE_DELETE_CONSUMED_PACKAGE 로 지워짐
        if name in by_name:
            item["consume"] = by_name[name]


def _final_status(summary: dict) -> Tuple[str, Optional[str]]:
    """적재까지 끝난 ZIP 의 확정 상태 — 거부 문서·ZIP 단위 오류가 있으면 부분완료, 실은 문서가 없으면 실패."""
    if not (summary.get("rejected") or summary.get("zip_errors")):
        return DONE, None
    if not summary.get("accepted"):
        return FAILED, "all_rejected"
    return PARTIAL, None


def _finish(run: _Run, zip_path: Path, record: dict) -> dict:
    """확정 상태를 먼저 기록하고 ZIP(과 manifest)을 done/ 또는 failed/ 로 옮긴다."""
    target = _free_target(run.failed if record["status"] == FAILED else run.done, zip_path.name)
    record["archive"] = target.relative_to(run.inbox).as_posix()
    record["finished_at"] = _now()
    _save(run, record)
    _move(zip_path, target)
    return _outcome(record)


def _skip_completed(run: _Run, zip_path: Path, record: dict) -> dict:
    """이미 완료·부분완료한 SHA-256. 이력을 확정하고 옮기기 전에 죽은 바로 그 ZIP 이면 옮기기만 마치고,
    아니면(같은 내용이 다시 옴 — 이름이 달라도) duplicate/ 로 옮긴다. 어느 쪽도 다시 적재하지 않는다."""
    archive = record.get("archive")
    if archive and record.get("zip_name") == zip_path.name and not (run.inbox / archive).exists():
        _move(zip_path, run.inbox / archive)
        return _outcome(record)
    target = _free_target(run.duplicate, zip_path.name)
    record.setdefault("duplicates", []).append(
        {"zip_name": zip_path.name, "at": _now(), "archive": target.relative_to(run.inbox).as_posix()})
    _save(run, record)
    _move(zip_path, target)
    return {"zip": zip_path.name, "sha256": record["sha256"], "status": DUPLICATE,
            "duplicate_of": record.get("zip_name"), "archive": target.relative_to(run.inbox).as_posix()}


# ── manifest ────────────────────────────────────────────────────────────

def find_manifest(zip_path: Path) -> Optional[Path]:
    """ZIP 옆 manifest. 수집기 zipbatch 형식은 `<stem>.manifest.json`(batch-….zip ↔ batch-….manifest.json)
    이고, `<이름>.zip.manifest.json` 도 받는다."""
    zip_path = Path(zip_path)
    for candidate in (zip_path.with_name(zip_path.stem + MANIFEST_SUFFIX),
                      zip_path.with_name(zip_path.name + MANIFEST_SUFFIX)):
        if candidate.is_file():
            return candidate
    return None


def _read_manifest(path: Path) -> Tuple[Optional[dict], List[Issue]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, [Issue("error", "manifest_invalid", f"manifest 를 읽을 수 없습니다: {exc}", path=path.name)]
    if not isinstance(data, dict):
        return None, [Issue("error", "manifest_invalid", "manifest 최상위가 객체가 아닙니다", path=path.name)]
    return data, []


def _check_manifest_file(manifest: dict, zip_path: Path, sha256: str) -> List[Issue]:
    """manifest 의 sha256(zipbatch: 완성 ZIP 의 SHA-256)을 실제 파일과 대조한다. filename 은 순번을 붙여
    이름만 바꿔 보낼 수 있어 경고만 한다."""
    issues = []
    expected = manifest.get("sha256")
    if not isinstance(expected, str) or not expected.strip():
        issues.append(Issue("error", "manifest_invalid", "manifest 에 sha256 이 없습니다", path=zip_path.name))
    elif expected.strip().lower() != sha256:
        issues.append(Issue("error", "manifest_mismatch",
                            f"SHA-256 이 다릅니다(manifest {expected.strip()}, 실제 {sha256}) — 전송 중 손상이거나 "
                            "다른 ZIP 의 manifest 입니다", path=zip_path.name))
    filename = manifest.get("filename")
    if isinstance(filename, str) and filename and filename != zip_path.name:
        issues.append(Issue("warning", "manifest_filename",
                            f"manifest 의 filename({filename})과 ZIP 이름이 다릅니다 — 이름만 바뀐 것으로 보고 "
                            "SHA-256 으로 대조했습니다", path=zip_path.name))
    return issues


def _check_manifest_counts(manifest: dict, result: ZipIntakeResult) -> List[Issue]:
    """manifest 의 documents(ZIP 에 담은 문서 JSON 수)·repealed(그중 폐지 수)를 ZIP 내용과 대조한다."""
    actual = {"documents": len(result.documents),
              "repealed": sum(1 for doc in result.documents if doc.repealed)}
    issues = []
    for key, label in (("documents", "문서 수"), ("repealed", "폐지 수")):
        value = manifest.get(key)
        if value is None:
            issues.append(Issue("warning", "manifest_field_missing",
                                f"manifest 에 {key}({label})가 없어 대조하지 않았습니다"))
        elif isinstance(value, bool) or not isinstance(value, int) or value != actual[key]:
            issues.append(Issue("error", "manifest_mismatch",
                                f"{label}가 다릅니다(manifest {value!r}, ZIP {actual[key]})"))
    return issues


# ── 이력 ────────────────────────────────────────────────────────────────

def _new_record(zip_path: Path, sha256: str, previous: Optional[dict]) -> dict:
    """새 처리 기록. 실패·중단 후 다시 처리하면 이전 기록의 상태를 history 에 남긴다."""
    history = list((previous or {}).get("history") or [])
    if previous:
        history.append({key: previous.get(key) for key in ("zip_name", "status", "reason", "updated_at")})
    now = _now()
    return {"sha256": sha256, "zip_name": zip_path.name, "size_bytes": zip_path.stat().st_size,
            "status": CONVERTING, "reason": None, "attempt": len(history) + 1,
            "started_at": now, "updated_at": now, "finished_at": None, "manifest": None,
            "summary": {}, "packages": [], "issues": [], "archive": None, "history": history}


def _fail(record: dict, issues: List[Issue], reason: str) -> dict:
    record.update(status=FAILED, reason=reason, issues=_dicts(issues))
    return record


def _summary(result: ZipIntakeResult, written: List[dict]) -> dict:
    """이력 요약 — read_zip 의 문서·삭제·거부 수 + package 에 실은 첨부 수."""
    summary = result.summary()
    summary["zip_errors"] = len(result.errors)          # 문서에 귀속되지 않는 오류(고아 첨부·경로·확장자 등)
    summary["files"] = sum(w["files"] for w in written)
    summary["pending_attachments"] = sum(w["pending_attachments"] for w in written)
    return summary


def _result_issues(result: ZipIntakeResult) -> List[Issue]:
    issues = list(result.errors) + list(result.warnings)
    for doc in result.documents:
        issues += doc.errors + doc.warnings
    return issues


def _errors(issues: List[Issue]) -> List[Issue]:
    return [issue for issue in issues if issue.level == "error"]


def _dicts(issues: List[Issue]) -> List[dict]:
    return [dataclasses.asdict(issue) for issue in issues]


def _outcome(record: dict) -> dict:
    """실행 요약에 넣을 ZIP 한 건의 결과(전체는 state/<sha256>.json)."""
    summary = record.get("summary") or {}
    issues = record.get("issues") or []
    return {"zip": record["zip_name"], "sha256": record["sha256"], "status": record["status"],
            "reason": record.get("reason"), "archive": record.get("archive"),
            "documents": summary.get("documents", 0), "deletes": summary.get("deletes", 0),
            "rejected": summary.get("rejected", 0), "files": summary.get("files", 0),
            "issue_errors": sum(1 for issue in issues if issue.get("level") == "error"),
            "issue_warnings": sum(1 for issue in issues if issue.get("level") == "warning"),
            "packages": [{"name": p["name"], "status": p["status"]} for p in record.get("packages") or []]}


def _record_path(run: _Run, sha256: str) -> Path:
    return run.state / f"{sha256}.json"


def _load_record(run: _Run, sha256: str) -> Optional[dict]:
    try:
        return json.loads(_record_path(run, sha256).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):              # 없으면 처음. 원자적으로 써서 깨질 일은 없다
        return None


def _save(run: _Run, record: dict) -> None:
    """기록을 원자적으로 쓴다(tmp → fsync → rename) — 쓰다 죽어도 이전 기록이 온전히 남는다."""
    record["updated_at"] = _now()
    path = _record_path(run, record["sha256"])
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


@contextmanager
def _exclusive(state_dir: Path) -> Iterator[bool]:
    """같은 수신 폴더를 두 실행이 동시에 처리하지 않게 한다(스케줄 겹침). 잡혀 있으면 기다리지 않고 False.
    '변환 중(converting) 기록 = 지난 실행이 죽음' 판단은 이 잠금이 있어야 성립한다."""
    with open(state_dir / ".lock", "a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


# ── 파일 ────────────────────────────────────────────────────────────────

def _work_dir(run: _Run, sha256: str) -> Path:
    """변환 작업 폴더 — package 폴더 안에 둬서(같은 파일시스템) 게시가 rename 한 번이다.
    consume-folder 는 폴더 바로 아래 *.jsonl 만 보므로 여기 있는 동안은 소비되지 않는다."""
    return run.packages / ".zip-work" / sha256


def _free_target(folder: Path, name: str) -> Path:
    """folder 안에서 겹치지 않는 경로(같은 이름이 있으면 `<stem>.1.zip`, `.2` …) — 옮기며 덮어쓰지 않는다."""
    target, stem, suffix = folder / name, Path(name).stem, Path(name).suffix
    n = 0
    while target.exists():
        n += 1
        target = folder / f"{stem}.{n}{suffix}"
    return target


def _move(zip_path: Path, target: Path) -> None:
    """ZIP 을 옮기고 옆 manifest 도 target 이름에 맞춘 `<stem>.manifest.json` 으로 따라 옮긴다(다시 넣을 때
    짝이 맞는다). ZIP 을 먼저 옮긴다 — 사이에 죽어도 manifest 없는 ZIP 이 남아 대조 없이 처리되는 일이 없다."""
    manifest = find_manifest(zip_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(zip_path), str(target))
    if manifest is not None:
        shutil.move(str(manifest), str(target.with_name(target.stem + MANIFEST_SUFFIX)))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(_READ_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
