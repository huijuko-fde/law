"""JSONL package 소비자.

`temporal_law`가 만든 handoff package를 읽어 Weaviate와 선택적 내부망 data repo에 반영한다.
생산자가 어떤 record를 넣었는지는 망 구성과 전처리 위치에 따라 달라지지만, 소비자는 모든
record_type을 같은 진입점에서 처리한다.

한 package는 JSONL 파일 하나다. 첫 줄은 보통 `package_header`, 마지막 줄은 선택적으로
`package_footer`가 오고, 중간에는 `document`, `normalized_chunk`, `preprocessed_chunk`,
`file`, `pending_attachment`, `delete` record가 섞일 수 있다.

같은 `law_id`의 record가 여러 줄에 흩어져 있을 수 있으므로 먼저 `law_id`별로 버퍼링한 뒤
한 번에 flush한다. 변경 문서는 새 record를 upsert한 뒤 옛 버전 청크를 정리하고, 폐지 문서는
기존 청크를 삭제한다. 재시도해도 같은 결과가 나와야 한다.

계약상 변경 문서는 `document` record에 현재 payload가 들어오는 것이 기본이다. `document`
없이 file/chunk만 온 경우는 본문 유실 위험이 있으므로 경고를 남기고 가능한 record만 처리한다.
"""
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePosixPath
from typing import Callable, Dict, List, Optional

from .config import Settings
from .embedder import LocalEmbedder
from .mapper import (
    _compact_name, build_search_text, iso_date, map_admrul_data, map_attachment_data,
    map_law_data, stable_id,
)
from .models import LegalProvision
from .preprocess import (
    DocParserClient, DocParserError, file_sha256, is_permanent_parser_error,
    ocr_image_file, resolve_article_images, run_doc_parser, substitute_image_markers,
)

# package record_type 상수.
HEADER = "package_header"
DOCUMENT = "document"
NORMALIZED_CHUNK = "normalized_chunk"
PREPROCESSED_CHUNK = "preprocessed_chunk"
FILE = "file"
PENDING_ATTACHMENT = "pending_attachment"
DELETE = "delete"
RENAME = "rename"
FOOTER = "package_footer"

# document payload(법령 JSON 최상위)에서 첨부/정규화 청크가 물려받을 공통 메타 필드.
# map_attachment_data 가 읽는 키와 이름을 맞춘다(그래야 그대로 재사용된다).
_LAW_META_KEYS = (
    "law_id", "mst", "version_uid", "law_name", "law_abbr", "law_type",
    "promulgation_date", "enforcement_date", "revision_date", "revision_type",
    "is_current", "is_future", "git_commit",
)


def peek_package_source(path: Path) -> Optional[str]:
    """package_header 의 source 만 미리 읽는다(헤더 없으면 None).

    genos 는 컬렉션마다 Weaviate API 키가 달라(RBAC) **연결을 열기 전에** source 를 알아야
    맞는 키로 붙을 수 있다. 그래서 소비 시작 전에 첫 줄만 들여다본다."""
    try:
        with open(Path(path), encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line:
                    continue
                record = json.loads(line)
                return record.get("source") if record.get("record_type") == HEADER else None
    except (OSError, json.JSONDecodeError, AttributeError):
        return None
    return None


def peek_package_law_ids(path: Path) -> tuple[Optional[str], List[str]]:
    """package 안의 source 와 문서 law_id 목록만 훑는다(격리 시 nack 을 쓰기 위한 최소 조회).

    소비에 최종 실패한 package 를 생산자에게 되돌리려면 "어떤 문서가 못 들어갔는지"를 알려줘야
    한다. 전체 소비 경로를 다시 타지 않고 record 의 law_id 만 모은다 — 깨진 줄은 건너뛴다
    (그 줄이 깨졌다는 것 자체가 이미 errors 에 담겨 있다)."""
    source: Optional[str] = None
    law_ids: List[str] = []
    seen = set()
    try:
        with open(Path(path), encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, AttributeError):
                    continue
                if record.get("record_type") == HEADER:
                    source = record.get("source")
                    continue
                law_id = record.get("law_id")
                if law_id and law_id not in seen:
                    seen.add(law_id)
                    law_ids.append(str(law_id))
    except OSError:
        return source, law_ids
    return source, law_ids


def is_package_file(path: Path) -> bool:
    """이 JSONL 의 첫 비어있지 않은 줄이 package_header 면 True(자동감지용).

    읽기 실패·JSON 아님·record_type 없음이면 False(= 1세대 change-set 으로 처리)."""
    try:
        with open(Path(path), encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line:
                    continue
                try:
                    return json.loads(line).get("record_type") == HEADER
                except (json.JSONDecodeError, AttributeError):
                    return False
    except OSError:
        return False
    return False


def _new_totals() -> dict:
    """package 소비 결과 카운터."""
    return {
        "documents": 0, "deleted_docs": 0,
        "normalized_chunks": 0, "preprocessed_chunks": 0,
        "files_preprocessed": 0, "files_pending": 0, "pending_attachments": 0,
        "images_success": 0, "images_failed": 0,
        "objects_success": 0, "objects_failed": 0,
        "laws_indexed": 0, "laws_failed": 0,
        "mirrored_docs": 0, "mirrored_files": 0,
        "renames": 0, "rename_refs_patched": 0,
        "errors": [],
    }


@dataclass
class _LawBuffer:
    """한 law_id 로 모이는 record 들(순서 무관, flush 때 한꺼번에 재적재)."""

    law_id: str
    delete: bool = False
    document: Optional[dict] = None
    document_git_path: Optional[str] = None
    commit_message: Optional[str] = None                              # 수집기와 동일(미러 push 커밋용)
    normalized: List[dict] = field(default_factory=list)               # normalized_chunk record[]
    preprocessed: Dict[str, dict] = field(default_factory=dict)        # file_id -> {meta, chunks[]}
    files: List[dict] = field(default_factory=list)                    # file record[]
    pending: List[dict] = field(default_factory=list)                  # pending_attachment record[]

    def has_content(self) -> bool:
        """재적재할 실제 색인 대상(본문/청크/파일)이 있는지 — pending 만 있으면 False."""
        return bool(self.document or self.normalized or self.preprocessed or self.files)


class OriginalStore:
    """원문 표시(subcase 2)용 내부 저장소 — document JSON·원본 파일을 내부망에 보관한다.

    genos 처럼 VDB 중심이면 config(package_store_original=off)로 아예 안 만든다."""

    def __init__(self, root: Path, source: str):
        self.root = Path(root) / source

    def save_document(self, law_id: str, version_uid: Optional[str], payload: dict) -> None:
        target = self.root / _safe(law_id)
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{_safe(version_uid) or 'current'}.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def save_file(self, law_id: str, file_name: str, src_path: Path) -> None:
        target = self.root / _safe(law_id) / "files"
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, target / _safe(file_name))


class RepoMirror:
    """C 옵션 — 내부망 데이터 레포를 증분마다 **repo 레이아웃 그대로** 최신화한다.

    초기 pull 로 이미 레포를 가진 내부망(초기반입 가능)에서, 별도 저장소가 아니라 바로 그 레포의
    같은 위치를 갱신해 원문/연혁 도구가 최신 데이터를 보게 한다. 위치 기준은 생산자가 실어 준
    document.git_path(레포 상대경로). off(PACKAGE_MIRROR_REPO 미설정)면 아예 안 만든다."""

    def __init__(self, repo_root: Path, push: bool = False, branch: str = "main"):
        self.repo_root = Path(repo_root).resolve()
        self.push_enabled = push               # True 면 미러 후 내부 git 서버로 commit+push
        self.branch = branch or "main"
        self._dirty = False                    # commit 됐고 아직 push 안 한 게 있는지

    def commit(self, message: Optional[str]) -> bool:
        """이번 문서 미러 변경을 커밋한다(변경 있을 때만 = 멱등). message 는 **수집기와 동일**
        (패키지 document 레코드에 실려 온 _commit_msg 결과). push_enabled 아니면 no-op(=폴더만 가짐)."""
        if not self.push_enabled:
            return False
        _git(self.repo_root, "add", "-A")
        if _git(self.repo_root, "diff", "--cached", "--quiet").returncode == 0:
            return False                       # 스테이징 변경 없음
        ok = _git(self.repo_root, "commit", "-q", "-m", message or "[mirror] update").returncode == 0
        self._dirty = self._dirty or ok
        return ok

    def push(self) -> bool:
        """쌓인 커밋을 내부 git 서버(origin/{branch})로 push. push_enabled + 커밋된 게 있을 때만."""
        if not (self.push_enabled and self._dirty):
            return False
        ok = _git(self.repo_root, "push", "origin", self.branch, timeout=1800).returncode == 0
        if ok:
            self._dirty = False
        return ok

    def _resolve(self, git_path: Optional[str]) -> Optional[Path]:
        """git_path(레포 상대경로) → 레포 안 절대경로. 절대경로·상위탈출(..)·synthetic 은 거른다."""
        if not git_path or git_path.startswith("package:") or os.path.isabs(git_path):
            return None
        target = (self.repo_root / git_path).resolve()
        try:
            target.relative_to(self.repo_root)                # 경로 탈출 방어
        except ValueError:
            return None
        return target

    def save_document(self, git_path: Optional[str], payload: dict) -> bool:
        """document payload 를 레포의 {git_path}.json 위치에 기록(원자적: tmp→replace)."""
        target = self._resolve(git_path)
        if target is None:
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, target)
        return True

    def save_file(self, git_path: Optional[str], file_name: Optional[str],
                  unit_type: Optional[str], src_path: Path) -> bool:
        """원본 파일을 초기 gitexport 와 **동일한 서브폴더**에 둔다.

        문서 디렉터리(=git_path 의 부모)를 기준으로, unit_type·파일명으로 서브폴더를 판별한다
        (gitexport 배치 규칙과 동일: 별표/별지/서식/별첨/부속서 등은 kind 폴더,
        FILE 첨부파일→첨부파일, 본문이미지→본문이미지).
        → 내부망 미러가 초기 레포와 파일 배치까지 동일해져, 원문/연혁 도구가 그대로 본다."""
        doc_target = self._resolve(git_path)
        if doc_target is None:
            return False
        name = file_name or src_path.name
        subdir = doc_target.parent / _mirror_subfolder(name, unit_type)
        orig = _safe(name)                                    # git 원본 이름(수집기와 동일)
        if len(orig.encode("utf-8")) <= _MAX_PATH_BYTES:
            subdir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, subdir / orig)             # 리눅스가 쓸 수 있는 이름 → 원본 그대로
            return True
        # >255바이트: 리눅스 작업트리에 못 쓴다.
        rel = (subdir / orig).relative_to(self.repo_root).as_posix()
        if self.push_enabled:                                 # push 모드 → git 엔 원본 경로 blob(정책 B)
            return self._stage_blob_original(rel, src_path)
        # folder-only → 로컬 브라우징용 safe 사본(색인은 package src_path 로 하니 이름 무관)
        from .preprocess import _safe_name
        subdir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, subdir / _safe_name(orig))
        return True

    def rename_dir(self, old_dir: Optional[str], new_dir: Optional[str]) -> bool:
        """개명된 문서의 **옛 폴더**를 정리한다(레포 상대경로).

        새 폴더가 이미 있으면(개명 문서가 같은 package 의 document 로 재적재돼 새 위치에 이미
        쓰였음) 옛 폴더를 삭제하고, 새 폴더가 아직 없으면 옛 폴더를 새 위치로 옮긴다(문서 재적재가
        늦어져도 미러가 파일을 잃지 않게). 옛 폴더가 없으면 아무것도 안 한다(멱등)."""
        old_p = self._resolve(old_dir)
        if old_p is None or not old_p.is_dir() or old_p == self.repo_root:
            return False
        new_p = self._resolve(new_dir) if new_dir else None
        if new_p is not None and new_p != old_p and not new_p.exists():
            new_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_p), str(new_p))
            return True
        if new_p is not None and new_p == old_p:
            return False
        shutil.rmtree(old_p, ignore_errors=True)
        return True

    def _stage_blob_original(self, rel_posix: str, src_path: Path) -> bool:
        """>255바이트라 리눅스 작업트리에 못 쓰는 파일을 git 에 blob 으로 직접 넣는다
        (hash-object → index cacheinfo, skip-worktree). 커밋 트리엔 **원본 이름**이 그대로
        들어가(정책 B), push 하면 내부 git 서버도 원본 이름으로 찾아갈 수 있다. 로컬 디스크엔
        아무 것도 안 남지만 색인은 package 의 src_path 로 하므로 문제없다."""
        ho = _git(self.repo_root, "hash-object", "-w", "--", str(src_path))
        sha = (ho.stdout or "").strip()
        if ho.returncode != 0 or not sha:
            return False
        if _git(self.repo_root, "update-index", "--add", "--cacheinfo",
                f"100644,{sha},{rel_posix}").returncode != 0:
            return False
        _git(self.repo_root, "update-index", "--skip-worktree", "--", rel_posix)
        return True


# 리눅스/NFS 경로요소 한계(255바이트). git_sync._MAX_PATH_BYTES 와 같은 값이어야 한다 —
# 이보다 긴 원본 이름은 리눅스 작업트리에 못 써서 미러가 plumbing(blob→index 원본경로)으로 처리.
_MAX_PATH_BYTES = 255


def _safe(name: Optional[str]) -> str:
    """경로 조각으로 안전한 문자열(구분자 제거).

    미러는 수집기가 git 에 쓴 원본 경로를 재현한다. 255바이트를 넘는 경로 요소는 working tree 에
    쓰지 않고 git blob/index 로 stage 하므로 여기서 길이로 자르지 않는다(정책 B).
    """
    if not name:
        return ""
    return "".join(c for c in str(name) if c not in '\\/:*?"<>|\n\t').strip()


def _git(repo: Path, *args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    """미러 레포 git subprocess. 타임아웃 필수(인증대기 hang 방지) + add/commit 전 오래된
    index.lock 자동 제거(크래시 잔해로 커밋 영구 실패 막기 — gitexport 와 같은 규칙)."""
    if args and args[0] in ("add", "commit", "update-index"):
        lock = os.path.join(str(repo), ".git", "index.lock")
        try:
            if time.time() - os.path.getmtime(lock) > 300:
                os.remove(lock)
        except OSError:
            pass
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"                      # 인증 프롬프트 금지 → hang 대신 즉시 실패
    try:
        return subprocess.run(["git", "-C", str(repo), *args], capture_output=True,
                              text=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(args, 1, "", f"timeout after {timeout}s")


_APPENDIX_DIRS = (
    "부속서", "별표", "별지", "서식", "부록", "별첨", "별도", "양식",
    "붙임", "기타", "부표", "도식", "부도",
)


def _mirror_subfolder(file_name: Optional[str], unit_type: Optional[str]) -> str:
    """gitexport 배치 규칙과 동일한 첨부 서브폴더를 판별한다.

    package 파일 레코드에는 appendix kind 가 별도 필드로 안 올 수 있어 파일명 prefix 를 우선 본다.
    판별 불가 APPENDIX 는 수집기 기본값과 같이 별표로 둔다.
    """
    fn = file_name or ""
    for kind in _APPENDIX_DIRS:
        if re.match(rf"{re.escape(kind)}\s*(?:제\s*)?(?:\d|[A-Za-z가-힣ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ])", fn):
            return kind
    if unit_type == "APPENDIX":
        return "별표"
    if unit_type == "FILE":
        return "첨부파일"
    if re.match(r"제.*_\d+\.(gif|png|jpe?g)$", fn, re.I):
        return "본문이미지"
    return "첨부미러"                                     # 판별 불가 시 안전 폴백


class _RoutedStores:
    """문서별 컬렉션 라우팅용 store 묶음.

    수집기 package 는 source 가 law/admrul 둘뿐인데 컬렉션은 3개다 — admrul package 안에
    학칙공단 문서(doc_target=school/pi/public)가 섞여 오면 schlpub 컬렉션으로 보내야 한다.
    기본 store(호출자가 그 source 의 키로 연 연결)는 빌려 쓰고, 다른 source 문서가 나오면
    그 컬렉션의 키로 새 연결을 **그때** 연다(genos RBAC — 키가 컬렉션 단위). 새로 연 것만 닫는다."""

    def __init__(self, settings: Settings, primary_store, primary_source: str):
        self._settings = settings
        self._stores = {primary_source: primary_store}
        self._opened: List[object] = []

    def get(self, source: str):
        store = self._stores.get(source)
        if store is None:
            from .weaviate_store import WeaviateStore
            store = WeaviateStore(self._settings, source)
            self._stores[source] = store
            self._opened.append(store)
        return store

    def close(self) -> None:
        for store in self._opened:
            try:
                store.client.close()
            except Exception:                     # 종료 실패가 소비 결과를 바꾸면 안 된다
                pass


def _repo_url_for(settings: Settings, source: str) -> str:
    """source 의 데이터 레포 URL(source_repository 메타용)."""
    if source == "law":
        return settings.law_repo_url
    if source == "schlpub":
        return settings.schlpub_repo_url
    return settings.admrul_repo_url


@dataclass
class _Context:
    """package 소비 1회에 걸친 불변 컨텍스트."""

    source: str                                   # "law" | "admrul" | "schlpub"
    collection: str
    mapper_fn: Callable[..., List[LegalProvision]]
    source_repository: Optional[str]
    preprocess_files: bool
    inbox_dir: Optional[Path]
    doc_parser: Optional[DocParserClient]
    settings: Settings
    original: Optional[OriginalStore]
    # 전처리까지 성공한 inbox 파일을 지울지. 옆채널은 생산자와 공유하는 전달 영역이라
    # 안 지우면 계속 쌓인다. 재처리·원인확인이 필요하면 끄고 쓴다(기본 off).
    delete_consumed_files: bool = False
    mirror: Optional[RepoMirror] = None           # C 옵션 — 데이터 레포 repo 레이아웃 최신화


class RepoMirrorSet:
    """doc_target 에 맞는 RepoMirror 를 골라 쓰는 래퍼. RepoMirror 와 같은 인터페이스다.

    데이터 레포는 3개인데(LAW/ADMRUL/SCHLPUBRUL) 패키지 하나에 admrul 과 학칙공단 문서가 섞여 온다.
    레포를 하나로 고정하면 학칙공단 문서·파일이 ADMRUL 레포에 잘못 쓰인다.
    문서→파일 순서로 처리되므로 `save_document` 에서 정한 레포를 그 법의 파일·커밋에 이어 쓴다.
    """

    def __init__(self, settings, source: str, push: bool = False):
        self._settings, self._source, self._push = settings, source, push
        self._mirrors: Dict[str, RepoMirror] = {}
        self._cur: Optional[RepoMirror] = None

    def _mirror_for(self, doc_target: Optional[str]) -> Optional[RepoMirror]:
        root = self._settings.repo_for(doc_target, self._source)
        if not root:
            return None
        key = str(Path(root).resolve())
        if key not in self._mirrors:
            self._mirrors[key] = RepoMirror(
                root, push=self._push,
                branch=self._settings.repo_branch_for(doc_target, self._source))
        return self._mirrors[key]

    def save_document(self, git_path: Optional[str], payload: dict) -> bool:
        self._cur = self._mirror_for((payload or {}).get("doc_target"))
        return bool(self._cur and self._cur.save_document(git_path, payload))

    def save_file(self, git_path, file_name, unit_type, src_path) -> bool:
        m = self._cur or self._mirror_for(None)
        return bool(m and m.save_file(git_path, file_name, unit_type, src_path))

    def rename_dir(self, doc_target: Optional[str], old_dir: Optional[str],
                   new_dir: Optional[str]) -> bool:
        """개명 정리 — doc_target 으로 레포를 고른 뒤 그 레포의 옛 폴더를 정리한다.
        뒤이어 부르는 commit 이 같은 레포에 걸리도록 _cur 를 잡아 둔다."""
        m = self._mirror_for(doc_target)
        if m is None:
            return False
        self._cur = m
        return m.rename_dir(old_dir, new_dir)

    def commit(self, message: Optional[str]) -> bool:
        m = self._cur
        self._cur = None                                   # 법 단위로 리셋
        return bool(m and m.commit(message))

    def push(self) -> bool:
        return all(m.push() for m in self._mirrors.values()) if self._mirrors else True


def _law_meta(payload: Optional[dict], fallback_law_id: str) -> dict:
    """document payload(법령 JSON) 에서 첨부/정규화 청크가 물려받을 공통 메타를 뽑는다.

    payload 가 없으면(계약 위반: chunk/file 만 온 경우) law_id 만 채운 최소 메타."""
    if not isinstance(payload, dict):
        return {"law_id": fallback_law_id}
    source = payload.get("source") or {}
    meta = {key: payload.get(key) for key in _LAW_META_KEYS}
    meta["law_id"] = meta.get("law_id") or fallback_law_id
    meta["source_url"] = source.get("source_url")
    meta["adm_uid"] = payload.get("adm_uid")
    meta["git_path"] = payload.get("git_path")
    return meta


def _attachment_input(law_meta: dict, source: str, file_id: str, file_meta: dict,
                      chunks: List[dict]) -> dict:
    """map_attachment_data 가 먹는 flat dict 를 조립한다(preprocessed_chunk·file 공용).

    map_attachment_data 는 이 dict 로 FILE source LegalProvision(is_file_only=True)을 만든다 —
    별표/문서전체 원문 청크를 그대로 재사용하는 진입점이다."""
    data = {key: law_meta.get(key) for key in _LAW_META_KEYS}
    data["law_id"] = law_meta.get("law_id")
    data["source_url"] = law_meta.get("source_url")
    data["collection_type"] = source
    data["file_id"] = file_id
    data["provision_id"] = file_meta.get("provision_id")
    data["parent_provision_id"] = file_meta.get("parent_provision_id")
    data["unit_type"] = file_meta.get("unit_type")           # None 이면 map 이 APPENDIX/FILE 로 추론
    data["file_kind"] = file_meta.get("file_kind")           # document_file / attachment (원문/첨부 구분)
    data["unit_no"] = file_meta.get("unit_no")
    data["unit_title"] = file_meta.get("unit_title") or file_meta.get("file_name")
    data["file_name"] = file_meta.get("file_name")
    data["file_url"] = file_meta.get("file_url")
    data["source_file_path"] = file_meta.get("source_file_path")
    data["git_path"] = law_meta.get("git_path")
    data["reference_ids"] = file_meta.get("reference_ids") or []
    data["chunks"] = chunks
    return data


def _build_normalized(law_meta: dict, source: str, records: List[dict],
                      git_path: Optional[str]) -> List[LegalProvision]:
    """normalized_chunk record[](= 생산자가 미리 청킹한 본문 정규화 텍스트)를 JSON source
    LegalProvision 으로 만든다.

    보조 경로다 — 기본 계약은 document(본문 JSON) 를 임베딩 쪽에서 청킹하는 것(청킹은 임베딩
    쪽 유지). DMZ 가 미리 정규화·청킹해서 보낼 때만 이 record 가 온다(subcase 6 등)."""
    result = []
    total = len(records)
    for index, record in enumerate(records):
        meta = record.get("metadata") or {}
        content = str(record.get("content") or "")
        provision_id = meta.get("provision_id")
        unit_type = meta.get("unit_type") or "ARTICLE"
        unit_no = meta.get("unit_no")
        unit_title = meta.get("unit_title")
        chapter = meta.get("chapter")
        chunk_index = record.get("chunk_index", index)
        chunk_id = stable_id(law_meta.get("version_uid"), provision_id or unit_no, unit_type,
                             "NORM", chunk_index)
        result.append(LegalProvision(
            chunk_id=chunk_id, provision_id=provision_id, parent_provision_id=meta.get("parent_provision_id"),
            reference_ids=[], file_id=None, law_id=law_meta.get("law_id"), mst=law_meta.get("mst"),
            version_uid=law_meta.get("version_uid"), law_name=law_meta.get("law_name"),
            law_abbr=law_meta.get("law_abbr"), law_type=law_meta.get("law_type"),
            unit_type=unit_type, unit_no=unit_no, unit_title=unit_title, chapter=chapter,
            content=content, collection_type=source,
            search_text=build_search_text(law_meta.get("law_name"), law_meta.get("law_type"),
                                          chapter, unit_no, unit_title, content),
            source_type="JSON", file_name=None, file_url=None, page_no=None,
            chunk_index=int(chunk_index), chunk_count=total,
            promulgation_date=iso_date(law_meta.get("promulgation_date")),
            enforcement_date=iso_date(law_meta.get("enforcement_date")),
            revision_date=iso_date(law_meta.get("revision_date")),
            revision_type=law_meta.get("revision_type"), is_current=law_meta.get("is_current"),
            is_future=law_meta.get("is_future"), source_url=law_meta.get("source_url"),
            git_path=git_path, git_commit=law_meta.get("git_commit"),
            content_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            source_repository=None, adm_uid=law_meta.get("adm_uid"),
        ))
    return result


def _embed_and_upsert(store, embedder: LocalEmbedder, objects: List[LegalProvision],
                      collection: str) -> dict:
    """임베딩 벡터를 채워 컬렉션에 upsert 한다(pipeline._embed_and_store 와 동일 규약)."""
    vectors = embedder.embed_documents([obj.search_text for obj in objects])
    for obj, vector in zip(objects, vectors):
        obj.vector = vector
        obj.embedding_model = embedder.model_name
        obj.embedding_dimension = embedder.dimension
    return store.upsert(objects, embedder.dimension, embedder.model_name, collection)


def _preprocess_file_record(ctx: _Context, law_id: str, record: dict, totals: dict,
                            git_path: Optional[str] = None) -> Optional[dict]:
    """`file` record → 내부 전처리기로 청크화. 반환: (file_id, {meta, chunks}) 형태 dict 또는 None.

    내부 전처리 off / inbox 미설정 / 파일 없음 / 전처리 실패 → 보류(pending)로 넘기고 None.
    C 미러(PACKAGE_MIRROR_REPO)가 켜져 있으면, 전처리에 앞서 원본 파일을 레포에 co-locate 한다."""
    file_id = record.get("file_id") or stable_id(law_id, record.get("file_name"))
    if not ctx.preprocess_files:
        totals["files_pending"] += 1
        return {"_pending": {"file_id": file_id, "reason": "internal_preprocess_disabled",
                             "file_name": record.get("file_name")}}

    # 파일 바이트 확보: content_b64(JSONL 자기완결) 우선, 없으면 inbox 옆채널(transfer_name).
    tmp_dir = None
    content_b64 = record.get("content_b64")
    if content_b64:
        try:
            data = base64.b64decode(content_b64)
        except (ValueError, TypeError) as exc:
            totals["files_pending"] += 1
            totals["errors"].append({"law_id": law_id, "file_id": file_id, "error": f"content_b64 디코딩 실패: {exc}"})
            return {"_pending": {"file_id": file_id, "reason": "b64_decode_failed", "file_name": record.get("file_name")}}
        tmp_dir = tempfile.mkdtemp(prefix="pkg_file_")
        local_path = Path(tmp_dir) / _safe(record.get("transfer_name") or record.get("file_name") or "attachment")
        local_path.write_bytes(data)
    else:
        if not ctx.inbox_dir:
            totals["files_pending"] += 1
            return {"_pending": {"file_id": file_id, "reason": "no_file_inbox_dir", "file_name": record.get("file_name")}}
        transfer_name = record.get("transfer_name") or record.get("file_name")
        local_path = Path(ctx.inbox_dir) / _safe(transfer_name)
        if not local_path.exists():
            totals["files_pending"] += 1
            totals["errors"].append({"law_id": law_id, "file_id": file_id,
                                     "error": f"전달 파일이 inbox 에 없습니다: {transfer_name}"})
            return {"_pending": {"file_id": file_id, "reason": "file_missing_in_inbox",
                                 "file_name": record.get("file_name")}}
    # C 미러: 원본 파일을 내부망 레포에 co-locate (전처리 성공/실패와 무관하게 원본 보존)
    if ctx.mirror is not None:
        try:
            if ctx.mirror.save_file(git_path, record.get("file_name"),
                                    record.get("unit_type"), local_path):
                totals["mirrored_files"] += 1
        except OSError as exc:
            totals["errors"].append({"law_id": law_id, "file_id": file_id,
                                     "error": f"repo 미러 파일 실패(무시): {exc}"})
    consumed_ok = False
    try:
        # sha256 검증(있으면) — 불일치 파일은 전처리하지 않는다. 깨진 파일을 그대로 색인하면
        # 재시도해도 같은 file_id 아래 오염된 청크가 남을 수 있어 pending 으로 보류한다.
        expected = record.get("sha256")
        if expected and file_sha256(local_path) != expected:
            totals["files_pending"] += 1
            totals["errors"].append({"law_id": law_id, "file_id": file_id,
                                     "error": "sha256 불일치: 파일 전송 손상 가능, 전처리 보류"})
            return {"_pending": {"file_id": file_id, "reason": "sha256_mismatch",
                                 "file_name": record.get("file_name")}}
        try:
            chunks = run_doc_parser(
                ctx.doc_parser, local_path, ctx.settings.doc_parser_chunk_size,
                ctx.settings.doc_parser_chunk_overlap, ctx.settings.doc_parser_shared_host_dir,
                ctx.settings.doc_parser_shared_container_dir, ctx.settings.doc_parser_keep_temp_files,
                image_endpoint_path=ctx.settings.doc_parser_image_endpoint_path)
        except DocParserError as exc:
            totals["files_pending"] += 1
            # 결정오류(DRM/손상/빈 결과 — 원천 파일 한계)는 package 오류로 만들지 않는다.
            # 오류로 넣으면 이 package 가 재시도 끝에 격리되는데, 같은 파일은 몇 번을 다시
            # 소비해도 결과가 같다. 보류(pending)로만 남기고 package 는 성공 처리 —
            # 문서가 개정되면 새 파일이 새 package 로 오므로 그때 자동으로 다시 시도된다.
            if is_permanent_parser_error(exc):
                totals["files_unsupported"] = totals.get("files_unsupported", 0) + 1
                return {"_pending": {"file_id": file_id, "reason": "unsupported_source_file",
                                     "detail": f"[{exc.code}] {exc.message}",
                                     "file_name": record.get("file_name")}}
            totals["errors"].append({"law_id": law_id, "file_id": file_id, "error": f"[{exc.code}] {exc.message}"})
            return {"_pending": {"file_id": file_id, "reason": "preprocess_failed",
                                 "file_name": record.get("file_name")}}
        totals["files_preprocessed"] += 1
        if ctx.original:
            ctx.original.save_file(law_id, record.get("file_name") or local_path.name, local_path)
        meta = {
            "provision_id": record.get("provision_id"), "parent_provision_id": record.get("parent_provision_id"),
            "unit_type": record.get("unit_type") or "FILE", "unit_no": record.get("unit_no"),
            # 원문/첨부 구분(file_kind): provision_id(#BYL 등) 있으면 별표 첨부, 없으면 본문 파일.
            "file_kind": record.get("file_kind") or ("attachment" if record.get("provision_id") else "document_file"),
            "unit_title": record.get("unit_title"), "file_name": record.get("file_name") or local_path.name,
            "file_url": record.get("file_url"), "source_file_path": str(local_path),
        }
        consumed_ok = True
        return {"file_id": file_id, "meta": meta, "chunks": chunks}
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        elif consumed_ok and ctx.delete_consumed_files:
            # 옆채널(inbox)은 생산자와 공유하는 전달 영역이라 그냥 두면 계속 쌓인다.
            # 전처리까지 성공한 파일만 지운다 — 실패분은 남겨야 재시도·원인 확인이 된다.
            try:
                local_path.unlink(missing_ok=True)
            except OSError as exc:
                totals["errors"].append({"law_id": law_id, "file_id": file_id,
                                         "error": f"inbox 파일 삭제 실패(무시): {exc}"})


ARTICLE_IMAGE = "ARTICLE_IMAGE"      # 생산자 package.iter_article_image_units 와 같은 문자열


def _article_images_from_package(records: List[dict]) -> Dict[str, List[dict]]:
    """package 의 ARTICLE_IMAGE file record 를 조문번호별로 image_seq 순서로 모은다.

    생산자가 이미지를 package 에 실어 보내면(base64/옆채널) 소비자가 데이터 레포를 갖고
    있지 않아도 OCR 이 된다 — 무저장(STORAGE_MODE=db/manifest) 배포에서 유일하게 동작하는 경로."""
    by_article: Dict[str, List[dict]] = {}
    for r in records:
        if r.get("unit_type") != ARTICLE_IMAGE:
            continue
        by_article.setdefault(str(r.get("article_no") or ""), []).append(r)
    for arts in by_article.values():
        arts.sort(key=lambda r: int(r.get("image_seq") or 0))
    return by_article


def _ocr_package_image(ctx: _Context, record: dict) -> tuple:
    """ARTICLE_IMAGE record 하나를 전처리기(/preprocess_intelligent)로 OCR 한다.

    반환 (텍스트 또는 None, 일시오류인가). 바이트는 content_b64(자기완결) 우선,
    없으면 inbox 옆채널(transfer_name). 이미지 전용 엔드포인트를 쓴다 —
    문서용(/preprocess_attachment)과 모델이 다르다."""
    data = None
    if record.get("content_b64"):
        try:
            data = base64.b64decode(record["content_b64"])
        except (ValueError, TypeError):
            return None, False                       # 결정 실패 — 재시도해도 같다
    elif ctx.inbox_dir:
        p = Path(ctx.inbox_dir) / _safe(record.get("transfer_name") or record.get("file_name") or "image")
        if p.exists():
            data = p.read_bytes()
    if not data:
        return None, False
    tmp_dir = tempfile.mkdtemp(prefix="pkg_img_")
    try:
        local = Path(tmp_dir) / _safe(record.get("file_name") or "image.gif")
        local.write_bytes(data)
        return ocr_image_file(ctx.doc_parser, ctx.settings, local)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _apply_article_image_ocr(ctx: _Context, payload: dict, totals: dict,
                             image_records: Optional[List[dict]] = None) -> None:
    """조문의 `[그림]` 마커를 본문이미지 OCR 텍스트로 치환한다(STEP1).

    이미지 바이트를 얻는 경로가 둘이고, **package 로 온 것을 우선한다**:
      1) package 의 ARTICLE_IMAGE file record — 무저장(db/manifest) 배포에서 유일하게 동작.
      2) 로컬 git 데이터 레포의 `{문서}/본문이미지/…` — 생산자·소비자가 같은 파일시스템을
         공유하는 git/both 배포. 초기적재(pipeline.index_documents)와 같은 경로.
    2)만 있던 시절에는 무저장 배포에서 이미지가 디스크에 없어 OCR 이 통째로 건너뛰어지고
    `[그림]` 마커가 그대로 굳었다(실측: images_success 0)."""
    articles = (payload.get("body") or {}).get("articles") or []
    from_package = _article_images_from_package(image_records or [])
    for article in articles:
        if not isinstance(article, dict) or "[그림]" not in str(article.get("content") or ""):
            continue
        article_no = str(article.get("article_no") or "")
        recs = from_package.get(article_no) or []
        try:
            if recs:
                # package 경로 — 마커 순서대로 OCR 텍스트를 갈아 끼운다.
                ocr = [_ocr_package_image(ctx, r) for r in recs]
                new_content, success, failed, transient_failed = substitute_image_markers(
                    str(article.get("content") or ""), ocr)
            else:
                new_content, success, failed, transient_failed = resolve_article_images(
                    ctx.doc_parser, ctx.settings,
                    # 학칙공단 문서의 본문이미지는 SCHLPUBRUL 레포에 있다 — doc_target 으로 레포를 고른다.
                    ctx.settings.repo_for(payload.get("doc_target"), ctx.source), payload, article)
        except Exception as exc:
            totals["errors"].append({
                "law_id": payload.get("law_id"), "article": str(article.get("article_no")),
                "error": f"본문이미지 처리 실패(원문 [그림] 마커 유지): {exc}",
            })
            continue
        totals["images_success"] += success
        totals["images_failed"] += failed
        # 일시 오류(전처리기 다운·timeout)로 못 읽은 그림은 조용히 마커로 굳히면 안 된다 —
        # 오류로 남겨 package 재시도(→성공 시 OCR 텍스트 회복)에 태운다. 결정 실패(못 읽는
        # 글리프 류)는 마커 유지가 최종 상태라 카운터로만 남긴다.
        if transient_failed:
            totals["errors"].append({
                "law_id": payload.get("law_id"), "article": str(article.get("article_no")),
                "error": f"본문이미지 OCR 일시 오류 {transient_failed}건 — 재시도 대상",
            })
        if new_content is not None:
            article["content"] = new_content


def _flush_law(ctx: _Context, store, embedder: LocalEmbedder, buffer: _LawBuffer, totals: dict) -> None:
    """한 law_id 버퍼를 재적재한다.

    ⚠️ 순서가 안전의 핵심이다: **새 버전을 upsert 먼저 → 성공 후에 옛 버전 청크만 삭제.**
    (예전엔 delete_by_law_id 로 먼저 싹 지우고 재적재했는데, embed/upsert 가 OOM·일시장애로
    실패하면 옛 청크는 이미 사라졌고 새 청크는 안 들어가 그 법이 검색에서 통째로 없어졌다.)
    이제 upsert 가 실패하면 옛 청크가 그대로 살아 있어 검색이 유지되고, package 는 failed/ 로
    가서 재시도된다. 명시적 폐지(delete·내용 없음)만 예외로 전량 삭제한다."""
    law_id = buffer.law_id

    # 순수 폐지(delete 이고 재적재할 내용 없음) = 전량 삭제하고 끝. (내용이 있으면 개정으로 보고 아래 upsert 경로)
    if buffer.delete and not buffer.has_content():
        try:
            store.delete_by_law_id(law_id, ctx.collection)
        except Exception as exc:
            totals["errors"].append({"law_id": law_id, "error": f"삭제 실패: {exc}"})
        totals["deleted_docs"] += 1
        return
    if not buffer.has_content():
        # pending 만 있고 본문도 없음 — 첨부 보류만 기록(본문은 건드리지 않음)
        totals["pending_attachments"] += len(buffer.pending)
        return

    law_meta = _law_meta(buffer.document, law_id)
    git_path = buffer.document_git_path or f"package:{law_id}"
    law_meta["git_path"] = law_meta.get("git_path") or buffer.document_git_path
    objects: List[LegalProvision] = []
    # ARTICLE_IMAGE record 는 **색인할 청크가 아니라** 조문 content 의 `[그림]` 치환 재료다 —
    #   첨부 전처리(_preprocess_file_record)로 보내면 이미지가 별개 FILE 청크로 들어가 버린다.
    image_records = [r for r in buffer.files if r.get("unit_type") == ARTICLE_IMAGE]
    attach_records = [r for r in buffer.files if r.get("unit_type") != ARTICLE_IMAGE]
    try:
        if buffer.document is not None:
            # 본문 [그림] OCR 은 행정규칙류(학칙공단 포함)만 — 법령은 표·수식이 이미 텍스트로 있다.
            if ctx.source != "law" and ctx.doc_parser is not None:
                _apply_article_image_ocr(ctx, buffer.document, totals, image_records)
            objects.extend(ctx.mapper_fn(buffer.document, Path(git_path),
                                         source_repository=ctx.source_repository))
            totals["documents"] += 1
            if ctx.original:
                ctx.original.save_document(law_id, law_meta.get("version_uid"), buffer.document)
            if ctx.mirror is not None and ctx.mirror.save_document(
                    buffer.document_git_path, buffer.document):
                totals["mirrored_docs"] += 1
        else:
            totals["errors"].append({"law_id": law_id,
                                     "error": "경고: document 없이 청크/파일만 왔습니다(package 계약 위반)"})

        if buffer.normalized:
            objects.extend(_build_normalized(law_meta, ctx.source, buffer.normalized, git_path))
            totals["normalized_chunks"] += len(buffer.normalized)

        # file record → 내부 전처리(켜져 있으면) → preprocessed 와 같은 경로로 합류
        for record in attach_records:
            outcome = _preprocess_file_record(ctx, law_id, record, totals, buffer.document_git_path)
            if outcome is None:
                continue
            if "_pending" in outcome:
                buffer.pending.append(outcome["_pending"])
                continue
            buffer.preprocessed[outcome["file_id"]] = {"meta": outcome["meta"], "chunks": outcome["chunks"]}

        for file_id, group in buffer.preprocessed.items():
            chunks = group["chunks"]
            if not chunks:
                continue
            attach_input = _attachment_input(law_meta, ctx.source, file_id, group["meta"], chunks)
            objects.extend(map_attachment_data(attach_input, Path(git_path), collection_type=ctx.source))
            totals["preprocessed_chunks"] += len(chunks)

        totals["pending_attachments"] += len(buffer.pending)

        if objects:
            for obj in objects:
                obj.git_commit = obj.git_commit or law_meta.get("git_commit")
            result = _embed_and_upsert(store, embedder, objects, ctx.collection)   # ① 새 버전 upsert 먼저
            totals["objects_success"] += result["success"]
            totals["objects_failed"] += result["failed"]
            if result["failed"]:
                totals["errors"].append({"law_id": law_id, "chunk_ids": result["failed_ids"]})
            # ② 새 버전이 **전부** 성공했을 때만(failed==0) 옛 버전 청크 정리. upsert 는 partial 실패에
            #    예외를 안 던지고 {success, failed} 만 주므로, 하나라도 실패했는데 옛 청크를 지우면
            #    새 청크가 일부 빠진 채 옛 것까지 없어져 그 법에 구멍이 생긴다. 실패면 옛 청크를 남겨
            #    두고 package 는 failed/ 로 재시도된다(그 법이 검색에서 사라지는 창 제거).
            keep_vuids = {getattr(o, "version_uid", None) for o in objects}
            keep_vuids.discard(None)
            if keep_vuids and result["failed"] == 0:
                try:
                    removed = store.delete_stale_law_chunks(law_id, list(keep_vuids), ctx.collection)
                    if removed:
                        totals["stale_removed"] = totals.get("stale_removed", 0) + removed
                except Exception as exc:
                    totals["errors"].append({"law_id": law_id, "error": f"옛 버전 청크 정리 실패(무시): {exc}"})
        # 이 법의 문서+파일을 미러에 다 쓴 뒤 커밋(push 모드일 때만). 메시지는 수집기와 동일.
        if ctx.mirror is not None and ctx.mirror.commit(buffer.commit_message):
            totals["mirror_commits"] = totals.get("mirror_commits", 0) + 1
        totals["laws_indexed"] += 1
    except Exception as exc:
        totals["laws_failed"] += 1
        totals["errors"].append({"law_id": law_id, "error": str(exc)})


def _add_record(buffers: Dict[str, _LawBuffer], record: dict, totals: dict) -> None:
    """record_type 을 해당 law_id 버퍼에 쌓는다(임베딩·삭제는 flush 단계에서)."""
    rtype = record.get("record_type")
    law_id = record.get("law_id") or record.get("doc_id")
    if not law_id and rtype != DOCUMENT:
        totals["errors"].append({"error": f"law_id 없는 {rtype} record 건너뜀"})
        return
    if rtype == DOCUMENT and not law_id:
        payload = record.get("payload") or {}
        law_id = payload.get("law_id")
        if not law_id:
            totals["errors"].append({"error": "document record 에 law_id 가 없습니다"})
            return
    buffer = buffers.setdefault(law_id, _LawBuffer(law_id=law_id))

    if rtype == DOCUMENT:
        if record.get("op") == "delete":
            buffer.delete = True
            return
        buffer.document = record.get("payload")
        buffer.document_git_path = record.get("git_path")
        buffer.commit_message = record.get("commit_message")
    elif rtype == DELETE:
        buffer.delete = True
    elif rtype == NORMALIZED_CHUNK:
        buffer.normalized.append(record)
    elif rtype == PREPROCESSED_CHUNK:
        file_id = record.get("file_id") or "ATTACHMENT"
        meta = record.get("metadata") or {}
        group = buffer.preprocessed.setdefault(file_id, {"meta": {
            "provision_id": meta.get("provision_id"), "parent_provision_id": meta.get("parent_provision_id"),
            "unit_type": meta.get("unit_type"), "unit_no": meta.get("unit_no"),
            "unit_title": meta.get("unit_title"), "file_name": meta.get("file_name"),
            "file_url": meta.get("file_url"), "source_file_path": meta.get("source_file_path"),
        }, "chunks": []})
        group["chunks"].append({
            "content": record.get("content", ""), "chunk_index": record.get("chunk_index"),
            "page_no": meta.get("page_no"), "start_page": meta.get("start_page"),
            "end_page": meta.get("end_page"), "chunk_bboxes": meta.get("chunk_bboxes"),
            "media_files": meta.get("media_files"), "guardrail_categories": meta.get("guardrail_categories"),
            "n_char": meta.get("n_char"), "n_word": meta.get("n_word"), "n_line": meta.get("n_line"),
        })
    elif rtype == FILE:
        buffer.files.append(record)
    elif rtype == PENDING_ATTACHMENT:
        buffer.pending.append({"file_id": record.get("file_id"), "reason": record.get("reason"),
                               "file_name": record.get("file_name")})
    else:
        totals["errors"].append({"error": f"알 수 없는 record_type: {rtype}"})


def _rename_prefix(doc_target: Optional[str]) -> str:
    """rename record 의 doc_target → reference_id 접두(수집기 id_prefix 와 같은 분류)."""
    t = str(doc_target or "")
    if t in ("", "eflaw", "law"):
        return "law"
    if t == "admrul":
        return "admrul"
    return "SchlPubRul"                                    # school/pi/public


def _apply_rename(ctx: _Context, routed: "_RoutedStores", settings: Settings,
                  buffers: Dict[str, _LawBuffer], record: dict, totals: dict) -> None:
    """rename record 1건 처리 — 문서 이름이 바뀌었다는 수집기 통지.

    개명된 문서 **자체**는 수집기가 재수집해 같은(늦어도 다음) package 의 document 로 보내므로
    여기서 재적재하지 않는다. 여기서 하는 일:
    ① 개명 문서 자신의 옛 이름 청크 잔존분 정리(버전이 안 바뀐 개명은 stale 정리가 못 잡는다).
    ② 동명 가드 — 옛 이름을 그대로 쓰는 다른 문서가 남아 있으면 참조 패치를 건너뛴다.
    ③ 그 문서를 **참조하던** 청크들의 reference_ids/meta 를 새 이름 기준으로 패치(벡터 보존).
       어느 문서종이든 개명 문서를 인용할 수 있어 세 컬렉션을 전부 훑는다(이 배포에 없는
       컬렉션은 store 가 조용히 스킵).
    ④ 미러(RepoMirror)의 옛 문서 폴더 정리 — 경로는 생산자가 실어 준 old_dir/new_dir 우선,
       없으면(수집기가 파일트리 없는 manifest 모드) 같은 package 의 document git_path 에서 역산.
    멱등 — 같은 record 를 다시 받아도(재발송) 두 번째는 바꿀 것이 없다."""
    old, new = record.get("old_name"), record.get("new_name")
    if not old or not new or old == new:
        return
    compact_old, compact_new = _compact_name(old), _compact_name(new)
    prefix = _rename_prefix(record.get("doc_target"))
    law_id = record.get("law_id") or ""
    patched_refs = 0
    if compact_old and compact_new and compact_old != compact_new:
        old_head, new_head = f"{prefix}:{compact_old}", f"{prefix}:{compact_new}"
        eff = settings.source_for_doc(record.get("doc_target"), law_id, default=ctx.source)
        eff_coll = settings.collection_for(eff)
        ambiguous = False
        try:
            eff_store = routed.get(eff)
            # ① 개명 문서 자신의 옛 이름 청크 정리 — 새 버전(MST) 없는 개명은 version_uid 가
            #    그대로라 flush 의 stale 정리가 못 잡고 옛/새 이름 청크가 중복 공존한다.
            if law_id:
                removed = eff_store.delete_renamed_self_chunks(law_id, old_head, eff_coll)
                if removed:
                    totals["stale_removed"] = totals.get("stale_removed", 0) + removed
            # ② 동명 가드 — 정리 후에도 옛 머리를 가진 청크가 남아 있으면 옛 이름을 그대로 쓰는
            #    **다른** 문서(동명)가 살아 있다는 뜻. 그 이름을 인용하던 참조는 원래도 어느 쪽인지
            #    확정 불가(enrich 가 동명 후보 목록으로 전달)라, 새 이름으로 돌리면 남은 문서를
            #    가리키던 참조까지 바뀐다 → 참조 패치를 건너뛴다(미러 정리는 그대로 한다).
            ambiguous = eff_store.has_provision_head(old_head, eff_coll)
        except Exception:
            pass                                           # 가드 실패 = 가드 없이 진행(best-effort)
        if ambiguous:
            totals.setdefault("rename_ambiguous", []).append(
                {"law_id": law_id, "old_name": old, "new_name": new})
        else:
            for src in ("law", "admrul", "schlpub"):
                try:
                    src_store = routed.get(src)
                except Exception:
                    # 이 배포에 그 컬렉션 연결이 없다(키 미설정 등) — 그 컬렉션엔 이 배포가 관리하는
                    # 참조자도 없다고 보고 건너뛴다. 오류로 만들면 package 가 영구 재시도에 빠진다.
                    skipped = totals.setdefault("rename_patch_skipped", [])
                    if src not in skipped:
                        skipped.append(src)
                    continue
                try:
                    result = src_store.patch_renamed_references(
                        old_head, new_head, old, new, settings.collection_for(src))
                    patched_refs += result["refs"]
                except Exception as exc:
                    totals["errors"].append({
                        "law_id": law_id,
                        "error": f"개명 참조 패치 실패({src}: {old} → {new}): {exc}"})
    if ctx.mirror is not None:
        old_dir, new_dir = record.get("old_dir"), record.get("new_dir")
        buffer = buffers.get(record.get("law_id") or "")
        doc_gp = buffer.document_git_path if buffer else None
        if not new_dir and doc_gp:
            new_dir = str(PurePosixPath(doc_gp).parent)
        if not old_dir and doc_gp and new in doc_gp:
            old_dir = str(PurePosixPath(doc_gp.replace(new, old)).parent)
        try:
            if old_dir and ctx.mirror.rename_dir(record.get("doc_target"), old_dir, new_dir):
                ctx.mirror.commit(f"chore: 개명 폴더 정리 — {old} → {new}")
        except Exception as exc:
            totals["errors"].append({"law_id": record.get("law_id"),
                                     "error": f"개명 미러 정리 실패({old} → {new}): {exc}"})
    totals["renames"] += 1
    totals["rename_refs_patched"] += patched_refs


def consume_package(store, embedder: LocalEmbedder, settings: Settings, package_path: Path,
                    source_override: Optional[str] = None,
                    doc_parser: Optional[DocParserClient] = None) -> dict:
    """JSONL package를 소비해 Weaviate에 적재한다.

    - source는 package_header의 source를 우선하고, 없으면 source_override(CLI --source)를 쓴다.
    - law_id별로 record를 버퍼링했다가 EOF/footer에서 flush한다.
    - `file` record는 package_preprocess_files가 켜져 있을 때 내부 전처리하고, 아니면 보류한다.
    - package_footer의 record_count를 실제 소비 수와 대조한다.
    """
    totals = _new_totals()
    buffers: Dict[str, _LawBuffer] = {}
    renames: List[dict] = []
    header: Optional[dict] = None
    footer: Optional[dict] = None
    consumed = 0

    with open(Path(package_path), encoding="utf-8") as handle:
        for lineno, raw in enumerate(handle, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                totals["errors"].append({"line": lineno, "error": f"JSONL 파싱 실패: {exc}"})
                continue
            rtype = record.get("record_type")
            if rtype == HEADER:
                header = record
                continue
            if rtype == FOOTER:
                footer = record
                continue
            consumed += 1
            if rtype == RENAME:
                # 개명 통지 — law_id 버퍼가 아니라 별도 처리(참조자 패치·미러 정리).
                # 문서 flush 가 다 끝난 뒤 적용한다(개명 문서의 재적재본이 새 경로에 먼저 쓰이게).
                renames.append(record)
                continue
            _add_record(buffers, record, totals)

    source = (header or {}).get("source") or source_override
    if source not in ("law", "admrul", "schlpub"):
        raise ValueError(
            f"source 를 결정할 수 없습니다(header.source={((header or {}).get('source'))!r}, "
            f"--source={source_override!r}). package_header 에 source 를 넣거나 --source 를 지정하세요.")

    collection = settings.collection_for(source)
    original = None
    if settings.package_store_original and settings.package_original_dir:
        original = OriginalStore(settings.package_original_dir, source)
    mirror = None
    if settings.package_mirror_repo:
        # 레포 3개(LAW/ADMRUL/SCHLPUBRUL) 중 문서마다 맞는 곳에 쓴다.
        mirror = RepoMirrorSet(settings, source, push=settings.package_mirror_push)
    ctx = _Context(
        source=source, collection=collection,
        mapper_fn=map_law_data if source == "law" else map_admrul_data,
        source_repository=_repo_url_for(settings, source),
        preprocess_files=settings.package_preprocess_files,
        inbox_dir=settings.package_file_inbox_dir,
        delete_consumed_files=settings.package_delete_consumed_files,
        mirror=mirror,
        doc_parser=doc_parser or (DocParserClient(
            settings.doc_parser_base_url, settings.doc_parser_timeout, settings.doc_parser_max_retries,
            endpoint_path=settings.doc_parser_endpoint_path, api_key=settings.doc_parser_api_key,
            upload=settings.doc_parser_upload)
            if settings.package_preprocess_files else None),
        settings=settings, original=original,
    )

    # 문서별 컬렉션 라우팅 — admrul package 에 섞여 온 학칙공단 문서(doc_target=school/pi/public,
    # delete 는 law_id 접두)는 schlpub 컬렉션으로 보낸다. 그 외에는 package source 그대로.
    routed = _RoutedStores(settings, store, source)
    ctx_by_source: Dict[str, _Context] = {source: ctx}
    try:
        for buffer in buffers.values():
            eff = settings.source_for_doc((buffer.document or {}).get("doc_target"),
                                          buffer.law_id, default=source)
            b_ctx = ctx_by_source.get(eff)
            if b_ctx is None:
                # mirror·doc_parser·original 등 나머지 자원은 공유한다(RepoMirrorSet 이 doc_target
                # 으로 레포를 스스로 고르므로 컨텍스트 분기는 컬렉션·mapper·레포URL 만이다).
                b_ctx = replace(ctx, source=eff, collection=settings.collection_for(eff),
                                mapper_fn=map_law_data if eff == "law" else map_admrul_data,
                                source_repository=_repo_url_for(settings, eff))
                ctx_by_source[eff] = b_ctx
            _flush_law(b_ctx, routed.get(eff), embedder, buffer, totals)
        for record in renames:
            _apply_rename(ctx, routed, settings, buffers, record, totals)
    finally:
        routed.close()

    # 이 package 의 모든 법을 커밋한 뒤, 내부 git 서버로 한 번에 push(push 모드일 때만).
    if ctx.mirror is not None and ctx.mirror.push():
        totals["mirror_pushed"] = True

    totals["source"] = source
    totals["package_id"] = (header or {}).get("package_id")
    if footer is not None and footer.get("record_count") is not None:
        if footer["record_count"] != consumed:
            totals["errors"].append({
                "error": f"footer record_count({footer['record_count']}) != 실제 소비({consumed})"})
    elif header is not None:
        # 주의: 생산자는 항상 footer 를 쓴다(emit_package). header 는 있는데 footer 가 없다는 것은
        # **뒤가 잘린** package 라는 뜻이다 — 종전엔 대조 자체를 건너뛰어 잘린 package 의 남은
        # 문서들이 조용히 누락된 채 "정상 소비"로 통과했다. 오류로 남긴다 — 소비자는 일시장애로
        # 재시도하고(복사 중이던 파일이면 다음 시도에 완성본을 본다) 임계 초과 시 격리한다.
        totals["errors"].append({
            "error": f"footer 없음 — 전송 중 잘린 package 의심(소비 {consumed}건까지만 확인됨)"})
    # 전송 채널(공유폴더/NFS/MinIO)에 쌓이지 않게: 무오류 소비 완료 후 package JSONL 삭제(옵션).
    # 오류가 하나라도 있으면 재시도 위해 남긴다. 소비는 멱등이라 재실행 안전.
    if settings.package_delete_consumed_package and not totals["errors"]:
        try:
            Path(package_path).unlink(missing_ok=True)
            totals["package_deleted"] = True
        except OSError as exc:
            totals["errors"].append({"error": f"package 삭제 실패(무시): {exc}"})
    return totals
