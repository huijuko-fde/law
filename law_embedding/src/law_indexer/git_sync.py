"""법령/행정규칙 데이터 저장소(law_data · admrul_data) clone/fetch.

`git_history`(../git_history)는 특정 시점 조회 전용이라 저장소 clone/fetch 는 다루지 않는다
(그 모듈 docstring 에 명시된 의도적 스코프). law_data·admrul_data 를 로컬에 준비/최신화하는
책임은 이 색인기가 새로 진다 — CLAUDE.md 가 "재색인 트리거: git pull 후 index, 아직 미구현"
이라고 밝힌 부분을 채우는 모듈이다.

이 저장소는 색인기 입장에서 읽기 전용 미러다(수집기 쪽이 쓰기 주체) — 정상 운영에서는 로컬에
아무도 손을 대지 않아야 한다. 그렇다고 해서 `reset --hard origin/<branch>` 로 강제 동기화하면
안 된다: 그 명령은 커밋 안 된 로컬 변경(예: 디버깅 중 손으로 고친 파일, 전처리기가 부산물로
남긴 파일)과 origin 에 없는 로컬 커밋을 **경고 없이 되돌릴 수 없게 버린다**. "읽기 전용이어야
한다"는 건 우리가 지킬 불변식이지, 실제로 그런지 매번 강제로 확인·집행할 권한까지는 아니다.
그래서 fetch 뒤에는 `merge --ff-only` 만 쓴다 — 로컬이 origin 의 조상(순수 fast-forward)일 때만
조용히 앞으로 이동하고, 로컬에 커밋 안 된 변경이 있거나 origin 과 이력이 갈라졌으면(예: 누군가
로컬에서 커밋했거나, 예상 밖으로 강제 push 된 경우) **아무것도 건드리지 않고 그대로 실패**한다
— 그러면 사람이 원인을 보고 판단해야 한다(자동으로 덮어쓰지 않는다).
"""
import os
import subprocess
from pathlib import Path
from typing import Optional


class GitSyncError(Exception):
    """저장소 clone/fetch 실패."""


# 리눅스/NFS 는 경로요소 한 개가 255바이트를 넘으면 파일을 못 만든다(ext4). git 에는 수집기가 쓴
# 원본 이름(한글 100자면 ~300바이트)이 그대로 들어있어, 그런 파일은 리눅스에서 checkout 이
# '파일명 너무 김'으로 실패한다 — git 원본은 유지한 채(정책 B) 그 파일만 safe 이름으로 로컬에
# 풀어(materialize) 색인이 찾게 한다. preprocess._safe_name 의 임계값과 반드시 같아야 한다.
_MAX_PATH_BYTES = 255


def _run(args: list, timeout: int) -> subprocess.CompletedProcess:
    """git subprocess 실행. 인자는 배열로만 전달(shell=True 금지), 인증 프롬프트로 인한 hang 방지."""
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        return subprocess.run(args, capture_output=True, text=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise GitSyncError(f"git 명령 타임아웃({timeout}s): {' '.join(args[:3])}") from exc
    except FileNotFoundError as exc:
        raise GitSyncError(f"git 실행 파일을 찾을 수 없습니다: {exc}") from exc


def current_commit(path: Path) -> Optional[str]:
    """로컬 저장소의 현재 HEAD commit hash. 저장소가 아니거나 조회 실패하면 None."""
    proc = _run(["git", "-C", str(path), "rev-parse", "HEAD"], timeout=30)
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _add_local_exclude(repo: Path, rel: str) -> None:
    """rel 을 .git/info/exclude 에 추가 — materialize 한 safe 사본이 어떤 'git add'(미러의 add -A
    포함)에도 스테이징되지 않게 한다(정책 B: git 엔 원본만). exclude 는 .git 안이라 커밋되지 않고
    레포마다 로컬이다. gitignore 메타문자(*?[]\\)만 이스케이프하고 루트('/')로 앵커한다."""
    esc = "/" + "".join(("\\" + c if c in "*?[]\\" else c) for c in rel)
    try:
        info = Path(repo) / ".git" / "info"
        info.mkdir(parents=True, exist_ok=True)
        excl = info / "exclude"
        lines = excl.read_text(encoding="utf-8").splitlines() if excl.exists() else []
        if esc in lines:
            return
        with open(excl, "a", encoding="utf-8") as fh:
            fh.write(esc + "\n")
    except OSError:
        pass


def _too_long_only(path: Path, proc: subprocess.CompletedProcess) -> bool:
    """clone/merge 가 '파일명 너무 김' 때문에만 실패했는지 판정한다. 오브젝트·HEAD 는 이미 받아졌고
    (HEAD 가 해석되고) stderr 가 길이 오류를 말할 때만 True — 네트워크 실패나 이력 분기(non-ff)면
    stderr 에 그 문자열이 없어 False 라, 그런 실패는 그대로 예외로 올린다(관용하지 않는다)."""
    s = proc.stderr or ""
    long_err = ("File name too long" in s) or ("Filename too long" in s) or ("file name too long" in s)
    return bool(long_err) and current_commit(path) is not None


def materialize_long_paths(path: Path, timeout: int = 300) -> int:
    """작업트리에 못 쓴(경로요소 255바이트 초과) tracked 파일을 safe 이름으로 로컬에 풀어 둔다.
    git 은 원본 경로를 그대로 유지하고(index/커밋 불변, 정책 B), 리눅스 디스크에만 safe 사본을 둔다.
      - safe 이름 규칙은 색인기 preprocess._safe_name 과 동일 → 색인이 그 이름으로 파일을 찾는다.
      - 원본(긴) 경로는 skip-worktree 로 표시해 'deleted' 노이즈와 이후 fetch 의 머지 충돌을 막는다.
      - 사본은 매 동기화마다 덮어써(overwrite) 내용이 바뀌어도 최신을 유지한다(그 파일만, 소수).

    **열거·blob 조회는 index 가 아니라 TREE(HEAD) 기준이다.** checkout 이 실패한 clone 은 index 가
    비어 `ls-files`/`:경로` 가 안 먹는 걸 리눅스 실측으로 확인 → `ls-tree`/`rev-parse HEAD:경로`/
    `cat-file <sha>` 로 트리에서 직접 읽는다(복구 경로가 read-tree 로 index 를 채워 skip-worktree 도 됨).
    반환: materialize 한 파일 수. blob 은 바이너리라 text=False 로 파일에 직접 흘려 쓴다."""
    from .preprocess import _safe_name                          # 명명 규칙 단일 출처
    proc = _run(["git", "-C", str(path), "ls-tree", "-r", "--name-only", "-z", "HEAD"], timeout=timeout)
    if proc.returncode != 0:
        return 0
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    count = 0
    for rel in proc.stdout.split("\0"):
        if not rel:
            continue
        segs = rel.split("/")
        if not any(len(s.encode("utf-8")) > _MAX_PATH_BYTES for s in segs):
            continue                                            # 리눅스가 쓸 수 있는 이름 → 손대지 않음
        # 원본(긴) 경로 skip-worktree — index 에 있을 때만 먹지만(복구 경로가 read-tree 로 채움),
        # 안 먹어도 아래 safe 사본은 트리 sha 로 만들어지므로 색인엔 지장 없다.
        _run(["git", "-C", str(path), "update-index", "--skip-worktree", "--", rel], timeout=timeout)
        rp = _run(["git", "-C", str(path), "rev-parse", f"HEAD:{rel}"], timeout=timeout)
        sha = (rp.stdout or "").strip()
        if rp.returncode != 0 or not sha:
            continue
        safe_rel = "/".join(_safe_name(s) for s in segs)
        dst = Path(path) / safe_rel
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            with open(dst, "wb") as fh:                         # 트리 blob(sha)을 그대로 흘려 씀
                cp = subprocess.run(
                    ["git", "-C", str(path), "cat-file", "blob", sha],
                    stdout=fh, stderr=subprocess.PIPE, env=env, timeout=timeout)
            if cp.returncode != 0:
                dst.unlink(missing_ok=True)
                continue
        except (subprocess.TimeoutExpired, OSError):
            continue
        _add_local_exclude(path, safe_rel)                     # safe 사본은 git add 대상에서 영구 제외
        count += 1
    return count


def _recover_long_names(path: Path, timeout: int = 300) -> int:
    """clone/merge 가 긴 이름으로 checkout 을 끝내지 못했을 때의 복구(리눅스 실측으로 검증한 순서):
      1) read-tree HEAD — 실패한 clone 은 index 가 비어 있어 HEAD 로 채운다(skip-worktree 전제).
      2) materialize_long_paths — 긴 경로 skip-worktree + safe 사본 + exclude.
      3) checkout -- . — checkout 이 첫 긴 파일에서 멈춰 못 받은 나머지 정상 파일을 마저 채운다
         (긴 건 skip-worktree 라 다시 '너무 김' 나지 않는다)."""
    _run(["git", "-C", str(path), "read-tree", "HEAD"], timeout=timeout)
    mat = materialize_long_paths(path, timeout=timeout)
    _run(["git", "-C", str(path), "checkout", "--", "."], timeout=timeout)
    return mat


def sync_repo(url: str, path: Path, branch: str = "main", timeout: int = 300) -> dict:
    """path 에 저장소가 없으면 clone, 있으면 fetch + fast-forward-only 로 최신화한다.

    fast-forward 가 안 되면(로컬에 커밋 안 된 변경이 충돌하거나 이력이 갈라졌으면) 아무것도
    지우지 않고 GitSyncError 를 낸다 — 그 경우는 사람이 저장소를 직접 확인해야 한다.

    반환: {"action": "cloned"|"fetched", "path": str, "commit": str|None}
    호출부(CLI)가 두 저장소(law_data/admrul_data)를 각각 독립적으로 처리할 수 있도록,
    이 함수 자체는 한 저장소만 다룬다.
    """
    path = Path(path)
    if not (path / ".git").exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        proc = _run(["git", "clone", "--branch", branch, "--single-branch", url, str(path)], timeout=timeout)
        # '파일명 너무 김'만으로 실패한 거면 오브젝트·HEAD 는 받아졌고 작업트리의 그 파일만 못 쓴 것
        # → 예외 대신 복구(read-tree+materialize+checkout)한다(정책 B). 그 외 실패(네트워크 등)면 예외.
        tolerated = False
        if proc.returncode != 0:
            if not _too_long_only(path, proc):
                raise GitSyncError(f"clone 실패 ({url} -> {path}): {proc.stderr.strip()}")
            tolerated = True
        mat = _recover_long_names(path, timeout) if tolerated else materialize_long_paths(path, timeout)
        return {"action": "cloned", "path": str(path), "commit": current_commit(path), "materialized": mat}

    proc = _run(["git", "-C", str(path), "fetch", "origin", branch], timeout=timeout)
    if proc.returncode != 0:
        raise GitSyncError(f"fetch 실패 ({path}, branch={branch}): {proc.stderr.strip()}")

    # 로컬 브랜치가 이미 있으면 그대로 두고, 없을 때만(첫 clone 이 아닌 특수한 경우) origin 을
    # 추적하는 새 로컬 브랜치를 만든다. -B(강제 이동)를 쓰지 않는다 — 이미 있는 로컬 브랜치를
    # origin 위치로 강제로 되감으면 그 브랜치의 로컬 커밋이 조용히 사라질 수 있다.
    proc = _run(["git", "-C", str(path), "checkout", branch], timeout=timeout)
    if proc.returncode != 0:
        proc = _run(["git", "-C", str(path), "checkout", "-b", branch, f"origin/{branch}"], timeout=timeout)
        if proc.returncode != 0:
            raise GitSyncError(f"checkout 실패 ({path}, branch={branch}): {proc.stderr.strip()}")

    # fast-forward-only: 되돌릴 게 없을 때만 조용히 전진한다. 로컬 변경/분기 이력이 있으면
    # 실패하고 아무것도 건드리지 않는다(git 자체의 안전장치 — 여기서 따로 강제하지 않는다).
    proc = _run(["git", "-C", str(path), "merge", "--ff-only", f"origin/{branch}"], timeout=timeout)
    # non-ff/로컬변경이면 stderr 에 길이 오류가 없어 그대로 예외(덮어쓰지 않음). 새로 온 >255바이트
    # 파일 때문에 ff checkout 만 실패한 거면 관용하고 복구로 흡수한다.
    tolerated = False
    if proc.returncode != 0:
        if not _too_long_only(path, proc):
            raise GitSyncError(
                f"fast-forward 갱신 실패 ({path}, branch={branch}): 로컬에 커밋되지 않은 변경이 있거나 "
                f"origin 과 이력이 갈라졌을 수 있습니다. 자동으로 덮어쓰지 않았으니 직접 확인하세요. "
                f"stderr={proc.stderr.strip()}"
            )
        tolerated = True
        # ⚠ merge 는 원자적이라 긴 이름 checkout 에서 멈추면 **HEAD/브랜치가 옛 커밋에 남는다.**
        #   그대로 _recover_long_names 를 돌리면 옛 HEAD 를 "복구 성공"으로 보고해 소비자가
        #   구버전 데이터를 계속 읽는다(코드스페이스 실측 — sync 가 fetched@옛커밋 반환).
        #   원본을 안 버리는 전진: 브랜치 포인터만 origin 으로 옮기고(reset --soft — ff 확인은
        #   위 merge --ff-only 가 이미 했다), index·작업트리는 아래 복구가 새 HEAD 로 채운다.
        rs = _run(["git", "-C", str(path), "reset", "--soft", f"origin/{branch}"], timeout=timeout)
        if rs.returncode != 0:
            raise GitSyncError(f"긴 이름 복구 중 reset 실패 ({path}): {rs.stderr.strip()}")
    # 정상 fetch 는 materialize 만(그 소수 파일 safe 사본 갱신, 값쌈). checkout 이 실패했으면 복구.
    mat = _recover_long_names(path, timeout) if tolerated else materialize_long_paths(path, timeout)
    return {"action": "fetched", "path": str(path), "commit": current_commit(path), "materialized": mat}
