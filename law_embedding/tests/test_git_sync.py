import subprocess

import pytest

from law_indexer import git_sync


def _git(*args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _make_origin(tmp_path):
    """origin 역할을 할 로컬 bare 저장소 + 한 번 커밋된 워킹 카피를 만든다(네트워크 불필요)."""
    origin = tmp_path / "origin.git"
    origin.mkdir()
    _git("init", "--bare", "-b", "main", cwd=origin)

    seed = tmp_path / "seed"
    seed.mkdir()
    _git("init", "-b", "main", cwd=seed)
    _git("config", "user.email", "test@example.com", cwd=seed)
    _git("config", "user.name", "Test", cwd=seed)
    (seed / "law.json").write_text("{}", encoding="utf-8")
    _git("add", "law.json", cwd=seed)
    _git("commit", "-m", "initial", cwd=seed)
    _git("remote", "add", "origin", str(origin), cwd=seed)
    _git("push", "origin", "main", cwd=seed)
    return origin, seed


def test_sync_repo_clones_when_missing(tmp_path):
    origin, _seed = _make_origin(tmp_path)
    dest = tmp_path / "law_data"

    result = git_sync.sync_repo(str(origin), dest, branch="main")

    assert result["action"] == "cloned"
    assert (dest / "law.json").exists()
    assert result["commit"] and len(result["commit"]) == 40


def test_sync_repo_fetches_when_already_cloned(tmp_path):
    origin, seed = _make_origin(tmp_path)
    dest = tmp_path / "law_data"
    first = git_sync.sync_repo(str(origin), dest, branch="main")

    # origin 쪽에 새 커밋을 추가한 뒤 다시 동기화하면 fetch 로 최신화돼야 한다.
    (seed / "law.json").write_text('{"v": 2}', encoding="utf-8")
    _git("commit", "-am", "update", cwd=seed)
    _git("push", "origin", "main", cwd=seed)

    second = git_sync.sync_repo(str(origin), dest, branch="main")

    assert second["action"] == "fetched"
    assert second["commit"] != first["commit"]
    assert (dest / "law.json").read_text(encoding="utf-8") == '{"v": 2}'


def test_sync_repo_leaves_untouched_local_drift_when_nothing_new(tmp_path):
    """origin 에 새 커밋이 없으면 fast-forward 할 것도 없어 로컬 변경에 손대지 않는다."""
    origin, _seed = _make_origin(tmp_path)
    dest = tmp_path / "law_data"
    git_sync.sync_repo(str(origin), dest, branch="main")

    (dest / "law.json").write_text("불필요한 로컬 수정", encoding="utf-8")

    result = git_sync.sync_repo(str(origin), dest, branch="main")

    assert result["action"] == "fetched"
    assert (dest / "law.json").read_text(encoding="utf-8") == "불필요한 로컬 수정"


def test_sync_repo_fails_safely_instead_of_discarding_conflicting_local_changes(tmp_path):
    """reset --hard 대신 fast-forward-only 를 쓰므로, 로컬에 커밋되지 않은 변경이 원격 갱신과
    충돌하면 그 변경을 지우지 않고 에러로 실패해야 한다(사람이 직접 확인)."""
    origin, seed = _make_origin(tmp_path)
    dest = tmp_path / "law_data"
    git_sync.sync_repo(str(origin), dest, branch="main")

    # 로컬에 커밋 안 된 변경(드리프트)
    (dest / "law.json").write_text("로컬에서만 바뀐 내용", encoding="utf-8")

    # 원격에는 같은 파일을 건드리는 새 커밋이 push됨 — fast-forward 시 이 파일을 갱신해야 함
    (seed / "law.json").write_text('{"v": 2}', encoding="utf-8")
    _git("commit", "-am", "update", cwd=seed)
    _git("push", "origin", "main", cwd=seed)

    with pytest.raises(git_sync.GitSyncError):
        git_sync.sync_repo(str(origin), dest, branch="main")

    # 로컬 드리프트가 그대로 보존돼야 한다(삭제·덮어쓰기 되지 않음)
    assert (dest / "law.json").read_text(encoding="utf-8") == "로컬에서만 바뀐 내용"


def test_sync_repo_raises_clear_error_for_invalid_url(tmp_path):
    dest = tmp_path / "law_data"
    with pytest.raises(git_sync.GitSyncError):
        git_sync.sync_repo("/no/such/path/law_data.git", dest, branch="main", timeout=10)


def test_current_commit_returns_none_for_non_repo(tmp_path):
    not_a_repo = tmp_path / "empty"
    not_a_repo.mkdir()
    assert git_sync.current_commit(not_a_repo) is None


def test_safe_name_preserves_short_and_truncates_long():
    """정책 B: git 원본 이름을 리눅스 로컬용으로. 255바이트 이내(checkout 성공분)면 원본 그대로,
    초과면 확장자 보존 바이트절단 — materialize 규칙과 같은 함수라 색인이 그 파일을 찾는다."""
    from law_indexer.preprocess import _safe_name
    # 255바이트 이내 = 수집기 clean 과 동일(원본 보존)
    assert _safe_name("관세법.json") == "관세법.json"
    assert _safe_name("별표1_과태료의 부과기준.hwp") == "별표1_과태료의 부과기준.hwp"
    assert _safe_name('a/b:c*d.hwp') == "a_b_c_d.hwp"          # 금지문자만 치환
    assert _safe_name("a" * 200) == "a" * 200                 # ASCII 200자는 255바이트 이내라 보존
    # 255바이트 초과 → 확장자 보존 + 해시꼬리 + 255 이내, 멱등, 충돌 없음
    long = "별표1_" + "가" * 86 + ".hwp"
    assert len(long.encode("utf-8")) > 255
    s = _safe_name(long)
    assert len(s.encode("utf-8")) <= 255 and s.endswith(".hwp") and "~" in s
    assert _safe_name(long) == s
    assert _safe_name("가" * 86 + "A.hwp") != _safe_name("가" * 86 + "B.hwp")


def test_materialize_long_path_file_after_clone(tmp_path):
    """255바이트를 넘는 이름은 리눅스에서 checkout 이 실패한다 → sync 가 그 파일만 safe 이름으로
    로컬에 풀고(materialize), 원본(긴) 경로는 skip-worktree 로 표시한다. git 원본은 불변(정책 B).

    macOS 는 255'글자' 한계라 clone 자체는 성공하지만(리눅스 실패는 재현 불가), materialize 는
    OS 무관하게 safe 사본을 만들어 색인이 그 이름으로 파일을 찾게 한다 — 그 동작을 검증한다."""
    from law_indexer.preprocess import _safe_name
    origin = tmp_path / "origin.git"
    origin.mkdir()
    _git("init", "--bare", "-b", "main", cwd=origin)
    seed = tmp_path / "seed"
    seed.mkdir()
    _git("init", "-b", "main", cwd=seed)
    _git("config", "user.email", "test@example.com", cwd=seed)
    _git("config", "user.name", "Test", cwd=seed)
    longname = "별표1_" + "가" * 86 + ".hwp"                   # ~270바이트 > 255 (그러나 <255 글자)
    assert len(longname.encode("utf-8")) > 255
    sub = seed / "관세법" / "법률" / "별표"
    sub.mkdir(parents=True)
    (sub / longname).write_bytes("HWP본문바이트".encode("utf-8"))
    _git("add", "-A", cwd=seed)
    _git("commit", "-m", "long name appendix", cwd=seed)
    _git("remote", "add", "origin", str(origin), cwd=seed)
    _git("push", "origin", "main", cwd=seed)

    dest = tmp_path / "law_data"
    result = git_sync.sync_repo(str(origin), dest, branch="main")
    assert result["action"] == "cloned"
    assert result["materialized"] == 1

    safe = _safe_name(longname)
    assert safe != longname                                    # 실제로 잘렸다
    copy = dest / "관세법" / "법률" / "별표" / safe
    assert copy.exists() and copy.read_bytes() == "HWP본문바이트".encode("utf-8")
    # 원본(긴) 경로는 skip-worktree(-v 태그 'S') → 이후 fetch 의 머지 충돌·deleted 노이즈 방지
    rel = "관세법/법률/별표/" + longname
    v = subprocess.run(["git", "-C", str(dest), "ls-files", "-v", "--", rel],
                       capture_output=True, text=True)
    assert v.stdout.startswith("S ")
    # safe 사본은 .git/info/exclude 에 등록 → git add -A 로도 스테이징 안 됨(미러가 같은 레포에
    # add -A 해도 git 엔 원본만 남는다).
    excl = (dest / ".git" / "info" / "exclude").read_text(encoding="utf-8")
    safe_rel = "관세법/법률/별표/" + safe
    assert ("/" + safe_rel) in excl.splitlines()
    subprocess.run(["git", "-C", str(dest), "add", "-A"], check=True)
    staged = subprocess.run(["git", "-C", str(dest), "diff", "--cached", "--name-only"],
                            capture_output=True, text=True).stdout
    assert safe_rel not in staged.splitlines()
