"""C 미러(RepoMirror = 데이터 레포 repo 레이아웃 최신화) + package 삭제 플래그 검증.

Weaviate/임베딩 모델 없이 fake 로. 미러는 생산자가 실어 준 document.git_path 위치를 재현한다.
"""
import dataclasses
import json

from law_indexer.config import Settings
from law_indexer.package import RepoMirror, consume_package


class FakeStore:
    def delete_by_law_id(self, law_id, collection):
        return 1

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        return 0

    def upsert(self, objects, dimension, model_name, collection):
        objs = list(objects)
        return {"success": len(objs), "failed": 0, "failed_ids": []}


class FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


class FakeDocParser:
    def run(self, request_path, chunk_size, chunk_overlap, endpoint_path=None):
        return [{"text": "파일 텍스트", "i_chunk_on_doc": 0, "i_page": 1, "e_page": 1}]


def _payload(law_id):
    return {"law_id": law_id, "version_uid": f"{law_id}:1:20240101", "law_name": f"법{law_id}",
            "law_type": "법률",
            "body": {"articles": [{"article_no": "제1조", "content": "본문",
                                   "provision_id": f"law:{law_id}#JO0001"}]}}


def _write(tmp_path, *records):
    path = tmp_path / "pkg.jsonl"
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n", encoding="utf-8")
    return path


def _header(source="law", **extra):
    return dict(record_type="package_header", package_id="pkg-1", source=source, mode="delta", **extra)


# ── RepoMirror 단위 ──────────────────────────────────────────────────────
def test_repo_mirror_save_document(tmp_path):
    repo = tmp_path / "law_data"
    mirror = RepoMirror(repo)
    assert mirror.save_document("관세법/법률/관세법.json", {"law_id": "001556"}) is True
    written = repo / "관세법" / "법률" / "관세법.json"
    assert written.exists() and json.loads(written.read_text(encoding="utf-8"))["law_id"] == "001556"


def test_repo_mirror_rejects_bad_paths(tmp_path):
    mirror = RepoMirror(tmp_path / "law_data")
    assert mirror.save_document(None, {}) is False
    assert mirror.save_document("package:L1", {}) is False        # synthetic
    assert mirror.save_document("/etc/passwd.json", {}) is False  # 절대경로
    assert mirror.save_document("../../evil.json", {}) is False   # 상위 탈출


def test_repo_mirror_save_file_subfolders(tmp_path):
    """save_file 이 unit_type/파일명으로 수집기와 같은 서브폴더에 정확히 배치한다."""
    repo = tmp_path / "law_data"
    src = tmp_path / "src.bin"
    src.write_bytes(b"bytes")
    mirror = RepoMirror(repo)
    gp = "관세법/법률/관세법.json"
    cases = [
        ("별표1_부과기준.hwp", "APPENDIX", "별표"),
        ("서식3_신청서.pdf", "APPENDIX", "서식"),
        ("별지2_양식.hwp", "APPENDIX", "별지"),
        ("별첨A_제출서류.hwp", "APPENDIX", "별첨"),
        ("부속서1_안전기준.pdf", "APPENDIX", "부속서"),
        ("부도1_위치도.png", "APPENDIX", "부도"),
        ("관세법 전문.hwp", "FILE", "첨부파일"),
        ("제5조_1.gif", None, "본문이미지"),
    ]
    for fn, ut, sub in cases:
        assert mirror.save_file(gp, fn, ut, src) is True
        assert (repo / "관세법" / "법률" / sub / fn).read_bytes() == b"bytes", f"{fn} → {sub}"


# ── 미러 git commit/push (내부망 Gitea 대상) ──────────────────────────────
def _git_init(repo):
    import subprocess
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)


def test_repo_mirror_commit_uses_collector_message(tmp_path):
    """push 모드: save_document 후 commit 이 패키지의 commit_message(수집기와 동일)로 찍힌다."""
    import subprocess
    repo = tmp_path / "law_data"
    _git_init(repo)
    mirror = RepoMirror(repo, push=True, branch="main")
    mirror.save_document("관세법/법률/관세법.json", {"law_id": "L1"})
    assert mirror.commit("[일부개정] 관세법 시행 2024.01.01 (MST 123)") is True
    log = subprocess.run(["git", "-C", str(repo), "log", "--oneline", "-1"],
                         capture_output=True, text=True).stdout
    assert "관세법 시행 2024.01.01" in log


def test_repo_mirror_long_name_push_stages_original_via_plumbing(tmp_path):
    """push 모드 + 255바이트 초과 파일: 리눅스가 작업트리에 못 쓰므로 git index 에 **원본 경로**로
    blob 을 넣는다(정책 B). 디스크엔 안 남지만 커밋 트리엔 원본 이름이 있어 git 으로 찾아간다."""
    import subprocess
    repo = tmp_path / "law_data"
    _git_init(repo)
    src = tmp_path / "src.hwp"
    src.write_bytes(b"appendix-bytes")
    mirror = RepoMirror(repo, push=True, branch="main")
    longname = "별표1_" + "가" * 86 + ".hwp"                   # >255바이트
    assert len(longname.encode("utf-8")) > 255
    assert mirror.save_file("관세법/법률/관세법.json", longname, "APPENDIX", src) is True
    rel = "관세법/법률/별표/" + longname
    ls = subprocess.run(["git", "-C", str(repo), "ls-files", "-z", "--", rel],
                        capture_output=True, text=True).stdout
    assert ls.strip("\0") == rel                              # index 에 원본 경로로 올라감
    assert not (repo / rel).exists()                          # 리눅스가 못 쓰는 이름 → 디스크 미기록
    blob = subprocess.run(["git", "-C", str(repo), "cat-file", "blob", f":{rel}"],
                          capture_output=True).stdout
    assert blob == b"appendix-bytes"                          # blob 내용은 원본 그대로
    # 커밋(add -A 가 skip-worktree 원본을 지우지 않는다) → 트리에 원본 경로 그대로
    assert mirror.commit("[일부개정] 관세법") is True
    tree = subprocess.run(["git", "-C", str(repo), "ls-tree", "-r", "--name-only", "-z", "HEAD"],
                          capture_output=True, text=True).stdout
    assert rel in [p for p in tree.split("\0") if p]


def test_repo_mirror_long_name_folder_only_writes_safe_copy(tmp_path):
    """folder-only(push off) + 255바이트 초과: git 조작 없이 로컬 브라우징용 safe 사본만 쓴다
    (색인은 어차피 package src_path 로 하므로 로컬 이름은 무관)."""
    from law_indexer.preprocess import _safe_name
    repo = tmp_path / "law_data"
    _git_init(repo)
    src = tmp_path / "src.hwp"
    src.write_bytes(b"bytes2")
    mirror = RepoMirror(repo, push=False)
    longname = "별표1_" + "가" * 86 + ".hwp"
    assert mirror.save_file("관세법/법률/관세법.json", longname, "APPENDIX", src) is True
    safe = _safe_name(longname)
    assert safe != longname
    assert (repo / "관세법" / "법률" / "별표" / safe).read_bytes() == b"bytes2"


def test_repo_mirror_no_commit_when_push_off(tmp_path):
    """push 꺼짐(폴더만 가짐): 파일은 쓰지만 커밋은 안 한다."""
    import subprocess
    repo = tmp_path / "law_data"
    _git_init(repo)
    mirror = RepoMirror(repo, push=False)
    assert mirror.save_document("관세법/법률/관세법.json", {"law_id": "L1"}) is True
    assert mirror.commit("msg") is False                     # push_enabled 아니면 no-op
    log = subprocess.run(["git", "-C", str(repo), "log", "--oneline"], capture_output=True, text=True)
    assert log.returncode != 0 or log.stdout.strip() == ""   # 커밋 0


# ── consume + 미러 ───────────────────────────────────────────────────────
def test_consume_mirrors_document_to_repo(tmp_path):
    repo = tmp_path / "law_data"
    settings = dataclasses.replace(Settings.from_env(), package_mirror_repo=True, law_repo_path=repo)
    pkg = _write(tmp_path, _header(),
                 {"record_type": "document", "op": "upsert", "law_id": "001556",
                  "git_path": "관세법/법률/관세법.json", "payload": _payload("001556")})
    totals = consume_package(FakeStore(), FakeEmbedder(), settings, pkg)
    assert totals["mirrored_docs"] == 1
    assert (repo / "관세법" / "법률" / "관세법.json").exists()


def test_consume_mirrors_file_to_repo(tmp_path):
    repo = tmp_path / "law_data"
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "f1.hwp").write_bytes(b"hwp-bytes")
    settings = dataclasses.replace(Settings.from_env(), package_mirror_repo=True, law_repo_path=repo,
                                   package_preprocess_files=True, package_file_inbox_dir=inbox)
    pkg = _write(tmp_path, _header(),
                 {"record_type": "document", "op": "upsert", "law_id": "001556",
                  "git_path": "관세법/법률/관세법.json", "payload": _payload("001556")},
                 {"record_type": "file", "law_id": "001556", "file_id": "f1",
                  "file_name": "별표1.hwp", "transfer_name": "f1.hwp",
                  "provision_id": "law:관세법#BYL0001", "unit_type": "APPENDIX"})
    totals = consume_package(FakeStore(), FakeEmbedder(), settings, pkg, doc_parser=FakeDocParser())
    assert totals["mirrored_files"] == 1
    # APPENDIX 별표1.hwp → 초기 gitexport 와 동일하게 '별표/' 서브폴더(첨부미러 아님)
    assert (repo / "관세법" / "법률" / "별표" / "별표1.hwp").read_bytes() == b"hwp-bytes"


def test_mirror_off_by_default(tmp_path):
    repo = tmp_path / "law_data"
    settings = dataclasses.replace(Settings.from_env(), law_repo_path=repo)  # mirror off
    pkg = _write(tmp_path, _header(),
                 {"record_type": "document", "op": "upsert", "law_id": "001556",
                  "git_path": "관세법/법률/관세법.json", "payload": _payload("001556")})
    totals = consume_package(FakeStore(), FakeEmbedder(), settings, pkg)
    assert totals["mirrored_docs"] == 0 and not repo.exists()


# ── package 삭제 플래그 ───────────────────────────────────────────────────
def test_package_deleted_on_clean_consume(tmp_path):
    settings = dataclasses.replace(Settings.from_env(), package_delete_consumed_package=True)
    pkg = _write(tmp_path, _header(),
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _payload("L1")},
                 {"record_type": "package_footer", "record_count": 1})
    totals = consume_package(FakeStore(), FakeEmbedder(), settings, pkg)
    assert totals.get("package_deleted") is True and not pkg.exists()


def test_package_kept_when_errors(tmp_path):
    settings = dataclasses.replace(Settings.from_env(), package_delete_consumed_package=True)
    pkg = _write(tmp_path, _header(),
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _payload("L1")},
                 {"record_type": "package_footer", "record_count": 999})   # 불일치 → errors
    totals = consume_package(FakeStore(), FakeEmbedder(), settings, pkg)
    assert totals.get("package_deleted") is not True and pkg.exists()


def test_package_kept_by_default(tmp_path):
    pkg = _write(tmp_path, _header(),
                 {"record_type": "document", "op": "upsert", "law_id": "L1", "payload": _payload("L1")})
    consume_package(FakeStore(), FakeEmbedder(), Settings.from_env(), pkg)
    assert pkg.exists()


def test_mirror_routes_schlpub_documents_to_their_own_repo(tmp_path, monkeypatch):
    """패키지 하나에 admrul 과 학칙공단 문서가 섞여 온다.

    레포를 하나로 고정하면 학칙공단 문서가 ADMRUL 레포에 잘못 쓰인다. doc_target 으로 갈라야 한다.
    """
    import subprocess
    from law_indexer.config import Settings
    from law_indexer.package import RepoMirrorSet

    admrul_root, schlpub_root = tmp_path / "ADMRUL", tmp_path / "SCHLPUBRUL"
    for root in (admrul_root, schlpub_root):
        root.mkdir()
        subprocess.run(["git", "init", "-q", str(root)], check=True)

    for k in ("LAW_REPO_PATH", "ADMRUL_REPO_PATH", "SCHLPUB_REPO_PATH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("ADMRUL_REPO_PATH", str(admrul_root))
    monkeypatch.setenv("SCHLPUB_REPO_PATH", str(schlpub_root))
    settings = Settings.from_env()

    mirrors = RepoMirrorSet(settings, "admrul", push=False)
    assert mirrors.save_document("고시A/고시A.json",
                                 {"law_name": "고시A", "doc_target": "admrul"})
    assert mirrors.save_document("학칙B/학칙B.json",
                                 {"law_name": "학칙B", "doc_target": "school"})

    assert (admrul_root / "고시A" / "고시A.json").exists()
    assert (schlpub_root / "학칙B" / "학칙B.json").exists()
    assert not (admrul_root / "학칙B").exists()       # 섞이지 않는다
    assert not (schlpub_root / "고시A").exists()


def test_repo_for_falls_back_to_admrul_when_schlpub_unset(monkeypatch, tmp_path):
    """SCHLPUB 경로를 비우면 옛 2레포 구성 그대로 동작한다."""
    from law_indexer.config import Settings

    for k in ("LAW_REPO_PATH", "ADMRUL_REPO_PATH", "SCHLPUB_REPO_PATH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("ADMRUL_REPO_PATH", str(tmp_path / "ADMRUL"))
    monkeypatch.setenv("SCHLPUB_REPO_PATH", "")
    settings = Settings.from_env()

    assert settings.repo_for("school") == settings.admrul_repo_path
    assert settings.repo_for("admrul") == settings.admrul_repo_path
    assert settings.repo_for("eflaw") == settings.law_repo_path
