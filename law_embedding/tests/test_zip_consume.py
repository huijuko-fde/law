"""ZIP 수신 2단계(zip_consume, LAW-41) 검증 — 수신 폴더 처리 순서·처리 이력·manifest 대조·재실행 정합.

외부 접속 없음: Weaviate 는 pipeline.WeaviateStore 를 fake 로 바꾸고(test_consume_folder.py 와 같은 방식),
임베딩은 fake, 전처리기는 package.DocParserClient 를 fake 로 바꾼다. 적재는 실제 consume_folder 를 탄다.
"""
import collections
import dataclasses
import fcntl
import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from law_indexer import package, pipeline, zip_consume
from law_indexer.cli import _failure_code
from law_indexer.config import Settings
from law_indexer.zip_consume import consume_zip_folder


# ── fake ────────────────────────────────────────────────────────────────

class _Vdb:
    """fake Weaviate — consume_folder 가 여는 모든 연결이 이 기록을 공유한다.
    fail_upserts 만큼 upsert 가 일시장애(연결 끊김)를 낸다."""

    def __init__(self):
        self.upserted, self.deleted = [], []
        self.upsert_calls = 0
        self.fail_upserts = 0

    def store_class(self):
        vdb = self

        class _Store:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def delete_by_law_id(self, law_id, collection):
                vdb.deleted.append(law_id)
                return 1

            def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
                return 0

            def current_versions(self, law_id, collection):
                out, seen = [], set()
                for obj in vdb.upserted:
                    if obj.law_id == law_id and obj.version_uid not in seen:
                        seen.add(obj.version_uid)
                        out.append({"version_uid": obj.version_uid, "mst": obj.mst})
                return out

            def upsert(self, objects, dimension, model_name, collection):
                if vdb.fail_upserts:
                    vdb.fail_upserts -= 1
                    raise RuntimeError("weaviate 연결 끊김(일시장애)")
                vdb.upsert_calls += 1
                vdb.upserted.extend(objects)
                return {"success": len(objects), "failed": 0, "failed_ids": []}

        return _Store

    def versions(self, law_id):
        """그 law_id 가 적재된 version_uid 를 적재 순서대로(중복 제거)."""
        out = []
        for obj in self.upserted:
            if obj.law_id == law_id and obj.version_uid not in out:
                out.append(obj.version_uid)
        return out


class _Embedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


class _DocParser:
    def run(self, request_path, chunk_size, chunk_overlap, endpoint_path=None):
        return [{"text": "전처리로 뽑은 파일 텍스트", "i_chunk_on_doc": 0, "i_page": 1, "e_page": 1}]


class _Crash(Exception):
    """프로세스가 그 지점에서 죽은 것을 흉내낸다."""


@pytest.fixture
def vdb(monkeypatch):
    fake = _Vdb()
    monkeypatch.setattr(pipeline, "WeaviateStore", fake.store_class())
    monkeypatch.setattr(package, "DocParserClient", lambda *args, **kwargs: _DocParser())
    return fake


@pytest.fixture
def settings(tmp_path):
    """미러·원문보관·삭제·공유폴더·nack 설정을 꺼서 tmp_path 밖을 건드리지 않게 한다."""
    return dataclasses.replace(
        Settings.from_env(), package_preprocess_files=True, package_file_inbox_dir=None,
        package_delete_consumed_files=False, package_store_original=False, package_mirror_repo=False,
        package_mirror_push=False, package_delete_consumed_package=False, package_nack_dir=None,
        doc_parser_shared_dir=None, doc_parser_shared_host_dir=None, doc_parser_shared_container_dir=None,
        admrul_repo_path=tmp_path / "no_such_repo")


@pytest.fixture
def inbox(tmp_path):
    path = tmp_path / "inbox"
    path.mkdir()
    return path


# ── ZIP·manifest 만들기 ─────────────────────────────────────────────────

def _law(law_id, name, *, version="1", appendices=None):
    return {"law_id": law_id, "mst": version, "version_uid": f"{law_id}:{version}:20260101",
            "law_name": name, "law_type": "법률", "is_current": True,
            "body": {"articles": [{"article_no": "제1조", "article_title": "목적",
                                   "content": f"{name} 목적 조문 v{version}",
                                   "provision_id": f"law:{name}#JO0001"}]},
            "appendices": appendices or []}


def _appendix(no, title, filename, *, owner):
    return {"no": f"{no:04d}", "branch": "00", "kind": "별표", "title": title, "is_file_only": True,
            "content": "", "filename": filename, "file_url": f"https://example.invalid/byl/{no}",
            "files": [{"type": "hwp", "url": f"https://example.invalid/byl/{no}", "name": ""}],
            "provision_id": f"law:{owner}#BYL{no:04d}"}


def _write_zip(path, members):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for arcname, content in members.items():
            data = content if isinstance(content, bytes) else json.dumps(content, ensure_ascii=False).encode("utf-8")
            zf.writestr(arcname, data)
    return path


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _manifest(zip_path, members, **override):
    """수집기 zipbatch.emit_batch 가 ZIP 옆에 쓰는 manifest(`<stem>.manifest.json`)와 같은 필드."""
    docs = [content for name, content in members.items() if name.endswith(".json")]
    record = {"filename": zip_path.name, "sha256": _sha(zip_path), "size_bytes": zip_path.stat().st_size,
              "documents": len(docs), "repealed": sum(1 for d in docs if d.get("repealed") is True),
              "events": ["e1"], "changes": []}
    record.update(override)
    zip_path.with_name(zip_path.stem + ".manifest.json").write_text(
        json.dumps(record, ensure_ascii=False), encoding="utf-8")
    return record


def _state(inbox, sha256):
    return json.loads((inbox / "state" / f"{sha256}.json").read_text(encoding="utf-8"))


def _run(settings, inbox):
    return consume_zip_folder(_Embedder(), settings, inbox)


def _records(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


# ── 정상 처리 · 순서 · 중복 ──────────────────────────────────────────────

def test_normal_zip_loaded_recorded_and_archived(inbox, vdb, settings):
    """정상 ZIP — 기존 consume_folder 로 적재(첨부는 file_transfer 로 packages/files 에 풀려 전처리), 이력
    done, ZIP 은 done/ 으로. 임시 확장자·점 파일·기타 파일은 건드리지 않는다. manifest 가 없으면 경고만."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {
        "law/형법/형법.json": _law("001692", "형법", appendices=[
            _appendix(1, "벌금표", "별표1_벌금표.hwp", owner="형법")]),
        "law/형법/별표/별표1_벌금표.hwp": b"HWP-FILE-ONLY",
    })
    sha = _sha(zip_path)
    for name in (".0002_batch.zip.tmp", ".hidden.zip", "notes.txt"):
        (inbox / name).write_bytes(b"not for me")

    summary = _run(settings, inbox)

    assert (summary["total"], summary["done"], summary["failed"]) == (1, 1, 0)
    assert _failure_code(summary) == 0
    assert {o.law_id for o in vdb.upserted} == {"001692"}
    assert any(o.source_type == "FILE" for o in vdb.upserted)        # 별표 파일 → 전처리 → FILE 청크
    assert (inbox / "done" / "0001_batch.zip").exists() and not zip_path.exists()
    for name in (".0002_batch.zip.tmp", ".hidden.zip", "notes.txt"):
        assert (inbox / name).exists()

    record = _state(inbox, sha)
    assert record["status"] == "done" and record["archive"] == "done/0001_batch.zip"
    assert record["summary"]["documents"] == 1 and record["summary"]["files"] == 1
    assert [p["status"] for p in record["packages"]] == ["ok"]
    assert [i["code"] for i in record["issues"]] == ["manifest_missing"]

    [processed] = (inbox / "packages" / "processed").glob("*.jsonl")
    [file_record] = [r for r in _records(processed) if r["record_type"] == "file"]
    assert "content_b64" not in file_record                          # file_transfer — 본문 대신 이름만
    assert (inbox / "packages" / "files" / file_record["transfer_name"]).read_bytes() == b"HWP-FILE-ONLY"
    assert not list((inbox / "packages").glob("*.jsonl"))


def test_zips_processed_in_filename_order_later_wins(inbox, vdb, settings):
    """같은 문서가 여러 ZIP 에 있으면 파일명 순서로 처리해 나중 ZIP 이 최종이다(만든 순서와 무관)."""
    _write_zip(inbox / "0002_batch.zip", {"law/형법/형법.json": _law("001692", "형법", version="2")})
    _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법", version="1")})

    summary = _run(settings, inbox)

    assert [z["zip"] for z in summary["zips"]] == ["0001_batch.zip", "0002_batch.zip"]
    assert summary["done"] == 2
    assert vdb.versions("001692") == ["001692:1:20260101", "001692:2:20260101"]


def test_same_sha256_not_reprocessed_even_renamed(inbox, vdb, settings):
    """이미 처리한 SHA-256 은 이름이 같든 다르든 다시 적재하지 않고 duplicate/ 로 옮긴다."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})
    data, sha = zip_path.read_bytes(), _sha(zip_path)
    _run(settings, inbox)
    calls = vdb.upsert_calls

    (inbox / "0001_batch.zip").write_bytes(data)                     # 같은 이름으로 다시 옴
    (inbox / "0009_resent.zip").write_bytes(data)                    # 이름만 바꿔 다시 옴
    summary = _run(settings, inbox)

    assert (summary["duplicate"], summary["done"]) == (2, 0)
    assert vdb.upsert_calls == calls
    assert (inbox / "duplicate" / "0001_batch.zip").exists() and (inbox / "duplicate" / "0009_resent.zip").exists()
    record = _state(inbox, sha)
    assert record["status"] == "done" and record["archive"] == "done/0001_batch.zip"
    assert [d["zip_name"] for d in record["duplicates"]] == ["0001_batch.zip", "0009_resent.zip"]


def test_concurrent_run_skips(inbox, vdb, settings):
    """다른 실행이 잠금을 잡고 있으면 기다리지 않고 아무것도 하지 않는다(스케줄 겹침)."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})
    (inbox / "state").mkdir()
    with open(inbox / "state" / ".lock", "a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        summary = _run(settings, inbox)
    assert "skipped" in summary and summary["total"] == 0
    assert zip_path.exists() and vdb.upsert_calls == 0


# ── 부분완료 · 실패 격리 ─────────────────────────────────────────────────

def test_partial_rejected_document_excluded_rest_loaded(inbox, vdb, settings):
    """문서 1건 오류로 ZIP 전체를 막지 않는다 — 거부 문서만 빼고 적재, 이력 partial, exit 2."""
    no_id = _law("x", "무번호법")
    del no_id["law_id"]
    zip_path = _write_zip(inbox / "0001_batch.zip", {
        "law/형법/형법.json": _law("001692", "형법"),
        "law/무번호법/무번호법.json": no_id,
    })
    sha = _sha(zip_path)

    summary = _run(settings, inbox)

    assert summary["partial"] == 1 and summary["zips"][0]["rejected"] == 1
    assert _failure_code(summary) == 2
    assert {o.law_id for o in vdb.upserted} == {"001692"}
    assert (inbox / "done" / "0001_batch.zip").exists()
    record = _state(inbox, sha)
    assert record["status"] == "partial" and record["summary"]["rejected"] == 1
    assert any(i["code"] == "law_id_missing" and i["path"] == "law/무번호법/무번호법.json"
               for i in record["issues"])


def test_corrupt_zip_quarantined_next_zip_continues(inbox, vdb, settings):
    """깨진 ZIP 은 failed/ 로 격리하고 다음 ZIP 은 그대로 처리한다."""
    (inbox / "0001_broken.zip").write_bytes(b"this is not a zip")
    _write_zip(inbox / "0002_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})

    summary = _run(settings, inbox)

    assert [(z["zip"], z["status"], z["reason"]) for z in summary["zips"]] == [
        ("0001_broken.zip", "failed", "zip_corrupt"), ("0002_batch.zip", "done", None)]
    assert _failure_code(summary) == 2
    assert (inbox / "failed" / "0001_broken.zip").exists()
    record = _state(inbox, hashlib.sha256(b"this is not a zip").hexdigest())
    assert record["status"] == "failed" and record["issues"][-1]["code"] == "zip_corrupt"
    assert {o.law_id for o in vdb.upserted} == {"001692"}


def test_multi_version_zip_rejected_nothing_loaded(inbox, vdb, settings):
    """같은 law_id 의 두 버전만 담긴 ZIP — 그 law_id 를 통째로 거부해 실을 문서가 없으므로 실패(all_rejected)."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {
        "law/형법/형법.json": _law("001692", "형법", version="1"),
        "law/형법2/형법.json": _law("001692", "형법", version="2"),
    })
    sha = _sha(zip_path)

    summary = _run(settings, inbox)

    assert [(z["status"], z["reason"]) for z in summary["zips"]] == [("failed", "all_rejected")]
    assert vdb.upserted == [] and vdb.deleted == []
    record = _state(inbox, sha)
    assert {i["code"] for i in record["issues"] if i["level"] == "error"} == {"multi_version_unsupported"}
    assert record["packages"] == [] and (inbox / "failed" / "0001_batch.zip").exists()


# ── manifest ────────────────────────────────────────────────────────────

def test_manifest_match_and_count_mismatch(inbox, vdb, settings):
    """manifest 가 맞으면(SHA-256·문서 수·폐지 수) 그대로 처리, 문서 수가 다르면 아무것도 적재하지 않고
    manifest 와 함께 failed/ 로 격리한다."""
    members_ok = {"law/형법/형법.json": _law("001692", "형법"),
                  "law/폐지법/폐지법.json": {"law_id": "001700", "law_name": "폐지법", "repealed": True}}
    ok = _write_zip(inbox / "0001_ok.zip", members_ok)
    _manifest(ok, members_ok)
    members_bad = {"law/민법/민법.json": _law("001706", "민법")}
    bad = _write_zip(inbox / "0002_count.zip", members_bad)
    _manifest(bad, members_bad, documents=2)
    sha_ok = _sha(ok)

    summary = _run(settings, inbox)

    assert [(z["zip"], z["status"], z["reason"]) for z in summary["zips"]] == [
        ("0001_ok.zip", "done", None), ("0002_count.zip", "failed", "manifest_mismatch")]
    assert not [i for i in _state(inbox, sha_ok)["issues"] if i["code"].startswith("manifest")]
    assert vdb.deleted == ["001700"] and {o.law_id for o in vdb.upserted} == {"001692"}
    assert (inbox / "done" / "0001_ok.manifest.json").exists()
    assert (inbox / "failed" / "0002_count.zip").exists() and (inbox / "failed" / "0002_count.manifest.json").exists()


def test_manifest_sha_mismatch_failed_then_reprocessed(inbox, vdb, settings):
    """SHA-256 이 다르면 실패 격리. failed/ 의 ZIP 을 (올바른 manifest 와) 수신 폴더에 다시 넣으면 같은
    SHA-256 이라도 처음부터 다시 처리한다 — 실패 이력은 재처리를 막지 않고 history 에 남는다."""
    members = {"law/형법/형법.json": _law("001692", "형법")}
    zip_path = _write_zip(inbox / "0001_batch.zip", members)
    sha = _sha(zip_path)
    _manifest(zip_path, members, sha256="0" * 64)

    summary = _run(settings, inbox)
    assert summary["failed"] == 1 and summary["zips"][0]["reason"] == "manifest_mismatch"
    assert vdb.upserted == []

    (inbox / "failed" / "0001_batch.zip").rename(zip_path)           # 재처리: failed/ → 수신 폴더
    (inbox / "failed" / "0001_batch.manifest.json").unlink()
    _manifest(zip_path, members)                                     # 상대가 다시 보낸 올바른 manifest
    summary = _run(settings, inbox)

    assert summary["done"] == 1 and {o.law_id for o in vdb.upserted} == {"001692"}
    record = _state(inbox, sha)
    assert record["status"] == "done" and record["attempt"] == 2
    assert (record["history"][0]["status"], record["history"][0]["reason"]) == ("failed", "manifest_mismatch")


def test_manifest_attachment_mapping_used(inbox, vdb, settings):
    """manifest 의 attachment_files 로 이름이 바뀐(`{stem}__{sha12}`) 첨부를 원래 별표에 붙인다 — 고아
    오류(부분완료)가 나지 않고 두 별표가 각자 자기 파일로 적재된다."""
    first, second = b"HWP-FIRST", b"HWP-SECOND"
    renamed = f"별표1_수수료__{hashlib.sha256(second).hexdigest()[:12]}.hwp"
    appendix_a = _appendix(1, "수수료", "별표1_수수료.hwp", owner="형법")
    appendix_b = dict(_appendix(1, "수수료", "별표1_수수료.hwp", owner="형법"), branch="02",
                      file_url="https://example.invalid/byl/1-2",
                      files=[{"type": "hwp", "url": "https://example.invalid/byl/1-2", "name": ""}],
                      provision_id="law:형법#BYL0001-2")
    members = {"law/형법/형법.json": _law("001692", "형법", appendices=[appendix_a, appendix_b]),
               "law/형법/별표/별표1_수수료.hwp": first,
               f"law/형법/별표/{renamed}": second}
    zip_path = _write_zip(inbox / "0001_batch.zip", members)
    _manifest(zip_path, members, changes=[{
        "doc_uid": "law:eflaw:001692", "name": "형법", "operation": "upsert", "attachment_files": [
            {"url": appendix_a["file_url"], "path": "law/형법/별표/별표1_수수료.hwp",
             "sha256": hashlib.sha256(first).hexdigest()},
            {"url": appendix_b["file_url"], "path": f"law/형법/별표/{renamed}",
             "sha256": hashlib.sha256(second).hexdigest()}]}])

    summary = _run(settings, inbox)

    assert summary["done"] == 1
    assert {o.provision_id for o in vdb.upserted if o.source_type == "FILE"} == {
        "law:형법#BYL0001", "law:형법#BYL0001-2"}


# ── 적재 대기 · 적재 실패 ────────────────────────────────────────────────

def test_load_retry_waits_and_defers_later_zips(inbox, vdb, settings):
    """앞 ZIP 의 package 가 일시장애로 재시도 대기면 그 ZIP 은 수신 폴더에 두고(loading) 뒤 ZIP 은 미룬다 —
    뒤 ZIP 이 먼저 들어가 옛 내용이 최종이 되는 일을 막는다. 다음 실행이 순서대로 이어 간다."""
    _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법", version="1")})
    _write_zip(inbox / "0002_batch.zip", {"law/형법/형법.json": _law("001692", "형법", version="2")})
    vdb.fail_upserts = 1

    summary = _run(settings, inbox)
    assert (summary["waiting"], summary["deferred"], summary["done"]) == (1, 1, 0)
    assert summary["zips"][0]["status"] == "loading" and _failure_code(summary) == 0
    assert (inbox / "0001_batch.zip").exists() and (inbox / "0002_batch.zip").exists()
    assert vdb.upserted == []

    summary = _run(settings, inbox)
    assert summary["done"] == 2
    assert vdb.versions("001692") == ["001692:1:20260101", "001692:2:20260101"]


def test_load_quarantine_marks_zip_failed(inbox, vdb, settings):
    """재시도를 다 써서 consume-folder 가 package 를 격리하면 ZIP 도 실패(load_failed)로 failed/ 에 둔다."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})
    sha = _sha(zip_path)
    vdb.fail_upserts = 99

    for _ in range(pipeline._MAX_CONSUME_ATTEMPTS):
        summary = _run(settings, inbox)

    assert summary["failed"] == 1 and summary["zips"][0]["reason"] == "load_failed"
    assert (inbox / "failed" / "0001_batch.zip").exists()
    assert [p["status"] for p in _state(inbox, sha)["packages"]] == ["failed"]
    assert len(list((inbox / "packages" / "failed").glob("*.jsonl"))) == 1


# ── 중간에 죽은 뒤 재실행 ────────────────────────────────────────────────

def test_crash_during_convert_restarts_cleanly(inbox, vdb, settings, monkeypatch):
    """변환 중 죽음(converting 기록) — package 는 아직 작업 폴더라 소비자에게 안 보이고, 다음 실행은
    작업 폴더를 버리고 처음부터 해 package 가 두 번 적재되지 않는다."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})
    sha = _sha(zip_path)
    real = zip_consume.write_packages

    def crash(result, out_dir, **kwargs):
        real(result, out_dir, **kwargs)
        raise _Crash("게시 전에 종료")

    monkeypatch.setattr(zip_consume, "write_packages", crash)
    with pytest.raises(_Crash):
        _run(settings, inbox)
    assert _state(inbox, sha)["status"] == "converting"
    assert not list((inbox / "packages").glob("*.jsonl"))
    assert list((inbox / "packages" / ".zip-work" / sha).glob("*.jsonl"))

    monkeypatch.setattr(zip_consume, "write_packages", real)
    summary = _run(settings, inbox)

    assert summary["done"] == 1 and vdb.upsert_calls == 1
    assert len(list((inbox / "packages" / "processed").glob("*.jsonl"))) == 1
    assert not (inbox / "packages" / ".zip-work" / sha).exists()
    record = _state(inbox, sha)
    assert record["attempt"] == 2 and record["history"][0]["status"] == "converting"


def test_crash_during_load_resumes_without_reconverting(inbox, vdb, settings, monkeypatch):
    """적재 중 죽음(loading 기록) — 다음 실행은 다시 변환하지 않고 게시된 package 를 소비해 마친다."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})
    sha = _sha(zip_path)
    real = zip_consume.consume_folder

    def crash(*args, **kwargs):
        raise _Crash("적재 중 종료")

    monkeypatch.setattr(zip_consume, "consume_folder", crash)
    with pytest.raises(_Crash):
        _run(settings, inbox)
    assert _state(inbox, sha)["status"] == "loading" and zip_path.exists()
    assert len(list((inbox / "packages").glob("*.jsonl"))) == 1

    monkeypatch.setattr(zip_consume, "consume_folder", real)
    summary = _run(settings, inbox)

    assert summary["done"] == 1 and vdb.upsert_calls == 1
    assert len(list((inbox / "packages" / "processed").glob("*.jsonl"))) == 1
    assert _state(inbox, sha)["attempt"] == 1


def test_crash_before_archive_finishes_move_only(inbox, vdb, settings, monkeypatch):
    """이력을 확정하고 ZIP 을 옮기기 전에 죽음 — 다음 실행은 중복으로 보지 않고 옮기기만 마친다."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})
    sha = _sha(zip_path)
    real = zip_consume._move

    def crash(*args, **kwargs):
        raise _Crash("옮기기 전에 종료")

    monkeypatch.setattr(zip_consume, "_move", crash)
    with pytest.raises(_Crash):
        _run(settings, inbox)
    assert _state(inbox, sha)["status"] == "done" and zip_path.exists()

    monkeypatch.setattr(zip_consume, "_move", real)
    summary = _run(settings, inbox)

    assert (summary["done"], summary["duplicate"]) == (1, 0) and vdb.upsert_calls == 1
    assert (inbox / "done" / "0001_batch.zip").exists() and not (inbox / "duplicate").exists()


# ── 재전송·갱신 규칙 (LAW-28) ─────────────────────────────────────────────
#
# 재전송은 갱신이다. 단 옛 버전이 나중에 오면 최신판을 되돌리지 않고 그 법령만 건너뛴다(실패 아님).
# 품질 가드(LAW-48)는 증분 경로에도 걸려야 한다 — 적재는 되돌리지 않되 결과·종료코드에 드러난다.

def test_older_version_resend_does_not_overwrite_newer(inbox, vdb, settings):
    _write_zip(inbox / "0002_batch.zip", {"law/형법/형법.json": _law("001692", "형법", version="2")})
    summary = _run(settings, inbox)
    assert summary["done"] == 1 and vdb.versions("001692") == ["001692:2:20260101"]

    _write_zip(inbox / "0003_resend_old.zip", {"law/형법/형법.json": _law("001692", "형법", version="1")})
    summary = _run(settings, inbox)
    assert summary["done"] == 1 and summary["version_skipped"] == 1 and _failure_code(summary) == 0
    assert summary["zips"][0]["version_skipped"] == 1
    assert vdb.versions("001692") == ["001692:2:20260101"]            # v1 은 들어가지 않았다
    sha = _sha(inbox / "done" / "0003_resend_old.zip")
    assert _state(inbox, sha)["summary"]["version_skipped_laws"][0]["incoming"] == "001692:1:20260101"


def test_quality_guard_surfaces_in_summary_and_exit_code(inbox, vdb, settings, monkeypatch):
    """적재 중 품질 가드가 걸리면 ZIP 은 done 이지만 요약·이력·종료코드가 그것을 말한다."""
    zip_path = _write_zip(inbox / "0001_batch.zip", {"law/형법/형법.json": _law("001692", "형법")})
    sha = _sha(zip_path)
    real = zip_consume.consume_folder

    def tainted(*args, **kwargs):
        from law_indexer.preprocess import note_quality
        note_quality("chunks_seen", 100)
        note_quality("chunks_dropped_empty", 1)
        note_quality("dropped_real_chars", 40)        # 버린 청크에 내용이 있었다 — 필터 오작동 신호
        return real(*args, **kwargs)

    monkeypatch.setattr(zip_consume, "consume_folder", tainted)
    summary = _run(settings, inbox)
    assert summary["done"] == 1 and vdb.upsert_calls == 1              # 적재분은 되돌리지 않는다
    assert summary["quality_guard"][0]["zip"] == "0001_batch.zip"
    assert summary["quality_guard"][0]["reason"] == "dropped_chunks_had_content"
    assert _failure_code(summary) == 2
    record = _state(inbox, sha)
    assert record["summary"]["quality_guard"]["reason"] == "dropped_chunks_had_content"
    assert any(i["code"] == "quality_guard" for i in record["issues"])


def test_runner_end_to_end_with_real_delivery(tmp_path, vdb, settings):
    """투라인 실제 배달분으로 수신 폴더 → 변환 → consume-zip 전 구간. 재전송은 duplicate 로 끝난다(변환 결정성)."""
    import shutil
    from law_indexer import auto_ingest as A
    from law_indexer.inbox_convert import convert_inbox

    fix = Path(__file__).parent / "fixtures" / "inbox"
    paths = A.IngestPaths(tmp_path / "root").ensure()

    def deliver():
        for suffix in (".zip", ".manifest.json"):
            shutil.copy(fix / f"batch-20261021-001{suffix}", paths.inbox / f"batch-20261021-001{suffix}")

    def consume(folder):
        return consume_zip_folder(_Embedder(), settings, folder)

    deliver()
    record = A.run_once(paths, convert_inbox, consume, trigger="test")
    assert record["verdict"] == "ok", record.get("reasons") or record.get("traceback")
    assert record["convert"][0]["status"] == "converted" and record["consume"]["done"] == 1
    assert (paths.done / "batch-20261021-001.zip").exists()                      # 원본 쌍 보관
    assert (paths.converted / "done" / "batch-20261021-001.zip").exists()        # 변환본 보관
    assert vdb.versions("001282") == ["001282:290605:20261002"]
    kinds = collections.Counter(o.unit_type for o in vdb.upserted)
    assert kinds["ARTICLE"] == 4 and kinds["APPENDIX"] == 2                      # 본문 있는 별표 → APPENDIX 청크

    deliver()                                                                    # 같은 배달 재전송
    record = A.run_once(paths, convert_inbox, consume, trigger="test")
    assert record["verdict"] == "ok" and record["consume"]["duplicate"] == 1 and record["consume"]["done"] == 0
