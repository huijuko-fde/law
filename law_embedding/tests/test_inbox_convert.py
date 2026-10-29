"""투라인 배달 ZIP → 적재기 정규화 ZIP 변환(law_indexer.inbox_convert) 검증.

fixture 는 2026-10-21 투라인이 실제로 보낸 배달분 2건(법령 1·행정규칙 1)이다 — 합성 데이터로는
law.go.kr 원본 응답의 모양(중첩 한글 키·CP949 파일명·여러 형식 재수출)을 재현하기 어렵다.
"""
import hashlib
import json
import os
import shutil
import zipfile
from pathlib import Path

from law_indexer import inbox_convert as C
from law_indexer.zip_intake import read_zip

FIX = Path(__file__).parent / "fixtures" / "inbox"


def _convert(name, tmp_path):
    mf = json.loads((FIX / f"{name}.manifest.json").read_text(encoding="utf-8"))
    rep = C.convert_zip(FIX / f"{name}.zip", mf, tmp_path / f"{name}.zip")
    new_mf = C.write_manifest(tmp_path / f"{name}.zip", mf["docs"], tmp_path / f"{name}.manifest.json")
    return rep, new_mf, tmp_path / f"{name}.zip"


def test_law_payload_has_identity_from_manifest_mst(tmp_path):
    rep, _, _ = _convert("batch-20261021-001", tmp_path)
    assert rep["errors"] == []
    p = rep["payloads"][0]
    # mst 는 본문 응답에 없고 manifest 로만 온다 — version_uid 가 기존 적재분과 같은 꼴이어야 한다
    assert p["law_id"] == "001282" and p["mst"] == "290605"
    assert p["version_uid"] == "001282:290605:20261002"
    assert p["articles"] == 4 and p["appendices"] == 2


def test_admrul_payload_from_raw_body(tmp_path):
    rep, _, _ = _convert("batch-20261021-002", tmp_path)
    assert rep["errors"] == []
    p = rep["payloads"][0]
    assert p["law_id"] == "37584" and p["mst"] == "2100000285778"
    assert p["articles"] == 73 and p["appendices"] == 39 and p["attachments"] == 1


def test_appendix_files_are_renamed_to_declared_names_and_deduped(tmp_path):
    """[별표 1] 제목(법령명).hwp → 별표1_제목.pdf. 같은 별표의 hwp/pdf/gif 는 하나만."""
    rep, _, zp = _convert("batch-20261021-001", tmp_path)
    with zipfile.ZipFile(zp) as z:
        names = sorted(n.rsplit("/", 1)[-1] for n in z.namelist() if not n.endswith(".json"))
    assert names == ["별표1_검사의 봉급표.pdf", "별표2_검사의 봉급기준표.pdf"]
    assert rep["renamed"] == 2 and rep["dropped"] == 4


def test_document_attachment_keeps_its_name(tmp_path):
    """원문 첨부는 attachments[].name 과 이미 같으므로 손대지 않는다."""
    _, _, zp = _convert("batch-20261021-002", tmp_path)
    with zipfile.ZipFile(zp) as z:
        assert any(n.endswith("「수용자 교육교화 운영지침」 일부 개정 전문.hwpx") for n in z.namelist())


def test_new_manifest_matches_converted_zip(tmp_path):
    """원본 manifest 의 sha256 은 원본 ZIP 것이다 — 변환본엔 새로 계산한 것이 붙어야 한다."""
    _, mf, zp = _convert("batch-20261021-001", tmp_path)
    assert mf["sha256"] == hashlib.sha256(zp.read_bytes()).hexdigest()
    assert mf["bytes"] == zp.stat().st_size
    assert mf["docs"][0]["mst"] == "290605"
    with zipfile.ZipFile(zp) as z:
        assert {f["path"] for f in mf["files"]} == {i.filename for i in z.infolist() if not i.is_dir()}


def test_converted_zip_passes_read_zip_with_no_errors(tmp_path):
    """끝까지 — 변환본이 적재기 read_zip 을 오류 0 으로 통과하고 첨부가 문서에 연결돼야 한다."""
    for name, files in (("batch-20261021-001", 2), ("batch-20261021-002", 40)):
        _, mf, zp = _convert(name, tmp_path)
        r = read_zip(zp, manifest=mf)
        assert r.errors == [] and len(r.documents) == 1 and r.documents[0].accepted
        assert len(r.documents[0].files) == files


def test_law_without_mst_is_rejected(tmp_path):
    """mst 없이 변환하면 version_uid 가 '001282::…' 가 되어 중복 버전을 만든다 — 막는다."""
    mf = json.loads((FIX / "batch-20261021-001.manifest.json").read_text(encoding="utf-8"))
    mf["docs"][0] = {**mf["docs"][0], "mst": ""}
    rep = C.convert_zip(FIX / "batch-20261021-001.zip", mf, tmp_path / "x.zip")
    assert rep["documents"] == 0 and "mst" in rep["errors"][0]["error"]


def test_plan_files_does_not_confuse_1_with_10():
    payload = {"appendices": [{"filename": "별지1_가.pdf"}, {"filename": "별지10_나.pdf"}], "attachments": []}
    rename, drop = C.plan_files(payload, ["d/[별지 1] 가(x).pdf", "d/[별지 10] 나(x).pdf"])
    assert rename == {"d/[별지 1] 가(x).pdf": "별지1_가.pdf", "d/[별지 10] 나(x).pdf": "별지10_나.pdf"}
    assert drop == []


# ── 결정성 · 게시 규약 · ZIP 단위 격리 (LAW-28 운영 가동 전제) ─────────────
#
# 자동 경로에서는 변환기가 매 실행 돈다. 같은 입력이면 같은 바이트를 내야 consume-zip 의 SHA-256
# 중복 방지가 재변환본을 알아보고, 한 ZIP 의 실패가 뒤 ZIP 을 막지 않아야 하며, 반쯤 쓴 산출물을
# consume-zip 이 집어가지 않아야 한다.

def _inbox_with(tmp_path, *names):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    for n in names:
        shutil.copy(FIX / f"{n}.zip", inbox / f"{n}.zip")
        shutil.copy(FIX / f"{n}.manifest.json", inbox / f"{n}.manifest.json")
    return inbox


def test_conversion_is_deterministic(tmp_path, monkeypatch):
    """시각(fetched_at·zip 타임스탬프)이 '지금' 이 아니라 입력에서 나와야 한다."""
    _, mf1, zp1 = _convert("batch-20261021-001", tmp_path / "a")
    monkeypatch.setattr(C._c, "now_kst", lambda: "2099-01-01T00:00:00+09:00")
    monkeypatch.setattr("zipfile.time.time", lambda: 4102444800.0)
    _, mf2, zp2 = _convert("batch-20261021-001", tmp_path / "b")
    assert zp1.read_bytes() == zp2.read_bytes()
    assert mf1["sha256"] == mf2["sha256"]


def test_convert_inbox_publishes_atomically_and_archives(tmp_path, monkeypatch):
    """manifest 가 먼저 최종 이름으로 놓이고 ZIP 은 .part 에서 rename 된다. 원본 쌍은 done/ 로."""
    inbox = _inbox_with(tmp_path, "batch-20261021-001")
    outbox, done = tmp_path / "out", tmp_path / "done"
    seen, real_replace = [], os.replace

    def spy(src, dst):
        if Path(dst).parent == outbox and str(dst).endswith(".zip"):
            seen.append((Path(src).name, (outbox / "batch-20261021-001.manifest.json").exists()))
        real_replace(src, dst)

    monkeypatch.setattr(C.os, "replace", spy)
    reps = C.convert_inbox(inbox, outbox, done_dir=done)
    assert [r["status"] for r in reps] == ["converted"]
    assert seen == [(".batch-20261021-001.zip.part", True)]
    assert (outbox / "batch-20261021-001.zip").exists() and not list(outbox.glob(".*.part"))
    assert not (inbox / "batch-20261021-001.zip").exists()
    assert (done / "batch-20261021-001.zip").exists() and (done / "batch-20261021-001.manifest.json").exists()
    mf = json.loads((outbox / "batch-20261021-001.manifest.json").read_text(encoding="utf-8"))
    assert mf["file"] == "batch-20261021-001.zip" and mf["source"]["sha256"]


def test_broken_zip_is_isolated_and_others_continue(tmp_path):
    """깨진 ZIP·manifest 없는 ZIP 이 뒤 ZIP 을 막지 않는다. 실패분은 failed/ 로 보고와 함께, 게시물 없음."""
    inbox = _inbox_with(tmp_path, "batch-20261021-002")
    (inbox / "batch-20261021-000.zip").write_bytes(b"not a zip")
    (inbox / "batch-20261021-000.manifest.json").write_text("{}", encoding="utf-8")
    (inbox / "batch-20261021-003.zip").write_bytes(b"x")       # manifest 없음
    outbox, done, failed = tmp_path / "out", tmp_path / "done", tmp_path / "failed"
    reps = {r["zip"]: r["status"] for r in C.convert_inbox(inbox, outbox, done_dir=done, failed_dir=failed)}
    assert reps == {"batch-20261021-000.zip": "failed", "batch-20261021-002.zip": "converted",
                    "batch-20261021-003.zip": "pending_manifest"}
    assert sorted(p.name for p in outbox.iterdir()) == ["batch-20261021-002.manifest.json",
                                                         "batch-20261021-002.zip"]
    assert (failed / "batch-20261021-000.zip").exists()
    assert (failed / "batch-20261021-000.convert-report.json").exists()
    assert (inbox / "batch-20261021-003.zip").exists()            # 제자리 — manifest 를 기다린다


def test_partial_document_failure_publishes_nothing(tmp_path):
    """문서 하나라도 실패하면 게시하지 않는다 — 일부만 담으면 consume-zip 이 manifest_mismatch 로 통째 거부."""
    inbox = _inbox_with(tmp_path, "batch-20261021-001")
    mf_path = inbox / "batch-20261021-001.manifest.json"
    mf = json.loads(mf_path.read_text(encoding="utf-8"))
    mf["docs"][0]["mst"] = ""
    mf_path.write_text(json.dumps(mf, ensure_ascii=False), encoding="utf-8")
    outbox, failed = tmp_path / "out", tmp_path / "failed"
    [rep] = C.convert_inbox(inbox, outbox, failed_dir=failed)
    assert rep["status"] == "failed" and "mst" in rep["errors"][0]["error"]
    assert list(outbox.iterdir()) == []
    assert (failed / "batch-20261021-001.zip").exists()
    report = json.loads((failed / "batch-20261021-001.convert-report.json").read_text(encoding="utf-8"))
    assert report["status"] == "failed"


def test_same_source_is_not_reconverted(tmp_path):
    """같은 원본이 제자리에 남아 있어도(수동 운영) 변환본을 덮어쓰지 않는다 — 변환본 SHA 보존."""
    inbox = _inbox_with(tmp_path, "batch-20261021-001")
    outbox = tmp_path / "out"
    [first] = C.convert_inbox(inbox, outbox)
    before = (outbox / "batch-20261021-001.zip").stat().st_mtime_ns
    [second] = C.convert_inbox(inbox, outbox)
    assert first["status"] == "converted" and second["status"] == "skipped"
    assert (outbox / "batch-20261021-001.zip").stat().st_mtime_ns == before
