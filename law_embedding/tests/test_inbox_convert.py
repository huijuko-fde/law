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
    with zipfile.ZipFile(tmp_path / "batch-20261021-001.zip") as z:
        body = json.loads(z.read([n for n in z.namelist() if n.endswith(".json")][0]))
    assert "lsiSeq=290605" in body["appendices"][0]["view_url"]       # 진짜 mst 면 별표 링크가 산다


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


def test_law_without_mst_falls_back_to_promulgation_no(tmp_path):
    """manifest 에 mst 가 없으면 본문 기본정보의 공포번호로 대신한다(투라인 운영 manifest 에는 mst 가 없다).
    그것도 없으면 version_uid 가 '001282::…' 가 되어 중복 버전을 만들므로 막는다."""
    import pytest
    mf = json.loads((FIX / "batch-20261021-001.manifest.json").read_text(encoding="utf-8"))
    mf["docs"][0] = {**mf["docs"][0], "mst": ""}
    rep = C.convert_zip(FIX / "batch-20261021-001.zip", mf, tmp_path / "x.zip")
    assert rep["documents"] == 1 and rep["mst_fallback"] == 1 and rep["payloads"][0]["mst"] == "21490"
    assert C._law_mst_fallback({}) == "" and C._law_mst_fallback({"법령": {"법령키": "0048672026110636801"}}) == "36801"
    with pytest.raises(C.ConvertError, match="mst"):
        C.assemble_law({}, "1", "", "x")
    rep = C.convert_zip(FIX / "batch-20261021-001.zip", {}, tmp_path / "y.zip")      # 문서 목록이 아예 없는 manifest
    assert rep["documents"] == 0 and "문서 목록" in rep["errors"][0]["error"]


def test_plan_files_does_not_confuse_1_with_10():
    payload = {"appendices": [{"filename": "별지1_가.pdf"}, {"filename": "별지10_나.pdf"}], "attachments": []}
    rename, drop = C.plan_files(payload, ["d/[별지 1] 가(x).pdf", "d/[별지 10] 나(x).pdf"])
    assert rename == {"d/[별지 1] 가(x).pdf": "별지1_가.pdf", "d/[별지 10] 나(x).pdf": "별지10_나.pdf"}
    assert drop == []


def test_plan_files_reads_form_and_unnumbered_labels():
    """2026-10-29 운영 배달분 — [별지 제N호서식] 은 서식N_, 번호 없는 [별표]·[별지] 는 별표0_·별지0_ 로 선언된다.
    전에는 원본 이름을 못 읽어 첨부가 전부 attachment_orphan 이 되고 ZIP 이 partial(HALT)로 멈췄다."""
    payload = {"attachments": [], "appendices": [
        {"filename": "서식100_압수ㆍ수색ㆍ검증영장 신청서(사후).pdf"},
        {"filename": "서식3의2_가 신청서.pdf"},
        {"filename": "별표0_기관장 감독보고 시기(제8조 관련).hwp"},
        {"filename": "별지0_조사개시통지서(제17조 제3항 관련).pdf"},
    ]}
    d = "law/시행규칙/중대범죄수사청 사건사무규칙/별표/"
    a = "admrul/훈령/교정본부 보고사무지침/별표/"
    members = [
        d + "[별지 제100호서식] 압수ㆍ수색ㆍ검증영장 신청서(사후)(중대범죄수사청 사건사무규칙).hwp",
        d + "[별지 제100호서식] 압수ㆍ수색ㆍ검증영장 신청서(사후)(중대범죄수사청 사건사무규칙).pdf",
        d + "[별지 제3호의2서식] 가 신청서(중대범죄수사청 사건사무규칙).pdf",
        a + "[별표] 기관장 감독보고 시기(제8조 관련)(교정본부 보고사무지침).hwp",
        a + "[별지] 조사개시통지서(제17조 제3항 관련)(교정특별사법경찰 운영규정).hwpx",
        a + "[별지] 조사개시통지서(제17조 제3항 관련)(교정특별사법경찰 운영규정).pdf",
        a + "[별표] 다른 제목의 번호 없는 별표(교정본부 보고사무지침).hwp",   # 키는 같지만 제목이 달라 잇지 않는다
    ]
    rename, drop = C.plan_files(payload, members)
    assert rename == {
        members[1]: "서식100_압수ㆍ수색ㆍ검증영장 신청서(사후).pdf",
        members[2]: "서식3의2_가 신청서.pdf",
        members[3]: "별표0_기관장 감독보고 시기(제8조 관련).hwp",
        members[5]: "별지0_조사개시통지서(제17조 제3항 관련).pdf",
    }
    assert sorted(drop) == sorted([members[0], members[4]])          # 같은 서식의 다른 형식만 버린다
    assert members[6] not in rename and members[6] not in drop


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
    mf["docs"][0]["dir"] = "law/법률/없는 폴더"                 # 문서 하나가 manifest 와 안 맞는다
    mf_path.write_text(json.dumps(mf, ensure_ascii=False), encoding="utf-8")
    outbox, failed = tmp_path / "out", tmp_path / "failed"
    [rep] = C.convert_inbox(inbox, outbox, failed_dir=failed)
    assert rep["status"] == "failed" and "문서 폴더" in rep["errors"][0]["error"]
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


def test_undeclared_junk_and_unsupported_files_are_dropped(tmp_path):
    """잡파일·지원 외 확장자는 변환본에서 뺀다 — 소비 쪽에서 ZIP 오류가 되어 전체가 partial→HALT 로 멈추던 것
    (2026-11-05 전체 검증 PS-4). 선언된 첨부와 지원 확장자의 미선언 파일은 그대로 둔다."""
    src = tmp_path / "batch-20261021-001.zip"
    shutil.copy(FIX / "batch-20261021-001.zip", src)
    folder = "law/법률/검사의 보수에 관한 법률"
    with zipfile.ZipFile(src, "a") as z:
        z.writestr(f"{folder}/Thumbs.db", b"junk")
        z.writestr(f"{folder}/notes.txt", b"memo")
        z.writestr(f"{folder}/별표/~$임시.hwp", b"lock")
    mf = json.loads((FIX / "batch-20261021-001.manifest.json").read_text(encoding="utf-8"))
    rep = C.convert_zip(src, mf, tmp_path / "out.zip")
    assert rep["errors"] == [] and rep["dropped_unsupported"] == 3 and rep["renamed"] == 2
    with zipfile.ZipFile(tmp_path / "out.zip") as z:
        names = sorted(n.rsplit("/", 1)[-1] for n in z.namelist() if not n.endswith(".json"))
    assert names == ["별표1_검사의 봉급표.pdf", "별표2_검사의 봉급기준표.pdf"]


# ── 투라인 운영 배달 형식 (2026-10-27 이후 실제 배달분 — 10/21 샘플과 다르다) ──────────────

def _tworine_style(name, tmp_path):
    """10/21 샘플 ZIP 을 투라인 운영 배달 모양으로 다시 싼다 — documents[] manifest(mst 없음), 별표·첨부 개명
    (별표/별표1.hwp, 첨부/첨부1.hwpx, 원래 이름은 files[].originalName), ZIP 안에 manifest.json 동봉."""
    old = json.loads((FIX / f"{name}.manifest.json").read_text(encoding="utf-8"))
    out = tmp_path / f"{name}-tworine.zip"
    by_dir, counters = {}, {}
    with zipfile.ZipFile(FIX / f"{name}.zip") as zin, zipfile.ZipFile(out, "w") as zout:
        names = [n for n in zin.namelist() if not n.endswith("/")]
        for n in [n for n in names if n.endswith(".json")]:
            data = zin.read(n)
            folder, base = n.rsplit("/", 1)
            d = next(x for x in old["docs"] if x["dir"] == folder)
            by_dir[folder] = {"path": n, "lawId": d["id"], "promulgationDate": "20261106",
                              "sha256": hashlib.sha256(data).hexdigest(),
                              "docType": "LAW" if folder.startswith("law/") else "ADMIN_RULE",
                              "kind": folder.split("/")[1], "name": base[:-5], "files": [], "skippedFiles": [],
                              "effectiveDate": "20261106", "ministry": "법무부"}
            zout.writestr(n, data)
        for n in [n for n in names if not n.endswith(".json")]:
            data = zin.read(n)
            base = n.rsplit("/", 1)[-1]
            doc_dir = max((d for d in by_dir if n.startswith(d + "/")), key=len)
            m = C._ORIG.match(base)
            ext = base.rsplit(".", 1)[-1]
            if m:
                new = f"{m['kind']}{int(m['no'])}" + (f"의{int(m['branch'])}" if m["branch"] else "") + f".{ext}"
                new_path, kind = f"{doc_dir}/별표/{new}", ("PDF" if ext == "pdf" else "서식파일")
            else:
                counters[doc_dir] = counters.get(doc_dir, 0) + 1
                new_path, kind = f"{doc_dir}/첨부/첨부{counters[doc_dir]}.{ext}", "첨부"
            by_dir[doc_dir]["files"].append({"originalName": base, "path": new_path, "size": len(data),
                                             "sha256": hashlib.sha256(data).hexdigest(), "kind": kind, "title": base})
            zout.writestr(new_path, data)
        manifest = {"documentCount": len(by_dir), "documents": list(by_dir.values()), "part": 1,
                    "generatedAt": "2026-11-05T20:34:27.443311813Z", "source": "국가법령정보센터(법제처)"}
        zout.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False))
    return out, manifest


def test_tworine_delivery_format_is_converted(tmp_path):
    """documents[] manifest · 개명된 별표 · 동봉 manifest.json · law 에 mst 없음(공포번호로 대체) — 전부 받는다.
    10/27 첫 배달이 'manifest docs[] 에 이 문서 폴더가 없습니다' 로 HALT 돼 열흘간 31건이 쌓였던 것."""
    zp, mf = _tworine_style("batch-20261021-001", tmp_path)
    rep = C.convert_zip(zp, mf, tmp_path / "out.zip")
    assert rep["errors"] == [] and rep["documents"] == 1 and rep["ignored"] == 1 and rep["mst_fallback"] == 1
    p = rep["payloads"][0]
    assert p["mst"] == "21490" and p["version_uid"] == "001282:21490:20261002"       # 기본정보.공포번호
    assert rep["renamed"] == 2 and rep["dropped"] == 4                                   # hwp/pdf/gif → pdf 하나
    with zipfile.ZipFile(tmp_path / "out.zip") as z:
        assert "manifest.json" not in z.namelist()
        names = sorted(n.rsplit("/", 1)[-1] for n in z.namelist() if not n.endswith(".json"))
    assert names == ["별표1_검사의 봉급표.pdf", "별표2_검사의 봉급기준표.pdf"]
    new_mf = C.write_manifest(tmp_path / "out.zip", rep["docs"], tmp_path / "out.manifest.json")
    assert new_mf["docs"] == [{"id": "001282", "mst": "21490", "dir": "law/법률/검사의 보수에 관한 법률"}]
    with zipfile.ZipFile(tmp_path / "out.zip") as z:
        body = json.loads(z.read([n for n in z.namelist() if n.endswith(".json")][0]))
    ap = body["appendices"][0]
    assert ap["view_url"] == "" and ap["file_url"]        # 대체 mst 는 법제처 별표 링크(lsiSeq)에 못 쓴다 — 비운다
    r = read_zip(tmp_path / "out.zip", manifest=new_mf)
    assert r.errors == [] and len(r.documents) == 1 and r.documents[0].accepted and len(r.documents[0].files) == 2


def test_tworine_admrul_delivery_keeps_attachment_and_seq(tmp_path):
    """행정규칙은 본문에 행정규칙일련번호가 있어 mst 대체 없이 그대로, 첨부(첨부/첨부1.hwpx)는 원래 이름으로."""
    zp, mf = _tworine_style("batch-20261021-002", tmp_path)
    rep = C.convert_zip(zp, mf, tmp_path / "out.zip")
    assert rep["errors"] == [] and rep["documents"] == 1 and rep["mst_fallback"] == 0
    assert rep["payloads"][0]["mst"] == "2100000285778"
    with zipfile.ZipFile(tmp_path / "out.zip") as z:
        assert any(n.endswith("「수용자 교육교화 운영지침」 일부 개정 전문.hwpx") for n in z.namelist())
    new_mf = C.write_manifest(tmp_path / "out.zip", rep["docs"], tmp_path / "out.manifest.json")
    r = read_zip(tmp_path / "out.zip", manifest=new_mf)
    assert r.errors == [] and r.documents[0].accepted and len(r.documents[0].files) == 40


def test_tworine_pair_round_trips_through_convert_inbox(tmp_path):
    """수신기가 놓은 모양 그대로(ZIP + 같은 이름 .manifest.json) convert_inbox → converted/ 의 manifest 에 docs[] 가 생긴다."""
    zp, mf = _tworine_style("batch-20261021-001", tmp_path)
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    shutil.copy(zp, inbox / "20261105-203428-313052_law-1-1.zip")
    (inbox / "20261105-203428-313052_law-1-1.manifest.json").write_text(json.dumps(mf, ensure_ascii=False), encoding="utf-8")
    reports = C.convert_inbox(inbox, tmp_path / "converted", done_dir=tmp_path / "done", failed_dir=tmp_path / "failed")
    assert [r["status"] for r in reports] == ["converted"]
    written = json.loads((tmp_path / "converted" / "20261105-203428-313052_law-1-1.manifest.json").read_text(encoding="utf-8"))
    assert written["docs"][0]["mst"] == "21490" and written["documents"] == 1


def test_documents_alias_keeps_slash_in_original_name_inside_one_folder():
    """2026-10-29 운영 배달분 — 원래 이름에 '/' 가 든 서식(…Apostille/ Certificate of Authentication…)이
    되돌린 경로에서 폴더를 하나 더 만들어 고아가 됐다. 경로 구분자는 '_' 로 바꿔 같은 폴더에 둔다."""
    manifest = {"documents": [{"path": "law/시행규칙/가/가.json", "lawId": "1", "files": [
        {"path": "law/시행규칙/가/별표/별표5.hwp",
         "originalName": "[별지 제5호서식] 신청서(Application for Apostille/ Certificate of Authentication)(가).hwp"},
    ]}]}
    _, alias, _ = C._documents_as_docs(manifest)
    restored = alias["law/시행규칙/가/별표/별표5.hwp"]
    assert restored == ("law/시행규칙/가/별표/"
                        "[별지 제5호서식] 신청서(Application for Apostille_ Certificate of Authentication)(가).hwp")
    payload = {"attachments": [], "appendices": [{"filename": "서식5_신청서(Application for Apostille_ Certificate of Authentication).pdf"}]}
    rename, _ = C.plan_files(payload, [restored])
    assert rename == {restored: "서식5_신청서(Application for Apostille_ Certificate of Authentication).hwp"}


def test_plan_files_restores_comma_that_lawgo_turned_into_cedilla():
    """법제처는 첨부 파일명의 ',' 를 '¸'(U+00B8)로 내려준다 — 본문 선언 이름(',')으로 되돌려야 연결된다."""
    declared = "2.「교정사고 유형별 문책기준 지침」개정안 전문(법무부예규 제1295호, 2022.4.1. 일부개정).hwp"
    payload = {"appendices": [], "attachments": [{"name": declared}, {"name": "1.교정사고 유형별 문책기준 지침 일부개정안.hwp"}]}
    d = "admrul/예규/교정사고 유형별 문책기준 지침/"
    members = [d + declared.replace(",", "¸"), d + "1.교정사고 유형별 문책기준 지침 일부개정안.hwp"]
    rename, drop = C.plan_files(payload, members)
    assert rename == {members[0]: declared} and drop == []


def test_plan_files_links_each_deleted_appendix_when_several_share_no_zero():
    """2026-11-06 재처리 뒤 남은 54건 — 한 법령에 삭제된 별표([별표 ] 삭제 <날짜>)가 여럿이면 전부 (별표,0) 라
    생산자가 `_flSeq<n>` 으로 파일명만 가른다. 선언을 하나만 남기던 전 버전은 나머지를 전부 고아로 만들었다."""
    payload = {"attachments": [], "appendices": [
        {"filename": "별표0_삭제 <2014.11.3.>_flSeq11111111.hwp"},
        {"filename": "별표0_삭제 <2016. 9. 27.>_flSeq22222222.hwp"},
        {"filename": "별표1_수수료(제5조 관련).pdf"},
    ]}
    d = "law/시행규칙/검사의 국선변호사 선정 등에 관한 규칙/별표/"
    members = [
        d + "[별표 ] 삭제 <2014.11.3.>(검사의 국선변호사 선정 등에 관한 규칙).hwp",
        d + "[별표 ] 삭제 <2014.11.3.>(검사의 국선변호사 선정 등에 관한 규칙).pdf",
        d + "[별표 ] 삭제 <2016. 9. 27.>(검사의 국선변호사 선정 등에 관한 규칙).hwp",
        d + "[별표 1] 수수료(제5조 관련)(검사의 국선변호사 선정 등에 관한 규칙).pdf",
        d + "[별표 ] 삭제 <2020.1.1.>(검사의 국선변호사 선정 등에 관한 규칙).hwp",   # 선언에 없는 날짜 — 건드리지 않는다
    ]
    rename, drop = C.plan_files(payload, members)
    assert rename == {
        members[0]: "별표0_삭제 <2014.11.3.>_flSeq11111111.hwp",
        members[2]: "별표0_삭제 <2016. 9. 27.>_flSeq22222222.hwp",
        members[3]: "별표1_수수료(제5조 관련).pdf",
    }
    assert drop == [members[1]]
    assert members[4] not in rename and members[4] not in drop
    assert C._declared_title("별표0_삭제 <2014.11.3.>_flSeq11111111.hwp") == "삭제 <2014.11.3.>"
    assert C._declared_title("별표3_개정 2014_연혁.pdf") == "개정 2014_연혁"      # 제목 속 '_' 와 숫자는 안 자른다


def test_plan_files_links_deleted_appendix_whose_declared_name_went_through_safe():
    """2026-11-09 남은 54건 전부 — 삭제된 별표·서식 `[별표 ] 삭제 <날짜>`·`[별지 서식] 삭제 <날짜>` 는 생산자가 제목의
    `<`·`>` 를 `_` 로 바꿔 `별표0_삭제 _2014.11.3._.pdf` 로 선언한다. 제목 비교를 _safe 로 접지 않으면 영영 안 맞는다.
    행정규칙 별표 제목의 `¸`(법제처가 바꾼 `,`)도 같은 길로 되돌린다."""
    payload = {"attachments": [], "appendices": [
        {"filename": "별표0_삭제 _2014.11.3._.pdf"},
        {"filename": "서식0_삭제 _2009.3.31_.pdf"},
        {"filename": "별표0_임상연구평가위원회의 설치기관, 대상기관, 관할 및 위원장.pdf"},
    ]}
    a = "law/시행규칙/검사의 국선변호사 선정 등에 관한 규칙/별표/"
    b = "law/시행령/공무원임용령/별표/"
    c = "admrul/훈령/교도소와 소년원 및 외국인보호소 등 의무관 임상연구비 지급지침/별표/"
    members = [
        a + "[별표 ] 삭제 <2014.11.3.>(검사의 국선변호사 선정 등에 관한 규칙).hwp",
        a + "[별표 ] 삭제 <2014.11.3.>(검사의 국선변호사 선정 등에 관한 규칙).pdf",
        b + "[별지 서식] 삭제 <2009.3.31>(공무원임용령).hwp",
        b + "[별지 서식] 삭제 <2009.3.31>(공무원임용령).pdf",
        c + "[별표] 임상연구평가위원회의 설치기관¸ 대상기관¸ 관할 및 위원장(교도소와 소년원 및 외국인보호소 등 의무관 임상연구비 지급지침).hwp",
        c + "[별표] 임상연구평가위원회의 설치기관¸ 대상기관¸ 관할 및 위원장(교도소와 소년원 및 외국인보호소 등 의무관 임상연구비 지급지침).pdf",
    ]
    rename, drop = C.plan_files(payload, members)
    assert rename == {
        members[1]: "별표0_삭제 _2014.11.3._.pdf",
        members[3]: "서식0_삭제 _2009.3.31_.pdf",
        members[5]: "별표0_임상연구평가위원회의 설치기관, 대상기관, 관할 및 위원장.pdf",
    }
    assert sorted(drop) == sorted([members[0], members[2], members[4]])     # 선언 확장자(pdf)가 우선, hwp 는 버린다


def test_plan_files_falls_back_to_title_for_label_forms_never_seen():
    """라벨 꼴이 처음 보는 것이어도(부표·양식·〔〕·【】·라벨 없음) 같은 폴더 선언 중 제목이 하나만 같으면 잇는다.
    변형이 나올 때마다 정규식을 늘리던 방식(2026-11-06~09 6종)을 끝내기 위한 것."""
    payload = {"attachments": [], "appendices": [
        {"filename": "별표3_수수료 기준.pdf"},
        {"filename": "별지5_신청서(사후).pdf"},
        {"filename": "서식7_확인서.pdf"},
    ]}
    d = "law/시행규칙/가법 시행규칙/별표/"
    members = [
        d + "[부표 3] 수수료 기준(가법 시행규칙).pdf",          # 종류 표기가 다름
        d + "〔별지 5〕 신청서(사후)(가법 시행규칙).pdf",       # 괄호 꼴이 다르고 제목 끝에 괄호
        d + "【양식 제7호】 확인서(가법 시행규칙).pdf",
    ]
    rename, drop = C.plan_files(payload, members)
    assert rename == {members[0]: "별표3_수수료 기준.pdf", members[1]: "별지5_신청서(사후).pdf",
                      members[2]: "서식7_확인서.pdf"}
    assert drop == []


def test_plan_files_title_fallback_never_guesses():
    """제목만으로 못 가르거나 라벨 번호가 선언과 어긋나면 잇지 않는다 — 다른 별표에 붙는 것이 고아보다 나쁘다."""
    payload = {"attachments": [], "appendices": [
        {"filename": "별지1_삭제.pdf"}, {"filename": "별지2_삭제.pdf"}, {"filename": "별표3_수수료 기준.pdf"},
    ]}
    d = "law/가법/별표/"
    members = [
        d + "[양식 1] 삭제(가법).pdf",        # 제목 '삭제' 가 둘 — 라벨 번호 1 로 가른다 → 별지1
        d + "삭제(가법).pdf",                 # 라벨이 없어 못 가른다 → 건드리지 않음
        d + "[부표 9] 수수료 기준(가법).pdf",  # 제목은 같지만 번호 9 ≠ 3 → 건드리지 않음
    ]
    rename, drop = C.plan_files(payload, members)
    assert rename == {members[0]: "별지1_삭제.pdf"} and drop == []
