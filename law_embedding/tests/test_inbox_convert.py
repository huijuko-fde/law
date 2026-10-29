"""투라인 배달 ZIP → 적재기 정규화 ZIP 변환(law_indexer.inbox_convert) 검증.

fixture 는 2026-10-21 투라인이 실제로 보낸 배달분 2건(법령 1·행정규칙 1)이다 — 합성 데이터로는
law.go.kr 원본 응답의 모양(중첩 한글 키·CP949 파일명·여러 형식 재수출)을 재현하기 어렵다.
"""
import hashlib
import json
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
