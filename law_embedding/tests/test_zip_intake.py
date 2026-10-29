"""ZIP 수신 1단계(zip_intake, LAW-40·41) 검증 — 임시 디렉터리에서 ZIP 을 만들어 해석·검증·package 변환을 본다.

외부 접속 없음: Weaviate·임베딩·전처리기는 test_package.py 와 같은 fake 로 대신하고, 변환 결과는
기존 package 읽기 함수(is_package_file·peek_*)와 소비자(consume_package)로 왕복 확인한다.
"""
import base64
import dataclasses
import hashlib
import json
import stat
import unicodedata
import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from law_indexer.config import Settings
from law_indexer.package import (
    consume_package, is_package_file, peek_package_law_ids, peek_package_source,
)
from law_indexer.zip_intake import (ZipIntakeError, flatten_nested_zips, read_zip,
                                    write_packages)


# ── fake (test_package.py 와 같은 것) ────────────────────────────────────

class FakeStore:
    def __init__(self):
        self.deleted, self.stale, self.upserted = [], [], []

    def delete_by_law_id(self, law_id, collection):
        self.deleted.append(law_id)
        return 1

    def delete_stale_law_chunks(self, law_id, keep_version_uids, collection):
        self.stale.append((law_id, set(keep_version_uids)))
        return 0

    def upsert(self, objects, dimension, model_name, collection):
        self.upserted.extend(objects)
        return {"success": len(objects), "failed": 0, "failed_ids": []}


class FakeEmbedder:
    model_name = "fake"
    dimension = 3

    def embed_documents(self, texts):
        return [[0.0, 0.0, 0.0] for _ in texts]


class FakeDocParser:
    def run(self, request_path, chunk_size, chunk_overlap, endpoint_path=None):
        return [{"text": "전처리로 뽑은 파일 텍스트", "i_chunk_on_doc": 0, "i_page": 1, "e_page": 1}]


def _consume_settings(tmp_path, **extra):
    """소비자 설정 — 미러·원문보관·삭제·공유폴더를 꺼서 테스트가 파일시스템 밖을 건드리지 않게 한다."""
    fields = dict(
        package_preprocess_files=True, package_file_inbox_dir=None,
        package_delete_consumed_files=False, package_store_original=False,
        package_mirror_repo=False, package_delete_consumed_package=False,
        doc_parser_shared_dir=None, doc_parser_shared_host_dir=None,
        doc_parser_shared_container_dir=None, admrul_repo_path=tmp_path / "no_such_repo")
    fields.update(extra)
    return dataclasses.replace(Settings.from_env(), **fields)


# ── payload·ZIP 만들기 ──────────────────────────────────────────────────

def _law(law_id, name, *, version="1", law_type="법률", appendices=None, **extra):
    """수집기 법령 payload 최소형(samples/법률/형법.payload.json 의 필드 이름)."""
    payload = {
        "law_id": law_id, "mst": version, "version_uid": f"{law_id}:{version}:20260101",
        "law_name": name, "law_type": law_type, "is_current": True,
        "body": {"articles": [{"article_no": "제1조", "article_title": "목적",
                               "content": f"{name} 목적 조문", "provision_id": f"law:{name}#JO0001"}]},
        "appendices": appendices or [],
    }
    payload.update(extra)
    return payload


def _appendix(no, title, filename, *, owner, file_only=True):
    """수집기 별표 항목 — filename 은 enrich_file_meta 가 심는 저장 파일명."""
    return {"no": f"{no:04d}", "branch": "00", "kind": "별표", "title": title,
            "is_file_only": file_only, "content": "" if file_only else f"{title} 표 본문",
            "filename": filename, "file_url": f"https://example.invalid/byl/{no}",
            "files": [{"type": "hwp", "url": f"https://example.invalid/byl/{no}", "name": ""}],
            "provision_id": f"law:{owner}#BYL{no:04d}"}


def _admrul(law_id, name, *, doc_target="admrul", **extra):
    """수집기 행정규칙류 payload 최소형 — version_uid 없음(adm_uid + revision_date 로 합성된다)."""
    payload = {
        "law_id": law_id, "adm_uid": f"{doc_target}:{law_id}", "law_name": name, "law_type": "훈령",
        "doc_target": doc_target, "doc_kind": "행정규칙", "revision_date": "20260101",
        "body": {"articles": [{"article_no": "제1조", "content": f"{name} 조문",
                               "provision_id": f"admrul:{name}#JO0001"}]},
    }
    payload.update(extra)
    return payload


def _bytes(content):
    if isinstance(content, (dict, list)):
        return json.dumps(content, ensure_ascii=False, indent=2).encode("utf-8")
    return content.encode("utf-8") if isinstance(content, str) else content


def _zip(tmp_path, members, name="batch.zip"):
    path = tmp_path / name
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for arcname, content in members.items():
            zf.writestr(arcname, _bytes(content))
    return path


def _zip_with_raw_names(path, members):
    """bytes 이름은 UTF-8 플래그 없이 원시 바이트 그대로(한국어 윈도우·옛 맥 압축기 흉내), str 이름은
    zipfile 기본(비ASCII 면 UTF-8 플래그)으로 쓴다.

    zipfile 은 비ASCII 이름에 항상 UTF-8 플래그를 켜므로, 같은 길이의 ASCII 자리표시 이름으로 쓴 뒤
    이름 바이트만 바꿔 끼운다(로컬 헤더·중앙 디렉터리 두 곳). 이름은 CRC 대상이 아니다.
    바꿔 끼운 뒤 "a" 모드로 다시 열면 zipfile 이 중앙 디렉터리를 UTF-8 로 다시 써 버리므로 한 번에 만든다."""
    swaps = []
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as zf:
        for index, (name, content) in enumerate(members):
            if isinstance(name, str):
                zf.writestr(name, _bytes(content))
                continue
            placeholder = f"Q{index:03d}Q".ljust(len(name), "Z").encode("ascii")
            assert len(placeholder) == len(name)
            zf.writestr(placeholder.decode("ascii"), _bytes(content))
            swaps.append((placeholder, name))
    data = path.read_bytes()
    for placeholder, raw_name in swaps:
        assert data.count(placeholder) == 2
        data = data.replace(placeholder, raw_name)
    path.write_bytes(data)
    return path


def _make_gif(size=24):
    from PIL import Image
    buf = BytesIO()
    Image.new("RGB", (size, size), "white").save(buf, format="GIF")
    return buf.getvalue()


def _doc(result, path):
    return next(d for d in result.documents if d.path == path)


def _codes(issues):
    return sorted(i.code for i in issues)


def _records(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


# ── ZIP 구조 ────────────────────────────────────────────────────────────

def test_partner_layout_by_law_type(tmp_path):
    """법종구분별 구조(law/법률/형법/…, admrul/훈령/…) — 폴더 이름이 아니라 JSON 필드로 식별한다."""
    path = _zip(tmp_path, {
        "law/법률/형법/형법.json": _law("001692", "형법", appendices=[
            _appendix(1, "벌금표", "별표1_벌금표.hwp", owner="형법")]),
        "law/법률/형법/별표/별표1_벌금표.hwp": b"HWP-1",
        "law/시행령/도로교통법 시행령/도로교통법 시행령.json":
            _law("002001", "도로교통법 시행령", law_type="대통령령"),
        "admrul/훈령/수용자 교육교화 운영지침/수용자 교육교화 운영지침.json":
            _admrul("2100001", "수용자 교육교화 운영지침"),
    })
    result = read_zip(path)

    assert result.errors == [] and result.summary()["accepted"] == 3
    criminal = _doc(result, "law/법률/형법/형법.json")
    assert (criminal.source, criminal.law_id, criminal.law_type) == ("law", "001692", "법률")
    assert [u.file.path for u in criminal.units] == ["law/법률/형법/별표/별표1_벌금표.hwp"]
    # git_path 는 ZIP 경로가 아니라 데이터 레포 레이아웃({패밀리}/{종류}/{문서}.json)
    assert criminal.git_path == "형법/법률/형법.json"
    assert _doc(result, "law/시행령/도로교통법 시행령/도로교통법 시행령.json").git_path == \
        "도로교통법/시행령/도로교통법 시행령.json"
    rule = _doc(result, "admrul/훈령/수용자 교육교화 운영지침/수용자 교육교화 운영지침.json")
    assert rule.source == "admrul" and rule.version_uid == "admrul:2100001:20260101"


def test_collector_layout_json_list_decides_owner(tmp_path):
    """수집기 구조(law/<법률>/시행규칙/…) — 한 폴더의 여러 문서 중 별표 주인은 JSON 첨부 목록이 정한다.

    같은 파일명(별표1_수수료.hwp)이 법률 폴더와 시행규칙 폴더에 각각 있어도 가까운 폴더 기준으로 갈린다."""
    path = _zip(tmp_path, {
        "law/출입국관리법/출입국관리법.json": _law("000101", "출입국관리법", appendices=[
            _appendix(1, "수수료", "별표1_수수료.hwp", owner="출입국관리법")]),
        "law/출입국관리법/별표/별표1_수수료.hwp": b"LAW",
        "law/출입국관리법/시행령/출입국관리법 시행령.json":
            _law("000102", "출입국관리법 시행령", law_type="대통령령"),
        "law/출입국관리법/시행규칙/출입국관리법 시행규칙.json": _law(
            "000103", "출입국관리법 시행규칙", law_type="법무부령", appendices=[
                _appendix(1, "수수료", "별표1_수수료.hwp", owner="출입국관리법시행규칙")]),
        "law/출입국관리법/시행규칙/출입국관리법 시행규칙(외국인).json": _law(
            "000104", "출입국관리법 시행규칙(외국인)", law_type="법무부령", appendices=[
                _appendix(2, "서식", "별표2_서식.hwp", owner="출입국관리법시행규칙(외국인)")]),
        "law/출입국관리법/시행규칙/별표/별표1_수수료.hwp": b"RULE-A",
        "law/출입국관리법/시행규칙/별표/별표2_서식.hwp": b"RULE-B",
    })
    result = read_zip(path)

    assert result.errors == [] and result.summary()["accepted"] == 4
    files = {d.law_id: [f.path for f in d.files] for d in result.documents}
    assert files["000101"] == ["law/출입국관리법/별표/별표1_수수료.hwp"]
    assert files["000102"] == []
    assert files["000103"] == ["law/출입국관리법/시행규칙/별표/별표1_수수료.hwp"]
    assert files["000104"] == ["law/출입국관리법/시행규칙/별표/별표2_서식.hwp"]


def test_same_name_different_law_id_both_loaded(tmp_path):
    """형법/형법2 처럼 같은 법명이 다른 law_id 면 서로 다른 문서 — 둘 다 싣는다."""
    path = _zip(tmp_path, {
        "law/법률/형법/형법.json": _law("001692", "형법"),
        "law/법률/형법2/형법.json": _law("009999", "형법"),
    })
    result = read_zip(path)
    assert result.summary()["accepted"] == 2

    [written] = write_packages(result, tmp_path / "out")
    source, law_ids = peek_package_law_ids(Path(written["path"]))
    assert source == "law" and sorted(law_ids) == ["001692", "009999"]
    assert written["documents"] == 2


def test_same_law_id_two_versions_rejected(tmp_path):
    """같은 law_id 의 다른 version_uid(현행+시행예정 등)는 함께 싣지 않고 그 law_id 전체를 거부한다 — 임시
    정책: 소비자는 law_id 당 한 버전만 보관하고, 동시 보관 여부는 기획 결정 대기. 다른 문서는 그대로 싣는다."""
    path = _zip(tmp_path, {
        "law/법률/형법/형법.json": _law("001692", "형법", version="1", appendices=[
            _appendix(1, "벌금표", "별표1_벌금표.hwp", owner="형법")]),
        "law/법률/형법/별표/별표1_벌금표.hwp": b"V1",
        "law/법률/형법2/형법.json": _law("001692", "형법", version="2", appendices=[
            _appendix(1, "벌금표", "별표1_벌금표.hwp", owner="형법")]),
        "law/법률/형법2/별표/별표1_벌금표.hwp": b"V2",
        "law/법률/민법/민법.json": _law("001706", "민법"),
    })
    result = read_zip(path)

    for doc in (_doc(result, "law/법률/형법/형법.json"), _doc(result, "law/법률/형법2/형법.json")):
        assert _codes(doc.errors) == ["multi_version_unsupported"]
        assert "기획 결정 전" in doc.errors[0].message
    assert result.errors == []                         # 거부된 버전의 별표가 고아 오류로 다시 잡히지 않는다
    assert [d.law_id for d in result.accepted()] == ["001706"]
    assert result.summary()["rejected"] == 2


def test_multi_version_law_id_is_not_loaded(tmp_path):
    """다중 버전 거부의 소비 결과 — 그 law_id 는 package 에 아무것도 실리지 않아 VDB 가 건드려지지 않는다
    (어느 한 버전만 들어가거나 기존 청크가 옛 버전 정리로 지워지는 일이 없다). 소비자 동작은 그대로다."""
    path = _zip(tmp_path, {
        "law/법률/형법/형법.json": _law("001692", "형법", version="1"),
        "law/법률/형법2/형법.json": _law("001692", "형법", version="2"),
        "law/법률/민법/민법.json": _law("001706", "민법"),
    })
    [written] = write_packages(read_zip(path), tmp_path / "out")
    assert peek_package_law_ids(Path(written["path"]))[1] == ["001706"]

    store = FakeStore()
    totals = consume_package(store, FakeEmbedder(), _consume_settings(tmp_path), Path(written["path"]),
                             doc_parser=FakeDocParser())
    assert totals["errors"] == [] and totals["documents"] == 1
    assert {o.law_id for o in store.upserted} == {"001706"}
    assert "001692" not in store.deleted and all(law_id != "001692" for law_id, _keep in store.stale)


# ── 검증 ────────────────────────────────────────────────────────────────

def test_same_version_uid_conflict_or_duplicate(tmp_path):
    """같은 version_uid — 내용이 다르면 충돌(둘 다 거부), 완전히 같으면 중복 경고(하나만)."""
    other = _law("001692", "형법")
    other["body"]["articles"][0]["content"] = "다른 내용"
    path = _zip(tmp_path, {
        "law/a/형법.json": _law("001692", "형법"),
        "law/b/형법.json": other,
        "law/c/민법.json": _law("001706", "민법"),
        "law/d/민법.json": _law("001706", "민법"),
    })
    result = read_zip(path)

    assert _codes(_doc(result, "law/a/형법.json").errors) == ["version_conflict"]
    assert _codes(_doc(result, "law/b/형법.json").errors) == ["version_conflict"]
    kept, dup = _doc(result, "law/c/민법.json"), _doc(result, "law/d/민법.json")
    assert kept.accepted and dup.duplicate_of == "law/c/민법.json"
    assert _codes(dup.warnings) == ["duplicate"]
    assert [d.path for d in result.accepted()] == ["law/c/민법.json"]


def test_law_id_missing_or_not_string_rejected(tmp_path):
    """law_id 없음·빈 값·숫자는 거부한다. 거부된 문서의 첨부는 고아 오류로 한 번 더 잡히지 않는다."""
    no_id = _law("x", "무번호법", appendices=[_appendix(1, "표", "별표1_표.hwp", owner="무번호법")])
    del no_id["law_id"]
    path = _zip(tmp_path, {
        "law/a/무번호법.json": no_id,
        "law/a/별표/별표1_표.hwp": b"X",
        "law/b/빈번호법.json": _law("  ", "빈번호법"),
        "law/c/숫자번호법.json": _law(1692, "숫자번호법"),
        "law/d/형법.json": _law("001692", "형법"),
    })
    result = read_zip(path)

    assert _codes(_doc(result, "law/a/무번호법.json").errors) == ["law_id_missing"]
    assert _codes(_doc(result, "law/b/빈번호법.json").errors) == ["law_id_missing"]
    assert _codes(_doc(result, "law/c/숫자번호법.json").errors) == ["law_id_not_string"]
    assert [d.law_id for d in result.accepted()] == ["001692"]
    assert result.errors == []


def test_orphan_attachment_is_error(tmp_path):
    """연결할 문서가 없는 첨부는 오류 — 문서는 그대로 싣는다."""
    path = _zip(tmp_path, {
        "law/형법/형법.json": _law("001692", "형법"),
        "law/형법/별표/별표9_없음.hwp": b"X",          # 문서는 있으나 JSON 첨부 목록에 없음
        "law/떠돌이/별표1.hwp": b"Y",                  # 상위 폴더 어디에도 문서 JSON 없음
    })
    result = read_zip(path)

    assert _codes(result.errors) == ["attachment_orphan", "attachment_orphan"]
    assert {e.path for e in result.errors} == {"law/형법/별표/별표9_없음.hwp", "law/떠돌이/별표1.hwp"}
    assert [d.law_id for d in result.accepted()] == ["001692"]


def test_ambiguous_attachment_rejects_candidates(tmp_path):
    """한 폴더의 두 문서가 같은 파일명을 첨부 목록에 가지면 소속을 추측하지 않고 둘 다 거부한다."""
    path = _zip(tmp_path, {
        "law/X/시행규칙/X 시행규칙.json": _law("000201", "X 시행규칙", appendices=[
            _appendix(1, "가", "별표1_가.hwp", owner="X시행규칙")]),
        "law/X/시행규칙/X 시행규칙(별도).json": _law("000202", "X 시행규칙(별도)", appendices=[
            _appendix(1, "가", "별표1_가.hwp", owner="X시행규칙(별도)")]),
        "law/X/시행규칙/별표/별표1_가.hwp": b"?",
    })
    result = read_zip(path)

    assert all(_codes(d.errors) == ["attachment_ambiguous"] for d in result.documents)
    assert result.accepted() == []
    assert write_packages(result, tmp_path / "out") == []     # 실을 문서가 없으면 package 도 없다


def test_repealed_becomes_delete_record(tmp_path):
    """repealed: true → delete record(기존 형식 {record_type, law_id}). 첨부는 싣지 않는다."""
    path = _zip(tmp_path, {
        "law/폐지법/폐지법.json": {"law_id": "001700", "law_name": "폐지법", "repealed": True,
                                 "repealed_at": "20261019",
                                 "appendices": [_appendix(1, "표", "별표1_표.hwp", owner="폐지법")]},
        "law/폐지법/별표/별표1_표.hwp": b"OLD",
    })
    result = read_zip(path)

    repealed = _doc(result, "law/폐지법/폐지법.json")
    assert repealed.accepted and repealed.repealed
    assert _codes(repealed.warnings) == ["repealed_with_files"]

    [written] = write_packages(result, tmp_path / "out")
    records = _records(written["path"])
    assert [r["record_type"] for r in records] == ["package_header", "delete", "package_footer"]
    assert records[1] == {"record_type": "delete", "law_id": "001700"}
    store = FakeStore()
    totals = consume_package(store, FakeEmbedder(), _consume_settings(tmp_path), Path(written["path"]),
                             doc_parser=FakeDocParser())
    assert store.deleted == ["001700"] and totals["deleted_docs"] == 1 and totals["errors"] == []


def test_repealed_doc_uid_without_law_id(tmp_path):
    """본문 없는 폐지 JSON(수집기 MANIFEST_ONLY — law_id 없이 doc_uid 만)은 doc_uid 가 법령
    `law:eflaw:<law_id>`(수집기 make_doc_uid)일 때만 delete 로 싣는다. 행정규칙류 doc_uid 는 id 가
    law_id 가 아니라(수집기 사본마다 이름 키·행정규칙ID) 기존처럼 거부한다."""
    path = _zip(tmp_path, {
        "law/무번호폐지법/무번호폐지법.json": {"doc_uid": "law:eflaw:001701", "law_name": "무번호폐지법",
                                         "repealed": True, "repealed_at": "20261019"},
        "admrul/폐지지침/폐지지침.json": {"doc_uid": "admrul:admrul:폐지지침", "law_name": "폐지지침",
                                    "repealed": True, "repealed_at": "20261019"},
        "admrul/잘못된위치/잘못된위치.json": {"doc_uid": "law:eflaw:001702", "law_name": "잘못된위치",
                                      "repealed": True, "repealed_at": "20261019"},
        "law/폐지아님/폐지아님.json": {"doc_uid": "law:eflaw:001703", "law_name": "폐지아님"},
    })
    result = read_zip(path)

    repealed = _doc(result, "law/무번호폐지법/무번호폐지법.json")
    assert repealed.accepted and repealed.repealed and repealed.law_id == "001701"
    assert _codes(repealed.warnings) == ["law_id_from_doc_uid"]
    assert _codes(_doc(result, "admrul/폐지지침/폐지지침.json").errors) == ["law_id_missing"]
    assert _codes(_doc(result, "admrul/잘못된위치/잘못된위치.json").errors) == ["source_mismatch"]
    assert _codes(_doc(result, "law/폐지아님/폐지아님.json").errors) == ["law_id_missing"]   # 폐지가 아니면 안 꺼낸다

    [written] = write_packages(result, tmp_path / "out")
    records = _records(written["path"])
    assert [r["record_type"] for r in records] == ["package_header", "delete", "package_footer"]
    assert records[1] == {"record_type": "delete", "law_id": "001701"}
    store = FakeStore()
    totals = consume_package(store, FakeEmbedder(), _consume_settings(tmp_path), Path(written["path"]),
                             doc_parser=FakeDocParser())
    assert store.deleted == ["001701"] and totals["deleted_docs"] == 1 and totals["errors"] == []


def test_manifest_restores_renamed_attachment(tmp_path):
    """내부 수집기 zipbatch 는 한 문서의 같은 이름·다른 내용 첨부를 `{stem}__{sha12}{ext}` 로 바꿔 싣고 그
    사실을 manifest(changes[].attachment_files)에만 남긴다. manifest 를 주면 원래 이름·URL 로 되돌려 각
    별표에 맞는 파일을 붙인다 — manifest 가 없으면 바뀐 이름은 고아 오류다(기존 규칙)."""
    first, second = b"HWP-FIRST", b"HWP-SECOND"
    renamed = f"별표1_수수료__{hashlib.sha256(second).hexdigest()[:12]}.hwp"
    appendix_a = _appendix(1, "수수료", "별표1_수수료.hwp", owner="형법")
    appendix_b = dict(_appendix(1, "수수료", "별표1_수수료.hwp", owner="형법"), branch="02",
                      file_url="https://example.invalid/byl/1-2",
                      files=[{"type": "hwp", "url": "https://example.invalid/byl/1-2", "name": ""}],
                      provision_id="law:형법#BYL0001-2")
    members = {
        "law/형법/형법.json": _law("001692", "형법", appendices=[appendix_a, appendix_b]),
        "law/형법/별표/별표1_수수료.hwp": first,
        f"law/형법/별표/{renamed}": second,
    }
    manifest = {"changes": [{"doc_uid": "law:eflaw:001692", "name": "형법", "attachment_files": [
        {"url": appendix_a["file_url"], "path": "law/형법/별표/별표1_수수료.hwp",
         "sha256": hashlib.sha256(first).hexdigest()},
        {"url": appendix_b["file_url"], "path": f"law/형법/별표/{renamed}",
         "sha256": hashlib.sha256(second).hexdigest()}]}]}

    assert _codes(read_zip(_zip(tmp_path, members, name="plain.zip")).errors) == ["attachment_orphan"]

    result = read_zip(_zip(tmp_path, members, name="mapped.zip"), manifest=manifest)
    assert result.errors == [] and result.documents[0].accepted
    assert {u.provision_id: u.file.path for u in result.documents[0].units} == {
        "law:형법#BYL0001": "law/형법/별표/별표1_수수료.hwp",
        "law:형법#BYL0001-2": f"law/형법/별표/{renamed}"}
    [written] = write_packages(result, tmp_path / "out")
    contents = {r["provision_id"]: base64.b64decode(r["content_b64"])
                for r in _records(written["path"]) if r["record_type"] == "file"}
    assert contents == {"law:형법#BYL0001": first, "law:형법#BYL0001-2": second}


def test_unsafe_paths_rejected(tmp_path):
    """경로 탈출(..)·절대경로·심볼릭 링크 멤버는 받지 않는다 — 나머지 문서는 그대로."""
    path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("../evil.json", _bytes(_law("000001", "탈출법")))
        zf.writestr("/abs/evil.json", _bytes(_law("000002", "절대법")))
        zf.writestr("law/형법/../../../evil.hwp", b"X")
        link = zipfile.ZipInfo("law/형법/별표/link.hwp")
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        zf.writestr(link, "../../../../etc/passwd")
        zf.writestr("law/형법/형법.json", _bytes(_law("001692", "형법")))
    result = read_zip(path)

    assert _codes(result.errors) == ["path_unsafe"] * 4
    assert [d.law_id for d in result.accepted()] == ["001692"]


def test_korean_filenames_without_utf8_flag(tmp_path):
    """UTF-8 플래그 없는 한글 이름(CP949·UTF-8)과 맥 NFD 이름을 모두 JSON 파일명과 맞춘다."""
    cp949 = lambda s: s.encode("cp949")          # noqa: E731
    utf8 = lambda s: s.encode("utf-8")           # noqa: E731
    path = _zip_with_raw_names(tmp_path / "ko.zip", [
        (cp949("law/형법/형법.json"), _law("001692", "형법", appendices=[
            _appendix(1, "벌금표", "별표1_벌금표.hwp", owner="형법")])),
        (cp949("law/형법/별표/별표1_벌금표.hwp"), b"CP949"),
        (utf8("law/민법/민법.json"), _law("001706", "민법", appendices=[
            _appendix(1, "기간표", "별표1_기간표.hwp", owner="민법")])),
        (utf8("law/민법/별표/별표1_기간표.hwp"), b"UTF8"),
        (b"law/x/\xff\xff.hwp", b"BROKEN"),
        # 맥 압축기: UTF-8 플래그는 있으나 자모 분리(NFD)
        (unicodedata.normalize("NFD", "law/상법/상법.json"), _law("001628", "상법", appendices=[
            _appendix(1, "상호표", "별표1_상호표.hwp", owner="상법")])),
        (unicodedata.normalize("NFD", "law/상법/별표/별표1_상호표.hwp"), b"NFD"),
    ])
    result = read_zip(path)

    assert _codes(result.errors) == ["filename_encoding"]
    linked = {d.path: [f.path for f in d.files] for d in result.accepted()}
    assert linked == {
        "law/형법/형법.json": ["law/형법/별표/별표1_벌금표.hwp"],
        "law/민법/민법.json": ["law/민법/별표/별표1_기간표.hwp"],
        "law/상법/상법.json": ["law/상법/별표/별표1_상호표.hwp"],
    }
    [written] = write_packages(result, tmp_path / "out")
    contents = sorted(base64.b64decode(r["content_b64"]) for r in _records(written["path"])
                      if r["record_type"] == "file")
    assert contents == [b"CP949", b"NFD", b"UTF8"]


def test_source_mismatch_and_unknown(tmp_path):
    """최상위 폴더(law/admrul)가 JSON 의 doc_target·law_id 접두와 모순되면 거부한다."""
    path = _zip(tmp_path, {
        "law/훈령/지침/지침.json": _admrul("2100002", "지침"),                       # admrul 문서가 law/
        "admrul/형법/형법.json": _law("001692", "형법", doc_target="eflaw"),          # 법령이 admrul/
        "admrul/조례/조례.json": _admrul("3000001", "조례", doc_target="ordin"),      # 모르는 target
        "law/학칙/학칙.json": _law("school:77", "학칙"),                               # 학칙 id 가 law/
        "법령/민법/민법.json": _law("001706", "민법"),                                 # 최상위가 law/admrul 아님
        "admrul/학칙/OO대학 학칙/OO대학 학칙.json": _admrul("4000001", "OO대학 학칙", doc_target="school"),
    })
    result = read_zip(path)

    assert _codes(_doc(result, "law/훈령/지침/지침.json").errors) == ["source_mismatch"]
    assert _codes(_doc(result, "admrul/형법/형법.json").errors) == ["source_mismatch"]
    assert _codes(_doc(result, "admrul/조례/조례.json").errors) == ["doc_target_unknown"]
    assert _codes(_doc(result, "law/학칙/학칙.json").errors) == ["source_mismatch"]
    assert _codes(_doc(result, "법령/민법/민법.json").errors) == ["source_unknown"]
    # 학칙공단(school/pi/public)은 수집기처럼 admrul package 로 간다(소비자가 schlpub 컬렉션으로 라우팅).
    assert [d.law_id for d in result.accepted()] == ["4000001"]


def test_mixed_sources_split_into_packages(tmp_path):
    """law·admrul(학칙공단 포함)이 섞인 ZIP → source 별 package 두 개. 기존 package 읽기 함수로 확인."""
    path = _zip(tmp_path, {
        "law/형법/형법.json": _law("001692", "형법"),
        "admrul/지침/지침.json": _admrul("2100003", "지침"),
        "admrul/학칙/OO대학 학칙/OO대학 학칙.json": _admrul("4000001", "OO대학 학칙", doc_target="school"),
    })
    written = write_packages(read_zip(path), tmp_path / "out")

    assert [w["source"] for w in written] == ["law", "admrul"]
    for item in written:
        pkg = Path(item["path"])
        assert pkg.name.startswith(f"{item['source']}-") and pkg.suffix == ".jsonl"
        assert is_package_file(pkg) and peek_package_source(pkg) == item["source"]
        records = _records(pkg)
        header, footer = records[0], records[-1]
        assert header["strategy"] == "base64" and header["mode"] == "delta"
        assert footer["record_count"] == len(records) - 2 and footer["package_id"] == header["package_id"]
    assert sorted(peek_package_law_ids(Path(written[1]["path"]))[1]) == ["2100003", "4000001"]
    assert not list((tmp_path / "out").glob("*.tmp"))


def test_mapper_failure_rejects_whole_law_id(tmp_path):
    """소비자 mapper 가 못 읽는 JSON(법령인데 version_uid 없음)은 거부 — 같은 law_id 의 다른 버전도
    함께 뺀다(일부 버전만 실으면 소비자의 옛 버전 정리가 빠진 버전을 지운다)."""
    broken = _law("001692", "형법", version="2")
    del broken["version_uid"]
    path = _zip(tmp_path, {
        "law/형법/형법.json": _law("001692", "형법", version="1"),
        "law/형법2/형법.json": broken,
    })
    result = read_zip(path)

    assert _codes(_doc(result, "law/형법2/형법.json").errors) == ["mapping_failed"]
    assert _codes(_doc(result, "law/형법/형법.json").errors) == ["group_rejected"]
    assert result.accepted() == []


def test_unsupported_extension_manifest_and_os_junk(tmp_path):
    """지원 밖 확장자는 오류, manifest·맥 메타 파일은 경고로 건너뛴다."""
    path = _zip(tmp_path, {
        "law/형법/형법.json": _law("001692", "형법"),
        "law/형법/별표/표.xlsx": b"X",
        "batch-20261015.manifest.json": {"documents": 1},
        "__MACOSX/law/형법/._형법.json": b"\x00\x05",
        "law/형법/.DS_Store": b"\x00",
    })
    result = read_zip(path)

    assert _codes(result.errors) == ["unsupported_extension"]
    assert _codes(result.warnings) == ["manifest_ignored", "os_metadata_ignored"]
    assert [d.law_id for d in result.accepted()] == ["001692"]


def test_corrupt_zip_fails_whole(tmp_path):
    """ZIP 자체가 깨지면(형식 아님·CRC 불일치) 문서 단위로 나누지 않고 전체 실패."""
    garbage = tmp_path / "garbage.zip"
    garbage.write_bytes(b"this is not a zip")
    with pytest.raises(ZipIntakeError):
        read_zip(garbage)

    damaged = tmp_path / "damaged.zip"
    with zipfile.ZipFile(damaged, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("law/형법/형법.json", _bytes(_law("001692", "형법")))
        zf.writestr("law/형법/별표/별표1.hwp", b"HWPDATA-ORIGINAL")
    damaged.write_bytes(damaged.read_bytes().replace(b"HWPDATA-ORIGINAL", b"HWPDATA-CORRUPTD"))
    with pytest.raises(ZipIntakeError):
        read_zip(damaged)


# ── 변환 결과 → 기존 소비자 왕복 ─────────────────────────────────────────

def test_roundtrip_through_consume_package(tmp_path):
    """변환한 package 를 기존 소비자(consume_package)가 오류 없이 읽는지 — fake store/embedder/parser.

    법령: is_file_only 별표 → file record(전처리→FILE 청크), 본문이 있는 별표의 파일 → 싣지 않음,
    ZIP 에 없는 is_file_only 별표 → pending_attachment. 행정규칙: `[그림]` 본문이미지 → OCR 치환."""
    gif = _make_gif()
    rule = _admrul("2100004", "도해 규칙")
    rule["body"]["articles"][0].update(content="앞.[그림]뒤.", images=[
        {"type": "image", "url": "https://example.invalid/img/1", "filename": "제1조_1.gif"}])
    path = _zip(tmp_path, {
        "law/형법/형법.json": _law("001692", "형법", appendices=[
            _appendix(1, "벌금표", "별표1_벌금표.hwp", owner="형법"),
            _appendix(2, "기준표", "별표2_기준표.hwp", owner="형법", file_only=False),
            _appendix(3, "누락표", "별표3_누락표.hwp", owner="형법")]),
        "law/형법/별표/별표1_벌금표.hwp": b"HWP-FILE-ONLY",
        "law/형법/별표/별표2_기준표.hwp": b"HWP-HAS-TEXT",
        "admrul/도해 규칙/도해 규칙.json": rule,
        "admrul/도해 규칙/본문이미지/제1조_1.gif": gif,
    })
    result = read_zip(path)
    assert result.errors == []
    assert _codes(_doc(result, "law/형법/형법.json").warnings) == ["attachment_missing"]

    law_pkg, admrul_pkg = write_packages(result, tmp_path / "out")
    law_records = _records(law_pkg["path"])
    document = next(r for r in law_records if r["record_type"] == "document")
    assert document["git_path"] == "형법/법률/형법.json" and document["version_uid"] == "001692:1:20260101"
    [file_record] = [r for r in law_records if r["record_type"] == "file"]
    assert base64.b64decode(file_record["content_b64"]) == b"HWP-FILE-ONLY"
    assert file_record["sha256"] == hashlib.sha256(b"HWP-FILE-ONLY").hexdigest()
    assert (file_record["unit_type"], file_record["provision_id"], file_record["file_name"]) == \
        ("APPENDIX", "law:형법#BYL0001", "별표1_벌금표.hwp")
    [pending] = [r for r in law_records if r["record_type"] == "pending_attachment"]
    assert (pending["provision_id"], pending["reason"]) == ("law:형법#BYL0003", "file_not_found_for_base64")

    settings = _consume_settings(tmp_path)
    store = FakeStore()
    totals = consume_package(store, FakeEmbedder(), settings, Path(law_pkg["path"]), doc_parser=FakeDocParser())
    assert totals["errors"] == [] and totals["documents"] == 1
    assert totals["files_preprocessed"] == 1 and totals["pending_attachments"] == 1
    file_chunks = [o for o in store.upserted if o.source_type == "FILE"]
    assert [o.provision_id for o in file_chunks] == ["law:형법#BYL0001"]

    store = FakeStore()
    totals = consume_package(store, FakeEmbedder(), settings, Path(admrul_pkg["path"]), doc_parser=FakeDocParser())
    assert totals["errors"] == [] and totals["images_success"] == 1
    article = next(o for o in store.upserted if o.provision_id == "admrul:도해 규칙#JO0001")
    assert "[그림]" not in article.content and "전처리로 뽑은 파일 텍스트" in article.content


def test_file_transfer_mode_stages_files(tmp_path):
    """file_stage_dir 를 주면 첨부를 그 폴더에 풀고 transfer_name 을 싣는다(소비자 inbox 와 같은 폴더)."""
    path = _zip(tmp_path, {
        "law/형법/형법.json": _law("001692", "형법", appendices=[
            _appendix(1, "벌금표", "별표1_벌금표.hwp", owner="형법")]),
        "law/형법/별표/별표1_벌금표.hwp": b"HWP-STAGED",
    })
    stage = tmp_path / "inbox"
    [written] = write_packages(read_zip(path), tmp_path / "out", file_stage_dir=stage)

    records = _records(written["path"])
    assert records[0]["strategy"] == "file_transfer"
    [file_record] = [r for r in records if r["record_type"] == "file"]
    assert "content_b64" not in file_record
    assert file_record["transfer_name"] == f"{file_record['file_id']}.hwp"
    assert (stage / file_record["transfer_name"]).read_bytes() == b"HWP-STAGED"
    assert file_record["sha256"] == hashlib.sha256(b"HWP-STAGED").hexdigest()

    totals = consume_package(FakeStore(), FakeEmbedder(), _consume_settings(tmp_path, package_file_inbox_dir=stage),
                             Path(written["path"]), doc_parser=FakeDocParser())
    assert totals["errors"] == [] and totals["files_preprocessed"] == 1


def test_file_only_document_attachments_and_unsupported_format(tmp_path):
    """본문 없는 행정규칙의 원문 첨부 — 형식 재수출(hwp+pdf)은 하나만, 전처리기 미지원(.doc)은 pending."""
    rule = _admrul("2100005", "원문 규칙", attachments=[
        {"name": "원문 규칙.hwp", "url": "https://example.invalid/a/1"},
        {"name": "원문 규칙.pdf", "url": "https://example.invalid/a/2"},
        {"name": "참고.doc", "url": "https://example.invalid/a/3"}])
    rule["body"] = {"articles": []}
    path = _zip(tmp_path, {
        "admrul/원문 규칙/원문 규칙.json": rule,
        "admrul/원문 규칙/원문/원문 규칙.hwp": b"HWP",
        "admrul/원문 규칙/원문/원문 규칙.pdf": b"PDF",
        "admrul/원문 규칙/원문/참고.doc": b"DOC",
    })
    result = read_zip(path)
    assert result.errors == []
    assert _codes(result.documents[0].warnings) == ["attachment_unsupported"]

    [written] = write_packages(result, tmp_path / "out")
    body = [r for r in _records(written["path"]) if r["record_type"] in ("file", "pending_attachment")]
    assert [(r["record_type"], r["file_name"], r["unit_type"]) for r in body] == [
        ("file", "원문 규칙.hwp", "DOCUMENT_FILE"),
        ("pending_attachment", "참고.doc", "DOCUMENT_FILE")]
    assert body[1]["reason"] == "unsupported_file_extension:doc"


# ── 첨부 ZIP 펼치기 (LAW-44) ─────────────────────────────────────────────
#
# 법령 사이트가 별표 묶음을 ZIP 으로 주면 _scan_members 가 unsupported_extension 으로 거부해
# 내용물이 통째로 유실된다. read_zip 전에 펼쳐서 평탄한 ZIP 으로 바꾼다.

def _zip_bytes(entries):
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return buf.getvalue()


def test_flatten_returns_none_without_nested_zip(tmp_path):
    """중첩 ZIP 이 없으면 None — 원본을 그대로 쓰고 파일도 만들지 않는다(추가 I/O 0)."""
    src = tmp_path / "plain.zip"
    src.write_bytes(_zip_bytes({"admrul/법령/법령.json": "{}"}))
    out = tmp_path / "work" / "flattened.zip"
    assert flatten_nested_zips(src, out) is None
    assert not out.exists()


def test_flatten_expands_nested_zip_in_place(tmp_path):
    """첨부 ZIP 은 그 자리(같은 디렉터리)에 평탄 추출된다 — 상위 폴더 문서 연결이 유지돼야 한다."""
    inner = _zip_bytes({
        "분류센터 운영지침 (법무부예규).hwp": b"a",
        "하위폴더/분류센터 운영지침 개정(전문).hwp": b"b",
    })
    src = tmp_path / "배달.zip"
    src.write_bytes(_zip_bytes({
        "admrul/분류센터 운영지침/분류센터 운영지침.json": json.dumps({"law_id": "X"}),
        "admrul/분류센터 운영지침/첨부파일/전문.zip": inner,
        "admrul/분류센터 운영지침/별표/별표1.hwp": b"plain",
    }))
    out = flatten_nested_zips(src, tmp_path / "work" / "flattened.zip")
    with zipfile.ZipFile(out) as archive:
        names = set(archive.namelist())
    assert not any(n.endswith(".zip") for n in names)              # 껍데기는 남지 않는다
    assert "admrul/분류센터 운영지침/분류센터 운영지침.json" in names
    assert "admrul/분류센터 운영지침/별표/별표1.hwp" in names        # 무관한 멤버는 그대로
    assert "admrul/분류센터 운영지침/첨부파일/전문__분류센터 운영지침 (법무부예규).hwp" in names
    # ZIP 안의 디렉터리는 평탄화된다 — 접두가 붙어 출처를 알 수 있다
    assert "admrul/분류센터 운영지침/첨부파일/전문__분류센터 운영지침 개정(전문).hwp" in names


def test_flatten_recurses_into_doubly_nested_zip(tmp_path):
    """ZIP 안의 ZIP 안의 파일까지 꺼낸다(운영 실측: 분류센터·분류처우 2건이 2단)."""
    deep = _zip_bytes({"[별지 1] 수형자 다면적 평가서.hwp": b"deep"})
    inner = _zip_bytes({"지침.hwp": b"a", "별지 양식.zip": deep})
    src = tmp_path / "배달.zip"
    src.write_bytes(_zip_bytes({"admrul/지침/첨부파일/전문.zip": inner}))
    out = flatten_nested_zips(src, tmp_path / "work" / "flattened.zip")
    with zipfile.ZipFile(out) as archive:
        names = set(archive.namelist())
    assert "admrul/지침/첨부파일/전문__별지 양식__[별지 1] 수형자 다면적 평가서.hwp" in names


def test_flatten_drops_json_inside_attachment_zip(tmp_path):
    """첨부 ZIP 안의 JSON 은 문서가 아니다 — 넣으면 문서 수 대조(manifest)가 틀어진다."""
    inner = _zip_bytes({"지침.hwp": b"a", "meta.json": b'{"not": "a document"}'})
    src = tmp_path / "배달.zip"
    src.write_bytes(_zip_bytes({"admrul/지침/첨부파일/전문.zip": inner}))
    out = flatten_nested_zips(src, tmp_path / "work" / "flattened.zip")
    with zipfile.ZipFile(out) as archive:
        names = set(archive.namelist())
    assert not any(n.endswith(".json") for n in names)
    assert "admrul/지침/첨부파일/전문__지침.hwp" in names


def test_flatten_keeps_unreadable_nested_zip_as_member(tmp_path):
    """못 푸는 중첩 ZIP 은 원래 멤버로 남긴다 — _scan_members 가 평소대로 오류로 보고하게."""
    src = tmp_path / "배달.zip"
    src.write_bytes(_zip_bytes({"admrul/지침/첨부파일/깨진.zip": b"not a zip at all"}))
    out = flatten_nested_zips(src, tmp_path / "work" / "flattened.zip")
    with zipfile.ZipFile(out) as archive:
        assert "admrul/지침/첨부파일/깨진.zip" in set(archive.namelist())


def test_flatten_avoids_name_collision(tmp_path):
    """서로 다른 첨부 ZIP 이 같은 이름을 담아도 덮어쓰지 않는다(file_id 가 파일명 기반)."""
    payload = _zip_bytes({"전문.hwp": b"x"})
    src = tmp_path / "배달.zip"
    src.write_bytes(_zip_bytes({
        "admrul/지침/첨부파일/a.zip": payload,
        "admrul/지침/첨부파일/b.zip": payload,
    }))
    out = flatten_nested_zips(src, tmp_path / "work" / "flattened.zip")
    with zipfile.ZipFile(out) as archive:
        names = [n for n in archive.namelist() if n.endswith(".hwp")]
    assert len(names) == 2 and len(set(names)) == 2
