"""외부 수집기(투라인) 배달 ZIP → 적재기 정규화 ZIP.

투라인은 law.go.kr 본문 API 응답을 **그대로** 보낸다(2026-10-21 실측): law 는 `{"법령": {...}}`,
admrul 은 `{"AdmRulService": {...}}`. `zip_intake.read_zip` 은 평탄한 영문 키 payload(law_id·
version_uid·body.articles·appendices…)를 기대하므로 그대로는 한 건도 안 들어간다.

변환은 lawgo_raw(수집기 temporal_law 의 순수 함수 복사본)로 하고, 조립은 수집기 build_payload /
build_admrul_payload 의 **관계 추출을 뺀** 부분만 옮겼다 — 관계(인용·위임)는 현행 화면 크롤링이
필요한데 배달 응답엔 없다. 수집기 출력과의 동일성은 tests/test_lawgo_raw_equivalence.py 가 고정한다.

파일명도 맞춘다. 투라인은 law.go.kr 원본 이름 `[별표 1] 검사의 봉급표(검사의 보수에 관한 법률).hwp`
로 보내는데 적재기는 enrich_file_meta 가 심은 `별표1_검사의 봉급표.pdf` 를 찾는다. 같은 별표를 여러
형식(hwp/pdf/gif)으로 보내는 것도 하나만 남긴다 — appendices[].filename 은 별표당 하나다.

manifest 는 **새로 쓴다**. 원본 manifest 의 sha256/bytes/files 는 원본 ZIP 기준이라 변환본에 그대로
붙이면 zip_consume 이 manifest_mismatch 로 거부한다. docs(id·mst·dir)만 가져오고 나머지는 재계산.

변환은 결정적이고(같은 ZIP → 같은 바이트) 게시는 원자적이다(manifest 먼저, ZIP 은 .part → rename).
한 ZIP 의 실패는 그 ZIP 만 failed 로 격리하고 아무것도 게시하지 않는다. convert_one 참고.

사용:  python -m law_indexer.inbox_convert <inbox_dir> <outbox_dir> [--done DIR] [--failed DIR]
"""
import argparse
import hashlib
import json
import os
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Optional

from .lawgo_raw import admrul as _adm
from .lawgo_raw import common as _c
from .lawgo_raw import law as _law

# 같은 별표의 여러 형식 중 남길 순서. 단 appendices[].filename 이 선언한 확장자가 있으면 그것이 먼저다 —
# 이름이 그대로 맞아야 적재기가 연결한다.
FORMAT_PRIORITY = (".pdf", ".hwpx", ".hwp", ".docx")

# law.go.kr 원본 파일명: [별표 1] 제목(법령명).hwp  /  [별지 2의3] 제목.pdf
_ORIG = re.compile(r"^\[(?P<kind>[가-힣]+)\s*(?P<no>\d+)(?:의(?P<branch>\d+))?\]\s*"
                   r"(?P<title>.+?)\s*(?:\([^()]*\))?\.(?P<ext>[A-Za-z0-9]+)$")
# 적재기가 기대하는 이름: 별표1_제목.pdf  /  별지2의3_제목.pdf
_WANT = re.compile(r"^(?P<kind>[가-힣]+)(?P<no>\d+)(?:의(?P<branch>\d+))?_")


class ConvertError(RuntimeError):
    pass


def _stats(articles, addenda, appendices, attachments=None) -> dict:
    """관계 추출 없이 조립했으므로 관계 수치는 전부 0 — 수집기 relation_stats 와 같은 모양."""
    d = {"relations_total": 0, "by_category": {}, "by_relation_type": {},
         "articles_total": len(articles), "articles_with_relations": 0, "unmatched_relations": 0}
    if attachments is None:
        d.update({"phantom_self_refs": 0, "phantom_self_ref_samples": [], "ordinance_groups": 0,
                  "ordinance_docs_total": 0, "addenda_count": len(addenda), "appendices_count": len(appendices),
                  "pop_miss": 0, "pop_miss_by_reason": {}, "pop_skipped_addendum": 0})
    else:
        d.update({"ordinance_groups": 0, "ordinance_docs_total": 0, "addenda_count": len(addenda),
                  "appendices_count": len(appendices), "attachments_count": len(attachments)})
    return d


def assemble_law(body_json: dict, law_id: str, mst: str, law_name: str, at: str = "") -> dict:
    """수집기 build_payload 의 관계 없는 조립. mst 는 본문 응답에 없어 manifest 로 받는다.

    at 은 fetched_at/synced_at 에 쓸 시각 — 비우면 지금. 변환은 같은 입력에 같은 바이트를
    내야 하므로(convert_zip 참고) 배달 ZIP 에서 뽑은 시각을 넘긴다."""
    fv = _c.first_value_recursive
    articles = _c.build_articles(body_json)
    addenda = _c.build_addenda(body_json)
    appendices = _c.build_appendices(body_json, viewer=("lsBylInfoPLinkR.do", "lsiSeq", str(mst)))
    basic_info = fv(body_json, ["기본정보"])
    if not isinstance(basic_info, dict):
        basic_info = {}
    law_id = law_id or fv(body_json, ["법령ID", "ID"]) or ""
    if not mst:
        # version_uid = law_id:mst:시행일 — mst 가 비면 기존 적재분과 형식이 어긋나 같은 법령이
        # 두 버전으로 들어간다. 본문 응답엔 없는 값이라 manifest 로 받기로 했다(LAW-39).
        raise ConvertError(f"law 문서의 mst 가 manifest 에 없습니다: {law_name}")
    if not articles:
        raise ConvertError(f"본문 조회 실패(조문 0건): {law_name} (mst={mst})")
    law_type = fv(body_json, ["법령구분명", "법령구분"]) or ""
    if not law_type:
        bt = fv(body_json, ["법종구분"])
        if isinstance(bt, dict):
            law_type = _c.normalize_text(bt.get("content", "")) or ""
    promulgation_no = fv(body_json, ["공포번호"]) or ""
    promulgation_date = fv(body_json, ["공포일자"]) or ""
    enforcement_date = fv(body_json, ["시행일자"]) or ""
    revision_type = fv(body_json, ["제개정구분명", "제개정구분"]) or ""
    law_abbr = basic_info.get("법령명약칭") or ""
    amendment_text = _c.extract_images(_c.lines_text(fv(body_json, ["개정문내용"])))[0]
    revision_reason = _c.extract_images(_c.lines_text(fv(body_json, ["제개정이유내용"])))[0]
    for art in articles:
        art["relations"] = []
        _law._fill_line_text_from_content(art)
    _law._drop_phantom_self_refs(articles)
    payload = {
        "law_id": str(law_id), "mst": str(mst),
        "version_uid": f"{law_id}:{mst}:{enforcement_date}",
        "law_name": law_name, "law_abbr": str(law_abbr),
        "name_aliases": _law._name_aliases(law_name, law_abbr, basic_info),
        "law_type": law_type,
        "promulgation_no": str(promulgation_no), "promulgation_date": str(promulgation_date),
        "enforcement_date": str(enforcement_date), "revision_date": str(promulgation_date),
        "revision_type": str(revision_type),
        "is_current": True, "is_future": False,
        "basic_info": basic_info, "amendment_text": amendment_text, "revision_reason": revision_reason,
        "body": {"format": "articles", "article_count": len(articles), "articles": articles},
        "addenda": addenda, "appendices": appendices, "ordinance_delegations": [],
        "relation_mode": "no_crawl",
        "relation_stats": _stats(articles, addenda, appendices),
        "source": {"source_url": _c.build_ref_url("법령", law_name), "fetched_at": at or _c.now_kst()},
        "sync": {"sync_reason": "INITIAL_LOAD", "synced_at": at or _c.now_kst()},
    }
    _c.attach_ref_ids(payload)                   # 조/별표/부칙 provision_id — 수집기와 같은 순서
    _law._enrich_file_meta(appendices, articles) # 별표/이미지에 filename·local_path
    return payload


def assemble_admrul(body_json: dict, aid: str, seq: str, name: str, target: str = "admrul",
                    at: str = "") -> dict:
    """수집기 build_admrul_payload 의 관계 없는 조립. at 은 assemble_law 와 같다."""
    fv = _c.first_value_recursive
    message = body_json.get("Law") if isinstance(body_json, dict) else None
    if isinstance(message, str) and "일치하는 행정규칙이 없습니다" in message:
        raise ConvertError(f"행정규칙 버전이 존재하지 않음: {name} (seq={seq})")
    articles = _adm.build_admrul_articles(body_json)
    gi = fv(body_json, ["행정규칙기본정보"])
    if not isinstance(gi, dict):
        gi = {}
    if aid and gi.get("행정규칙ID") and str(gi["행정규칙ID"]) != str(aid):
        raise ConvertError(f"행정규칙 본문 ID 불일치: requested={aid}, returned={gi['행정규칙ID']}")
    if seq and gi.get("행정규칙일련번호") and str(gi["행정규칙일련번호"]) != str(seq):
        raise ConvertError(f"행정규칙 본문 버전 불일치: requested={seq}, returned={gi['행정규칙일련번호']}")
    seq = seq or str(gi.get("행정규칙일련번호") or "")
    aid = aid or str(gi.get("행정규칙ID") or "")
    canonical = _c.normalize_text(gi.get("행정규칙명") or name)
    appendices = _c.build_appendices(body_json, viewer=("admRulBylInfoPLinkR.do", "admRulSeq", str(seq)))
    attachments = _adm.build_admrul_attachments(body_json)
    if not articles and not appendices and not attachments:
        raise ConvertError(f"행정규칙 본문 조회 실패(조문·별표·첨부 모두 0건): {name} (seq={seq})")
    url_cat = "학칙공단" if target in ("school", "pi", "public") else "행정규칙"
    for art in articles:
        art["relations"] = []
    addenda = _adm.build_admrul_addenda(body_json)
    amendment_text = _c.extract_images(_c.lines_text(fv(body_json, ["개정문내용"])))[0]
    revision_reason = _c.extract_images(_c.lines_text(fv(body_json, ["제개정이유내용"])))[0]
    law_type = _c.normalize_text(gi.get("행정규칙종류") or "행정규칙")
    enforcement_date = str(gi.get("시행일자") or "")
    promulgation_date = str(gi.get("발령일자") or "")
    promulgation_no = str(gi.get("발령번호") or "")
    payload = {
        "law_id": str(aid or seq), "mst": str(seq), "law_name": canonical, "law_abbr": "",
        "law_type": law_type, "doc_target": target, "doc_kind": _adm.DOC_KIND.get(target, "행정규칙"),
        "adm_uid": f"{target}:{aid or seq}",
        "promulgation_no": promulgation_no, "promulgation_date": promulgation_date,
        "enforcement_date": enforcement_date, "revision_date": promulgation_date,
        "revision_type": _c.normalize_text(gi.get("제개정구분명") or ""),
        "is_current": True, "basic_info": gi,
        "amendment_text": amendment_text, "revision_reason": revision_reason,
        "body": {"format": "articles", "article_count": len(articles), "articles": articles},
        "addenda": addenda, "appendices": appendices, "attachments": attachments, "ordinance_delegations": [],
        "relation_mode": "no_crawl",
        "relation_stats": _stats(articles, addenda, appendices, attachments),
        "source": {"source_url": _c.build_ref_url(url_cat, canonical), "fetched_at": at or _c.now_kst()},
        "sync": {"sync_reason": "INITIAL_LOAD", "synced_at": at or _c.now_kst()},
    }
    _c.attach_ref_ids(payload)                   # 수집기와 같은 순서
    _c.enrich_file_meta(payload)                 # 별표·첨부·본문이미지에 filename/file_dir/local_path
    return payload


def convert_payload(body_json: dict, doc: dict, source: str, name: str, at: str = "") -> dict:
    """원본 body_json → 적재기 payload. doc 은 manifest docs[] 의 {id, mst, dir}."""
    if source == "law":
        return assemble_law(body_json, doc.get("id") or "", doc.get("mst") or "", name, at=at)
    return assemble_admrul(body_json, doc.get("id") or "", doc.get("mst") or "", name, at=at)


def _member_names(archive: zipfile.ZipFile) -> list:
    """CP949 로 압축된 한글 이름도 되돌린다(UTF-8 플래그 없는 멤버)."""
    out = []
    for info in archive.infolist():
        if info.is_dir():
            continue
        name = info.filename
        if not info.flag_bits & 0x800:
            try:
                name = name.encode("cp437").decode("cp949")
            except UnicodeError:
                pass
        out.append((info.filename, name))
    return out


def plan_files(payload: dict, members: list) -> tuple:
    """같은 문서 폴더의 첨부 멤버 → (rename: 원본경로→새이름, drop: 버릴 경로).

    원문 첨부(attachments[].name)는 이름이 이미 맞으므로 그대로. 별표는 (종류·번호·가지)로
    appendices[].filename 에 맞추고, 같은 별표의 여러 형식은 하나만 남긴다."""
    want = {}
    for ap in payload.get("appendices") or []:
        m = _WANT.match(ap.get("filename") or "")
        if m:
            want[(m["kind"], int(m["no"]), int(m["branch"]) if m["branch"] else None)] = ap["filename"]
    declared = {a.get("name") for a in (payload.get("attachments") or []) if a.get("name")}
    groups = {}
    for path in members:
        base = path.rsplit("/", 1)[-1]
        if base in declared:
            continue
        m = _ORIG.match(base)
        if not m:
            continue
        key = (m["kind"], int(m["no"]), int(m["branch"]) if m["branch"] else None)
        if key in want:
            groups.setdefault(key, []).append((path, "." + m["ext"].lower()))
    rename, drop = {}, []
    for key, items in groups.items():
        target = want[key]
        declared_ext = "." + target.rsplit(".", 1)[-1].lower()

        def rank(item):
            ext = item[1]
            if ext == declared_ext:
                return (0, 0)
            return (1, FORMAT_PRIORITY.index(ext) if ext in FORMAT_PRIORITY else len(FORMAT_PRIORITY))

        items.sort(key=rank)
        path, ext = items[0]
        if ext not in FORMAT_PRIORITY:              # gif 뿐인 별표 — 전처리기가 못 읽는다
            drop.extend(p for p, _ in items)
            continue
        rename[path] = target.rsplit(".", 1)[0] + ext
        drop.extend(p for p, _ in items[1:])
    return rename, drop


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_manifest(zip_path: Path, docs: list, out: Path, *, file_name: str = "",
                   source: Optional[dict] = None) -> dict:
    """변환본 기준으로 manifest 를 다시 쓴다(docs 는 원본 것을 그대로).

    file_name 은 manifest 에 적을 ZIP 이름 — 임시 이름(.part)으로 쓰는 중이면 최종 이름을 준다.
    source 는 원본 배달 ZIP 의 {file, sha256} — 같은 원본을 다시 변환하지 않는 판단(convert_one)에 쓴다."""
    files = []
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if not info.is_dir():
                files.append({"path": info.filename, "sha256": hashlib.sha256(z.read(info)).hexdigest(),
                              "bytes": info.file_size})
    mf = {"file": file_name or zip_path.name, "sha256": _sha256(zip_path), "bytes": zip_path.stat().st_size,
          "documents": len(docs), "docs": docs, "files": files}
    if source:
        mf["source"] = source
    out.write_text(json.dumps(mf, ensure_ascii=False, indent=1), encoding="utf-8")
    return mf


# 변환은 **결정적**이어야 한다 — 같은 배달 ZIP 이면 같은 바이트가 나와야 consume-zip 의 SHA-256
# 중복 방지가 재변환본을 알아본다. 비결정 요소 둘을 입력에 묶는다: payload 의 fetched_at/synced_at
# 은 배달 ZIP 멤버의 가장 늦은 타임스탬프로, 출력 멤버 타임스탬프는 원본 멤버 것으로.
def _zip_clock(infos) -> str:
    latest = max((i.date_time for i in infos if not i.is_dir()), default=(1980, 1, 1, 0, 0, 0))
    return datetime(*latest, tzinfo=_c.KST).isoformat(timespec="seconds")


def _entry(name: str, like: zipfile.ZipInfo) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=like.date_time)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    return info


def convert_zip(zip_path: Path, manifest: dict, out_zip: Path) -> dict:
    """배달 ZIP 1건 변환. 반환: 보고 dict(문서·rename·drop·오류). 같은 입력이면 같은 바이트를 쓴다."""
    docs_by_dir = {d["dir"]: d for d in manifest.get("docs") or []}
    report = {"zip": zip_path.name, "documents": 0, "renamed": 0, "dropped": 0, "kept": 0,
              "errors": [], "payloads": []}
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as src, zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as dst:
        members = _member_names(src)
        by_name = {name: raw for raw, name in members}
        at = _zip_clock(src.infolist())
        for jpath in [n for _, n in members if n.endswith(".json")]:
            folder = jpath.rsplit("/", 1)[0] if "/" in jpath else ""
            doc = docs_by_dir.get(folder)
            if doc is None:
                report["errors"].append({"path": jpath, "error": "manifest docs[] 에 이 문서 폴더가 없습니다"})
                continue
            source = folder.split("/", 1)[0]
            if source not in ("law", "admrul"):
                report["errors"].append({"path": jpath, "error": f"최상위 폴더가 law/admrul 이 아닙니다: {source}"})
                continue
            try:
                body_json = json.loads(src.read(by_name[jpath]).decode("utf-8-sig"))
                payload = convert_payload(body_json, doc, source, PurePosixPath(jpath).stem, at=at)
            except Exception as exc:                 # 한 문서가 깨져도 나머지는 변환해 보고에 모은다
                report["errors"].append({"path": jpath, "error": f"{type(exc).__name__}: {exc}"})
                continue
            dst.writestr(_entry(jpath, src.getinfo(by_name[jpath])), json.dumps(payload, ensure_ascii=False))
            report["documents"] += 1
            report["payloads"].append({
                "path": jpath, "law_id": payload.get("law_id"), "mst": payload.get("mst"),
                "version_uid": payload.get("version_uid"),
                "articles": len((payload.get("body") or {}).get("articles") or []),
                "appendices": len(payload.get("appendices") or []),
                "attachments": len(payload.get("attachments") or [])})
            siblings = [n for _, n in members if n.startswith(folder + "/") and not n.endswith(".json")]
            rename, drop = plan_files(payload, siblings)
            dropped = set(drop)
            for path in siblings:
                if path in dropped:
                    report["dropped"] += 1
                    continue
                new = rename.get(path)
                target = (path.rsplit("/", 1)[0] + "/" + new) if new else path
                dst.writestr(_entry(target, src.getinfo(by_name[path])), src.read(by_name[path]))
                report["renamed" if new else "kept"] += 1
    return report


PENDING_MANIFEST, CONVERTED, SKIPPED, FAILED = "pending_manifest", "converted", "skipped", "failed"


def _pair(zp: Path) -> Path:
    return zp.with_name(zp.stem + ".manifest.json")


def _move_pair(zp: Path, dest: Optional[Path]) -> None:
    if dest is None:
        return
    dest.mkdir(parents=True, exist_ok=True)
    for p in (zp, _pair(zp)):
        if p.exists():
            os.replace(p, dest / p.name)


def convert_one(zp: Path, outbox: Path, *, done_dir: Optional[Path] = None,
                failed_dir: Optional[Path] = None) -> dict:
    """배달 ZIP 1건: 변환 → 변환본·manifest 원자적 게시 → 원본 쌍 이동. 반환: 보고 dict(status 포함).

    status
      converted        게시함. 원본 쌍은 done_dir 로(주어졌을 때).
      skipped          같은 원본(sha256)을 이미 변환해 둠 — 다시 쓰지 않는다. 원본 쌍은 done_dir 로.
      failed           변환 오류. **아무것도 게시하지 않는다.** 원본 쌍과 보고는 failed_dir 로.
      pending_manifest manifest 가 아직 없음 — 제자리(수신기가 manifest 를 먼저 두므로 자동 경로엔 안 생긴다).

    게시 순서는 consume-zip 의 수신 규약을 따른다 — `.part` 에 다 쓰고, manifest 를 최종 이름으로 먼저,
    그 다음 ZIP 을 rename. 문서 하나라도 실패하면 게시하지 않는다: 일부만 담긴 ZIP 은 manifest 문서 수와
    어긋나 consume-zip 이 통째로 거부하므로(manifest_mismatch) 반쯤 게시해 봐야 의미가 없다."""
    mf_path = _pair(zp)
    if not mf_path.exists():
        return {"zip": zp.name, "status": PENDING_MANIFEST, "errors": []}
    final_zip, final_mf, part = outbox / zp.name, outbox / mf_path.name, outbox / f".{zp.name}.part"
    report = {"zip": zp.name, "status": FAILED, "errors": []}
    try:
        source_sha = _sha256(zp)
        manifest = json.loads(mf_path.read_text(encoding="utf-8"))
        if final_zip.exists() and final_mf.exists():
            prev = (json.loads(final_mf.read_text(encoding="utf-8")).get("source") or {}).get("sha256")
            if prev == source_sha:
                report["status"] = SKIPPED
                _move_pair(zp, done_dir)
                return report
        outbox.mkdir(parents=True, exist_ok=True)
        part.unlink(missing_ok=True)
        report.update(convert_zip(zp, manifest, part))
        if report["errors"]:
            raise ConvertError(f"문서 {len(report['errors'])}건 변환 실패")
        write_manifest(part, manifest.get("docs") or [], final_mf, file_name=zp.name,
                       source={"file": zp.name, "sha256": source_sha})
        os.replace(part, final_zip)
        report["status"] = CONVERTED
        _move_pair(zp, done_dir)
        return report
    except ConvertError:
        pass                                         # 문서 단위 오류는 이미 report["errors"] 에 있다
    except Exception as exc:                         # BadZipFile · JSONDecodeError · OSError — 이 ZIP 만 실패
        report["errors"].append({"error": f"{type(exc).__name__}: {exc}"})
    part.unlink(missing_ok=True)
    if failed_dir is not None:
        failed_dir.mkdir(parents=True, exist_ok=True)
        (failed_dir / f"{zp.stem}.convert-report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
        _move_pair(zp, failed_dir)
    return report


def convert_inbox(inbox: Path, outbox: Path, *, done_dir: Optional[Path] = None,
                  failed_dir: Optional[Path] = None) -> list:
    """inbox 의 `*.zip`(+ 같은 이름 .manifest.json)을 파일명 순서로 변환한다. 한 ZIP 의 실패가 다음 ZIP 을
    막지 않는다. done_dir/failed_dir 를 주면 원본 쌍을 그리로 옮기고, 없으면 제자리에 둔다(수동 실행)."""
    return [convert_one(zp, outbox, done_dir=done_dir, failed_dir=failed_dir)
            for zp in sorted(inbox.glob("*.zip")) if not zp.name.startswith(".")]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="투라인 배달 ZIP → 적재기 정규화 ZIP")
    ap.add_argument("inbox", type=Path)
    ap.add_argument("outbox", type=Path)
    ap.add_argument("--done", type=Path, default=None, help="변환한 원본 쌍을 옮길 폴더(없으면 제자리)")
    ap.add_argument("--failed", type=Path, default=None, help="실패한 원본 쌍과 보고를 옮길 폴더(없으면 제자리)")
    args = ap.parse_args(argv)
    reports = convert_inbox(args.inbox, args.outbox, done_dir=args.done, failed_dir=args.failed)
    print(json.dumps(reports, ensure_ascii=False, indent=1))
    return 1 if any(r.get("status") == FAILED for r in reports) else 0


if __name__ == "__main__":
    sys.exit(main())
