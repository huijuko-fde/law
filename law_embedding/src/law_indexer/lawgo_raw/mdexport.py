"""파일 저장 이름 규칙 (수집기 temporal_law/collector/mdexport.py 의 순수 함수 복사본).

lawgo_raw.common 의 enrich_file_meta / attach_ref_ids 가 별표·이미지 저장 이름을 만들 때 쓴다.
출처·이유는 lawgo_raw/common.py 머리말 참조. **네트워크·파일시스템 의존 없음.**
"""

import re

_FILE_PRIORITY = ("pdf", "hwpx", "hwp", "image")


_APPENDIX_DIR_DEFAULT = "별표"


_APPENDIX_DIRS = (
    "별표", "별지", "서식", "부록", "별첨", "별도", "양식", "붙임",
    "기타", "부속서", "부표", "도식", "부도",
)


def doc_kind_of(law_name: str, law_type: str = "") -> str:
    n = (law_name or "").strip()
    if n.endswith("시행규칙"):
        return "시행규칙"
    if n.endswith("시행령"):
        return "시행령"
    return "법률"


def doc_family(law_name: str) -> str:
    """법률+시행령+시행규칙을 묶는 패밀리(=법률 기본명)."""
    n = (law_name or "").strip()
    for suf in (" 시행규칙", " 시행령", "시행규칙", "시행령"):
        if n.endswith(suf):
            return n[: -len(suf)].strip()
    return n


def _safe(name: str) -> str:
    """경로 요소로 안전한 이름.

    Git 데이터 레포는 법제처 원본 파일명을 보존해야 하므로 길이로 자르지 않는다. 금지문자만
    치환하고, 리눅스/NFS 255바이트를 넘는 첨부 파일은 gitexport 가 working tree 에 쓰지 않고
    git blob/index(plumbing) 로 원본 경로 그대로 커밋한다.
    """
    return re.sub(r'[\\/:*?"<>|\n\t]', "_", (name or "").strip()) or "미상"


def _ftype(f: dict) -> str:
    t = (f.get("type") or f.get("kind") or "").lower()
    if t in ("img", "image", "gif", "jpg", "jpeg", "png"):
        return "image"
    if t in ("hwpx", "hwp", "pdf"):
        return t
    nm = (f.get("name") or "").lower()
    for ext in ("hwpx", "hwp", "pdf"):
        if nm.endswith("." + ext):
            return ext
    return "image" if re.search(r"\.(gif|jpe?g|png)$", nm) else "etc"


def pick_file(files: list) -> dict | None:
    """여러 형식 중 우선순위(_FILE_PRIORITY: pdf>hwpx>hwp>img) 1개."""
    if not files:
        return None
    ranked = sorted((f for f in files if f.get("download_status") != "source_unavailable"), key=lambda f: (not f.get("preferred", False),
                    _FILE_PRIORITY.index(_ftype(f)) if _ftype(f) in _FILE_PRIORITY else len(_FILE_PRIORITY)))
    return ranked[0] if ranked else None


def _ext_of(f: dict, default: str = "bin") -> str:
    """파일 확장자 추론(type→이름→url)."""
    t = _ftype(f)
    if t in ("hwpx", "hwp", "pdf"):
        return t
    src = (f.get("name") or "") + " " + (f.get("url") or "")
    m = re.search(r"\.(hwpx|hwp|pdf|gif|jpe?g|png)(?:$|[?&\s])", src.lower())
    if m:
        return "jpg" if m.group(1) == "jpeg" else m.group(1)
    return "gif" if t == "image" else default


def appendix_label(ap: dict) -> str:
    """별표/별지/서식 식별 라벨 = {종류}{번호}[의{가지}]_{제목}. 파일명·참조 공용.
    예: 별표1_과태료의 부과기준 · 서식3_개발행위 사전협의 요청서"""
    kind = (ap.get("kind") or "별표").strip()
    no = (ap.get("no") or "").lstrip("0") or "0"
    br = (ap.get("branch") or "").lstrip("0")
    label = f"{kind}{no}" + (f"의{br}" if br else "")
    title = re.sub(r"\s+", " ", (ap.get("title") or "").strip())
    return f"{label}_{title}" if title else label


def appendix_dir(ap: dict) -> str:
    """부속문서 kind 를 그대로 반영한 저장 폴더명.

    예전에는 별지/서식을 제외한 모든 부속문서를 `별표/`에 넣었지만, provision_id 는
    별첨·별도·부속서·부도 등을 이미 구분한다. relation/reference_id 로 파일 위치를 유추하려면
    저장 폴더도 같은 kind 를 따라야 한다.
    """
    kind = (ap.get("kind") or "").strip()
    return kind if kind in _APPENDIX_DIRS else _APPENDIX_DIR_DEFAULT


def appendix_fname(ap: dict, f: dict) -> str:
    """별표/별지/서식 파일명 = {종류}{번호}[의{가지}]_{제목}.{확장자} — 번호+제목으로 식별·참조 쉽게.
    예: 별표1_과태료의 부과기준.hwp · 서식3_개발행위 사전협의 요청서.pdf"""
    label = appendix_label(ap)
    if ap.get("appendix_duplicate_count"):
        m = re.search(r"(?:[?&]flSeq=|/)(\d{5,})(?:$|[&#?/])", str(f.get("url") or ""))
        suffix = m.group(1) if m else str(ap.get("appendix_instance_id") or "").rsplit(":", 1)[-1]
        if suffix:
            label = f"{label}_flSeq{suffix}" if suffix.isdigit() else f"{label}_{suffix}"
    return _safe(f"{label}.{_ext_of(f)}")


def image_fname(article_no: str, idx: int, im: dict) -> str:
    """본문이미지 파일명 = {조문번호}_{순번}.{확장자} — 어느 조문 이미지인지 식별.
    예: 제3조_1.gif · 제12조의2_2.png"""
    base = _safe((article_no or "본문").replace(" ", ""))
    return _safe(f"{base}_{idx}.{_ext_of(im, 'gif')}")

