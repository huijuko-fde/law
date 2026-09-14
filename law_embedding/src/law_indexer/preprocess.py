"""첨부파일 전처리 — Doc Parser(첨부용 전처리기) HTTP 클라이언트 + 로컬 파일 경로 역산.

이 모듈은 원래 흐름만 보여주는 목업이었다(파일을 읽지 않고 자리표시 청크만 생성). 이제 실제
Doc Parser API(GET /healthcheck, POST /run)를 호출한다. `preprocess_file`이 원래 지정된
교체 지점이라 시그니처를 유지했고, 나머지(파일 탐색 → 매핑 → 임베딩 → 적재)는 그대로 재사용된다.

로컬 첨부파일 경로는 JSON에 필드로 없다(실제 샘플 전수 확인: appendices[].files[].name 은
항상 빈 문자열이고, payload_desc.md 가 말하는 filename 필드도 export된 JSON에는 한 번도
채워지지 않는다 — DB ERD 전용 컬럼). 대신 temporal_law/collector/mdexport.py 가 실제로 파일을
쓸 때 쓰는 명명 규칙(law_dir/appendix_fname)을 그대로 재구현해 역산한다. law_embedding 은
temporal_law 코드에 의존하지 않는다(경계 유지, CLAUDE.md §1). 수집기 쪽 명명 규칙이 바뀌면
이 부분도 같이 맞춰야 한다.
"""
import hashlib
import json
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .config import Settings

# 수집기 미러에서 첨부파일이 놓이는 폴더 이름. temporal_law 의 appendix_dir 규칙과 맞춰야
# 파일-only 부속문서를 재수집 후에도 그대로 찾아 전처리할 수 있다.
APPENDIX_DIRS = {
    "별표", "별지", "서식", "부록", "별첨", "별도", "양식", "붙임",
    "기타", "부속서", "부표", "도식", "부도",
}
ATTACHMENT_DIRS = {*APPENDIX_DIRS, "첨부파일", "원문", "본문이미지"}
# 전처리 대상 확장자
ATTACHMENT_SUFFIXES = {".hwp", ".hwpx", ".pdf", ".doc", ".docx", ".png", ".jpg", ".jpeg", ".gif", ".zip"}

# 데이터 루트 폴더(여기까지 올라가면 상위 탐색 중단)
#   레포가 3개로 갈렸다(LAW/ADMRUL/SCHLPUBRUL). 옛 이름도 남겨 기존 미러가 그대로 동작하게 한다.
_DATA_ROOTS = {"LAW", "ADMRUL", "SCHLPUBRUL", "law_data", "admrul_data"}


# ── 기존 index-files(파일 크롤링) 경로용 — 그대로 재사용 ──────────────────

def find_parent_law(file_path: Path) -> Optional[Dict[str, Any]]:
    """첨부파일의 상위 문서 JSON 을 찾아 돌려준다.

    미러 구조상 파일은 ``{문서명}/{종}/{별표|별지|…}/파일`` 이므로, 조상 폴더를
    올라가며 첫 번째 문서 JSON(``_manifest.json`` 제외)을 찾는다. 못 찾으면 None.
    """
    for folder in file_path.parents:
        candidates = [p for p in sorted(folder.glob("*.json")) if p.name != "_manifest.json"]
        if candidates:
            try:
                return json.loads(candidates[0].read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return None
        if folder.name in _DATA_ROOTS:
            break
    return None


def match_appendix_provision_id(parent: Dict[str, Any], file_path: Path) -> Optional[str]:
    """상위 문서의 별표 목록에서 이 파일이 속한 별표 provision_id 를 best-effort 로 찾는다.

    로컬 파일명이 원본 파일명과 다를 수 있어 매칭이 안 될 수 있고, 그러면 None
    (= 일반 첨부로 적재)을 돌려준다. 정확한 연결은 실제 전처리기의 책임이다.
    """
    name = file_path.name
    for appendix in parent.get("appendices") or []:
        names = {appendix.get("filename")}
        for entry in appendix.get("files") or []:
            names.add(entry.get("name"))
        if name in names:
            return appendix.get("provision_id")
    return None


# ── 로컬 파일명 역산 (temporal_law/collector/mdexport.py 저장 규칙 재구현) ──

_SAFE_RE = re.compile(r'[\\/:*?"<>|\n\t]')
_FILE_PRIORITY = ("hwpx", "hwp", "pdf", "image")

# 리눅스/NFS 경로요소 한계는 255바이트. git 에는 수집기가 쓴 원본 이름(금지문자만 치환)이
# 들어있지만, 그 이름이 255바이트를 넘으면 리눅스에선 checkout 이 실패한다 → git_sync 가 그런
# 파일만 아래 규칙으로 safe 이름을 지어 materialize 하고, 색인도 같은 규칙으로 그 파일을 찾는다.
# 임계값 255 는 fs 한계와 일치시킨 값 — 255바이트 이내 이름은 checkout 이 성공해 원본 그대로
# 디스크에 있으므로 손대지 않아야 색인이 찾는다(초과분만 절단).
_MAX_NAME_BYTES = 255
_TRUNC_BUDGET = 240                                          # 잘랐을 때 결과 예산(255 아래 여유)


def _byte_trunc_name(name: str) -> str:
    """UTF-8 바이트가 예산을 넘으면 확장자를 보존한 채 잘라 결정적 해시 꼬리(~xxxxxxxx)를 붙인다.
    같은 원본이면 항상 같은 결과(멱등) — 재동기화해도 로컬 경로가 흔들리지 않는다."""
    stem, dot, ext = name.rpartition(".")
    suffix = "." + ext if (dot and 1 <= len(ext) <= 5 and re.fullmatch(r"[A-Za-z0-9]+", ext)) else ""
    if not suffix:
        stem = name
    tag = "~" + hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    room = _TRUNC_BUDGET - len((tag + suffix).encode("utf-8"))
    trunc = stem.encode("utf-8")[: max(room, 0)].decode("utf-8", "ignore")
    return (trunc + tag + suffix) or "미상"


def _safe_name(name: str) -> str:
    """git 원본 이름 → 리눅스 로컬용 경로요소.

    수집기 mdexport._safe 와 동일하게 금지문자만 치환한다. 255바이트 이내면 그대로 두어 checkout
    성공분과 정확히 일치시키고, 초과할 때만 확장자를 보존한 safe 이름으로 materialize 한다.
    """
    cleaned = _SAFE_RE.sub("_", (name or "").strip())
    if len(cleaned.encode("utf-8")) <= _MAX_NAME_BYTES:
        return cleaned or "미상"
    return _byte_trunc_name(cleaned)                          # 리눅스 경로요소 한계 초과 → 로컬 사본만 절단


def _doc_kind_of(law_name: str) -> str:
    n = (law_name or "").strip()
    if n.endswith("시행규칙"):
        return "시행규칙"
    if n.endswith("시행령"):
        return "시행령"
    return "법률"


def _doc_family(law_name: str) -> str:
    n = (law_name or "").strip()
    for suf in (" 시행규칙", " 시행령", "시행규칙", "시행령"):
        if n.endswith(suf):
            return n[: -len(suf)].strip()
    return n


def _schlpub_split_candidates(base: Path, data: Dict[str, Any]) -> List[Path]:
    """이름 충돌로 `{문서명}/{suffix}/` 로 갈라진 학칙공단·행정규칙 문서의 실제 폴더를 찾는다.

    suffix 는 `admrul`·`public` 같은 target 일 수도, 같은 target 안 동명충돌이면 `admrul_71712`
    처럼 **문서 ID** 가 붙기도 한다(수집기 `mdexport.schlpub_target_dir`). payload 에서 그 값을
    되짚을 수 없다(gitexport 가 `_schlpub_id_suffix` 를 JSON 에 안 남긴다) — 그래서 **추측하지
    않고**, 하위 폴더의 문서 JSON 을 실제로 열어 `law_id`(+`doc_target`)가 같은 것만 고른다.
    같은 이름 다른 문서의 파일을 잘못 물지 않기 위한 조건이다."""
    if not base.is_dir():
        return []
    want_id = str(data.get("law_id") or "").strip()
    want_target = str(data.get("doc_target") or "").strip()
    if not want_id:
        return []
    fname = _safe_name(str(data.get("law_name") or "")) + ".json"
    found = []
    for sub in sorted(base.iterdir()):
        if not sub.is_dir() or sub.name in ATTACHMENT_DIRS:
            continue
        doc_json = sub / fname
        if not doc_json.is_file():
            continue
        try:
            meta = json.loads(doc_json.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if str(meta.get("law_id") or "").strip() != want_id:
            continue
        if want_target and str(meta.get("doc_target") or "").strip() != want_target:
            continue
        found.append(sub)
    return found


def document_dir_candidates(repo_root: Path, data: Dict[str, Any],
                            doc_dir: Optional[Path] = None) -> List[Path]:
    """이 문서가 저장돼 있을 수 있는 디렉터리 후보들(우선순위 순).

    ★ **`doc_dir` 을 주면 역산하지 않고 그것만 쓴다.** 디스크의 문서 JSON 을 순회해 색인할 때는
    문서 폴더가 곧 `그 JSON 의 부모 폴더`라 역산 자체가 불필요하고, 역산은 저장 레이아웃이
    바뀔 때마다 조용히 100% 실패한다(실제로 그랬다 — 레포를 LAW/ADMRUL/SCHLPUBRUL 로 나누고
    문서종 래퍼 폴더를 없앤 뒤, is_file_only 별표와 본문이미지가 전부 미해결이 됐다).
    아래 역산 후보는 **payload 만 있고 원본 경로를 모르는 증분(package) 경로 전용 폴백**이다.

    현재 저장 레이아웃(수집기 `mdexport.law_dir`):
      · 법령(LAW)             = `{패밀리}/{법률|시행령|시행규칙}/`
      · 행정규칙류(ADMRUL·SCHLPUBRUL) = `{문서명}/` — 이름 충돌 시 `{문서명}/{suffix}/`
    옛 레이아웃(`{doc_kind}/{문서명}`·`학칙공단/{문서명}`)은 기존 미러 호환으로 뒤에만 둔다."""
    if doc_dir is not None:
        return [Path(doc_dir)]
    if data.get("doc_target") in ("admrul", "school", "pi", "public"):
        doc_kind = data.get("doc_kind") or "행정규칙"
        name = _safe_name(data.get("law_name", ""))
        base = Path(repo_root) / name
        cands = [base, *_schlpub_split_candidates(base, data)]
        cands.append(Path(repo_root) / "학칙공단" / name)          # 레거시
        cands.append(Path(repo_root) / doc_kind / name)             # 레거시
        return cands
    fam = _safe_name(_doc_family(data.get("law_name", "")))
    kind = _doc_kind_of(data.get("law_name", ""))
    return [Path(repo_root) / fam / kind]


def document_dir(repo_root: Path, data: Dict[str, Any], doc_dir: Optional[Path] = None) -> Path:
    """이 문서가 저장된 디렉터리(document_dir_candidates() 의 대표 후보 1개)."""
    return document_dir_candidates(repo_root, data, doc_dir)[0]


def _ftype(f: Dict[str, Any]) -> str:
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


def _pick_file(files: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """여러 형식 중 우선순위(hwpx>hwp>pdf>img) 1개."""
    if not files:
        return None
    ranked = sorted(files, key=lambda f: _FILE_PRIORITY.index(_ftype(f))
                    if _ftype(f) in _FILE_PRIORITY else len(_FILE_PRIORITY))
    return ranked[0]


def _ext_of(f: Dict[str, Any], default: str = "bin") -> str:
    t = _ftype(f)
    if t in ("hwpx", "hwp", "pdf"):
        return t
    src = (f.get("name") or "") + " " + (f.get("url") or "")
    m = re.search(r"\.(hwpx|hwp|pdf|gif|jpe?g|png)(?:$|[?&\s])", src.lower())
    if m:
        return "jpg" if m.group(1) == "jpeg" else m.group(1)
    return "gif" if t == "image" else default


def _appendix_label(ap: Dict[str, Any]) -> str:
    kind = (ap.get("kind") or "별표").strip()
    no = (ap.get("no") or "").lstrip("0") or "0"
    br = (ap.get("branch") or "").lstrip("0")
    label = f"{kind}{no}" + (f"의{br}" if br else "")
    title = re.sub(r"\s+", " ", (ap.get("title") or "").strip())
    return f"{label}_{title}" if title else label


def _appendix_subdir(kind: Optional[str]) -> str:
    """부속문서 kind 를 수집기 저장 폴더와 같은 이름으로 변환한다."""
    k = (kind or "").strip()
    return k if k in APPENDIX_DIRS else "별표"


def appendix_local_path(repo_root: Path, data: Dict[str, Any], appendix: Dict[str, Any],
                        doc_dir: Optional[Path] = None) -> Optional[Path]:
    """부속문서 항목의 실제 로컬 파일 경로를 찾는다.

    ⚠️ 2026-07-28 실제 law_data(39,459건)·admrul_data(13,716건) 전수 검증으로 확정: appendix
    객체 자체의 `filename` 필드(files[].name 이 아니다 — 그건 항상 빈 문자열)가 git export 저장
    파일명을 그대로 담고 있고 100% 신뢰할 수 있다(است is_file_only=false라 파일 자체가 없는
    경우만 예외 — 그건 애초에 이 함수가 호출되지 않는다). 그래서 `filename` 을 최우선으로 쓰고,
    혹시 없는 과거 데이터가 있을까봐 예전 라벨 재구성 방식을 폴백으로만 남긴다.

    후보 파일이 없거나 실제로 존재하지 않으면 None을 돌려준다(호출부가 "파일 없음"으로 로그를
    남기고 다음 항목으로 계속 진행 — 배치를 막지 않는다)."""
    bases = document_dir_candidates(repo_root, data, doc_dir)
    # ① 수집기가 payload 에 직접 심어준 문서폴더 기준 상대경로(`{file_dir}/{filename}`).
    #    저장 규칙과 같은 함수(mdexport.appendix_dir/appendix_fname)로 만든 값이라 kind→폴더
    #    변환을 여기서 다시 추측할 필요가 없다(수집기 common.enrich_file_meta).
    rel = str(appendix.get("local_path") or "").strip()
    if rel:
        parts = [_safe_name(x) for x in rel.split("/") if x]
        for base in bases:
            candidate = base.joinpath(*parts)
            if candidate.exists():
                return candidate
    sub = _appendix_subdir(appendix.get("kind"))
    filename = appendix.get("filename")
    if filename:
        fname = _safe_name(str(filename))
        for base in bases:
            candidate = base / sub / fname
            if candidate.exists():
                return candidate
    # 폴백: filename 필드가 없는 예전 데이터 대비(실측으로는 발생하지 않았음)
    picked = _pick_file(appendix.get("files") or [])
    if not picked:
        return None
    fname = _safe_name(f"{_appendix_label(appendix)}.{_ext_of(picked)}")
    for base in bases:
        candidate = base / sub / fname
        if candidate.exists():
            return candidate
    return None


_ARTICLE_IMAGE_SUFFIXES = (".gif", ".png", ".jpg", ".jpeg")


def find_article_images(repo_root: Path, data: Dict[str, Any], article: Dict[str, Any],
                        doc_dir: Optional[Path] = None) -> List[Path]:
    """조문 content 안의 `[그림]` 마커에 대응하는 본문이미지 파일을 순번 순으로 찾는다(STEP1).

    실제 admrul_data로 확인(2026-07-29): 본문이미지/{article_no}_{순번}.{gif|png|jpg} 형식이며
    article_no 필드 값 자체가 이미 "제N조" 또는 "제N조의M" 형태를 포함한다(별도로 "제"·"조"를
    덧붙이지 않는다 — 예: 제5조 content 안 `[그림]` 2개 ↔ 제5조_1.gif·제5조_2.gif 실측 확인).
    순번 순으로 정렬하면 본문 등장 순서와 1:1 대응한다."""
    article_no = str(article.get("article_no") or "").strip()
    if not article_no:
        return []
    for base in document_dir_candidates(repo_root, data, doc_dir):
        image_dir = base / "본문이미지"
        if image_dir.is_dir():
            break
    else:
        return []
    prefix = f"{article_no}_"
    candidates = []
    for p in image_dir.iterdir():
        if not p.is_file() or p.suffix.lower() not in _ARTICLE_IMAGE_SUFFIXES:
            continue
        if not p.name.startswith(prefix):
            continue
        seq = p.name[len(prefix):-len(p.suffix)]
        if seq.isdigit():
            candidates.append((int(seq), p))
    return [p for _, p in sorted(candidates)]


def ocr_image_file(doc_parser: "DocParserClient", settings: Settings, image_path: Path) -> Tuple[Optional[str], bool]:
    """이미지 파일 하나를 **이미지 전용** 엔드포인트(/preprocess_intelligent)로 OCR 한다.

    반환 (텍스트 또는 None, 일시 오류였는지). 인라인 글리프(초소형 수식·기호 조각)는
    Doc Parser 가 못 읽어 낭비이므로 아예 건너뛴다 — 그 자리는 마커로 남는다(성공/실패 아님).
    레포 경로 기반(resolve_article_images)과 package 전달 기반(package._ocr_package_image)이
    같은 호출 규약을 쓰도록 여기 하나로 모아 둔다."""
    side = image_min_side(image_path)
    if side is not None and side < MIN_ARTICLE_IMAGE_SIDE:
        return None, False
    try:
        chunks = run_doc_parser(
            doc_parser, image_path, settings.doc_parser_chunk_size, settings.doc_parser_chunk_overlap,
            settings.doc_parser_shared_host_dir, settings.doc_parser_shared_container_dir,
            settings.doc_parser_keep_temp_files, image_endpoint_path=settings.doc_parser_image_endpoint_path)
        return ("\n".join(c.get("content", "") for c in chunks if c.get("content")) or None), False
    except DocParserError as exc:
        return None, not is_permanent_parser_error(exc)


def substitute_image_markers(content: str, ocr: List[Tuple[Optional[str], bool]]
                             ) -> Tuple[Optional[str], int, int, int]:
    """content 의 `[그림]` 을 OCR 결과로 **등장 순서대로** 치환한다.

    ocr[i] = (텍스트 또는 None, 일시오류인가). 텍스트가 없으면 그 자리는 `[그림]` 마커로 남긴다
    (조문 전체를 실패로 치지 않는다). `[별표N]`·`[별지]`·`[서식]`·`[표]` 같은 다른 마커는
    이미지가 아니라 텍스트 참조라 절대 건드리지 않는다.
    반환: (치환된 content 또는 변화 없으면 None, 성공, 실패, 그중 일시오류 수)."""
    texts = [t for t, _ in ocr]
    success = sum(1 for t in texts if t)
    failed = len(texts) - success
    transient_failed = sum(1 for t, tr in ocr if not t and tr)
    parts = content.split("[그림]")
    rebuilt = parts[0]
    for i, tail in enumerate(parts[1:]):
        rebuilt += (texts[i] if i < len(texts) and texts[i] else "[그림]") + tail
    return (rebuilt if rebuilt != content else None), success, failed, transient_failed


def resolve_article_images(doc_parser: "DocParserClient", settings: Settings, repo_root: Path,
                           data: Dict[str, Any], article: Dict[str, Any],
                           doc_dir: Optional[Path] = None) -> Tuple[Optional[str], int, int, int]:
    """조문 content의 `[그림]` 마커를 본문이미지 OCR 텍스트로 순서대로 치환한다(STEP1).

    `[별표N]`·`[별지]`·`[서식]`·`[표]` 같은 다른 참조 마커는 이미지가 아니라 텍스트 참조이므로
    절대 건드리지 않는다 — `[그림]` 문자열 자체만 치환 대상이다(실측: 이 마커들은 법령
    34건·행정규칙 10,873건 등장하지만 이미지 개수와는 무관한 별개 텍스트).

    이미지를 못 찾거나 OCR이 실패하면 그 `[그림]` 하나만 마커 그대로 남기고(조문 전체를 실패로
    치지 않음) 나머지는 계속 치환한다. 반환: (치환된 content 또는 변화 없으면 None,
    성공 이미지 수, 실패 이미지 수, 그중 **일시 오류**(timeout·연결·5xx) 수).
    일시 오류 수를 따로 돌려주는 이유: 결정 실패(못 읽는 글리프)는 마커 유지가 최종 상태지만,
    전처리기가 잠깐 죽어서 실패한 것은 재시도하면 살아난다 — 호출자가 이 수가 0 이 아닐 때
    문서를 오류로 남겨 재시도 경로(package 재소비·failed-out 재실행)에 태워야, 그 시간대에
    소비된 문서들의 그림 텍스트가 조용히 빠진 채 굳지 않는다.

    초기적재(pipeline.index_documents)와 증분(package._flush_law) 양쪽에서 공유한다 — repo_root 만
    맞게 넘기면 두 경로 모두 로컬 git 데이터 레포에서 같은 방식으로 이미지를 찾는다."""
    content = str(article.get("content") or "")
    if "[그림]" not in content:
        return None, 0, 0, 0
    images = find_article_images(repo_root, data, article, doc_dir)
    if not images:
        return None, 0, 0, 0

    def _ocr_one(image_path: Path) -> tuple:
        return ocr_image_file(doc_parser, settings, image_path)

    # 인라인 글리프(초소형 수식·기호 조각)는 Doc Parser 가 못 읽어(낭비) 아예 건너뛴다 —
    # 그 [그림] 자리는 마커 그대로 남는다(성공/실패로 세지 않음).
    jobs: list = []                                     # (index, path) — OCR 을 실제로 돌릴 것만
    ocr_texts: list = []
    for image_path in images:
        side = image_min_side(image_path)
        if side is not None and side < MIN_ARTICLE_IMAGE_SIDE:
            ocr_texts.append(None)
        else:
            jobs.append((len(ocr_texts), image_path))
            ocr_texts.append(None)                       # 자리 확보 — 결과는 index 로 되쓴다

    # ⚠ 이미지 OCR(intelligent)은 장당 21~42초(실측)라 순차 호출이 병목이다. 그림 많은 문서는
    #   문서 하나에 수 분~수십 분 → DOC_PARSER_IMAGE_CONCURRENCY(기본 1=종전과 동일)로
    #   같은 문서 안 이미지들을 동시에 호출한다. 호출은 서로 독립이고 결과는 자리(index)로
    #   되쓰므로 [그림] 치환 순서는 그대로 유지된다.
    transient: dict = {}
    workers = max(1, int(getattr(settings, "doc_parser_image_concurrency", 1) or 1))
    if jobs and workers > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(workers, len(jobs))) as pool:
            for (idx, _), (text, is_transient) in zip(jobs, pool.map(lambda j: _ocr_one(j[1]), jobs)):
                ocr_texts[idx] = text
                transient[idx] = is_transient
    else:
        for idx, image_path in jobs:
            ocr_texts[idx], transient[idx] = _ocr_one(image_path)

    success = sum(1 for idx, _ in jobs if ocr_texts[idx])
    failed = len(jobs) - success
    transient_failed = sum(1 for idx, _ in jobs if not ocr_texts[idx] and transient.get(idx))

    parts = content.split("[그림]")
    rebuilt = parts[0]
    for i, tail in enumerate(parts[1:]):
        replacement = ocr_texts[i] if i < len(ocr_texts) and ocr_texts[i] else "[그림]"
        rebuilt += replacement + tail
    return (rebuilt if rebuilt != content else None), success, failed, transient_failed


# §2 공통 규칙(전처리CASE 문서): Doc Parser 가 실제로 지원하는 확장자. zip·hml 은 명시적으로
# 미지원(실측: admrul zip 610건·hml 3건 — 전부 skip 대상).
SUPPORTED_DOC_PARSER_SUFFIXES = {
    ".pdf", ".hwp", ".hwpx", ".docx", ".ppt", ".pptx", ".csv", ".xlsx",
    ".jpg", ".jpeg", ".png", ".gif", ".txt",
}
# 같은 문서의 형식 재수출(예: X.hwp + X.pdf)을 하나로 합칠 때 남길 우선순위.
_FORMAT_DEDUP_PRIORITY = (".hwpx", ".hwp", ".docx", ".pdf")


def resolve_pending_document_files(repo_root: Path, data: Dict[str, Any],
                                   doc_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """조문0(본문=파일) 문서의 attachments[] 를 §2 공통 규칙대로 걸러 실제 처리할 파일 목록을 만든다.

    document_is_file_only=True 일 때만 의미가 있다(본문 있으면 호출부가 아예 안 부름). 파일명
    키워드로 "원문/이유서/신구조문" 같은 역할을 추측하지 않는다 — payload에 그런 구분 신호가
    없어(실측) role 기반 필터링은 불가능하다(전처리CASE §3 결정). 대신 결정적 규칙 2개만 적용:

    1) 미지원 확장자(zip·hml 등) 제외.
    2) 같은 basename(확장자만 다름 — 예: "고시.hwp"+"고시.pdf")은 형식 재수출이므로
       hwpx>hwp>docx>pdf 우선순위로 1개만 남긴다.

    그 외 남는 파일은 role 상관없이 전부 처리 대상이다 — 실측(2026-07-29): 조문0+attachments
    611건 중 다수가 "원문+조문별제개정이유서+고시문"처럼 서로 다른 실제 문서라(예: "전원개발사업
    실시계획(변경)…" 문서는 4개 파일=2개 실제 문서×hwp/pdf 쌍 → 중복 제거 후 2개 다 처리),
    하나만 골라 버리면 나머지 실제 문서의 내용이 통째로 유실된다."""
    if not document_is_file_only(data):
        return []
    groups: Dict[str, Dict[str, Any]] = {}
    seen_paths = set()
    for attachment in data.get("attachments") or []:
        if not isinstance(attachment, dict):
            continue
        local_path = attachment_local_path(repo_root, data, attachment, doc_dir)
        if local_path is None or local_path.suffix.lower() not in SUPPORTED_DOC_PARSER_SUFFIXES:
            continue
        resolved = str(local_path.resolve())
        if resolved in seen_paths:
            continue  # 완전히 같은 파일을 가리키는 중복 항목(실측 사례 있음)
        seen_paths.add(resolved)

        suffix = local_path.suffix.lower()
        if suffix in _FORMAT_DEDUP_PRIORITY:
            key = _safe_name(local_path.stem).lower()
            existing = groups.get(key)
            if existing is not None:
                existing_suffix = existing["local_path"].suffix.lower()
                if _FORMAT_DEDUP_PRIORITY.index(suffix) >= _FORMAT_DEDUP_PRIORITY.index(existing_suffix):
                    continue  # 이미 더 우선순위 높은 포맷을 채택한 상태
            groups[key] = {"attachment": attachment, "local_path": local_path}
        else:
            # hwpx/hwp/docx/pdf 그룹 밖(이미지 등)은 basename 이 같아도 재수출 쌍으로 보지 않는다.
            groups[f"{resolved}"] = {"attachment": attachment, "local_path": local_path}
    return list(groups.values())


def attachment_local_path(repo_root: Path, data: Dict[str, Any], attachment: Dict[str, Any],
                          doc_dir: Optional[Path] = None) -> Optional[Path]:
    """행정규칙류 최상위 attachments[] 항목(document_is_file_only 판정된 문서의 첨부파일)의 실제
    로컬 파일 경로를 찾는다. attachment.filename 필드가 100% 신뢰 가능함을 실제 admrul_data
    9,541건 중 9,540건 일치로 확인했다(나머지 1건은 개별 데이터 이슈로 별도 확인 필요, §10 참고)."""
    filename = attachment.get("filename") or attachment.get("name")
    if not filename:
        return None
    bases = document_dir_candidates(repo_root, data, doc_dir)
    rel = str(attachment.get("local_path") or "").strip()
    if rel:                                              # 수집기가 심어준 `{file_dir}/{filename}`
        parts = [_safe_name(x) for x in rel.split("/") if x]
        for base in bases:
            candidate = base.joinpath(*parts)
            if candidate.exists():
                return candidate
    for base in bases:
        for folder in ("첨부파일", "원문"):              # 원문은 기존 데이터 repo 호환 fallback
            candidate = base / folder / _safe_name(str(filename))
            if candidate.exists():
                return candidate
    return None


# ── 문서 전체가 파일에만 있는 경우 판정(§3-2) ────────────────────────────
#
# 최상위 attachments[] 를 무조건 전처리하지 않는다(§1 리뷰 이후 확정). 대신 본문에 의미 있는
# 텍스트가 실제로 있는지 확인한 뒤에만 "문서 전체가 파일뿐"이라고 판단한다.

_PLACEHOLDER_ONLY_RE = re.compile(
    r"^\s*[\[［(]?\s*(첨부\s*파일\s*참조|별첨\s*참조|원문\s*은\s*첨부\s*파일\s*참조|첨부\s*파일\s*원문\s*참조|"
    r"별지\s*참조|그림\s*참조|이미지\s*참조)\s*[\])］]?\s*$"
)


def _looks_like_placeholder(content: str) -> bool:
    """공백뿐이거나 '[첨부파일 참조]' 류 안내문뿐인 content 인지 판정한다(본문으로 보지 않는다).

    ⚠️ `[그림]` 단독은 여기 포함하지 않는다 — 콜렉터가 인라인 이미지를 표시하는 토큰이라
    "본문 없음" 신호가 아니라 "이미지가 있는 본문"이다(실제 admrul_data 문서로 확인, §4).
    "그림 참조"처럼 명시적 안내문(그림 하나 없이 참조하라는 문구)만 placeholder로 본다."""
    text = content.strip()
    if not text:
        return True
    return bool(_PLACEHOLDER_ONLY_RE.match(text))


def body_has_meaningful_text(data: Dict[str, Any]) -> bool:
    """body.articles[] 중 실제 텍스트로 볼 수 있는 content 가 하나라도 있으면 True.

    단순히 body/articles 존재 여부만 보지 않는다 — 공백뿐이거나 "[첨부파일 참조]" 같은
    안내문만 있는 조문은 실질 본문으로 치지 않는다(실제 admrul_data 에 body.articles=[] 인
    문서 178건/3,300건이 있었고, 그 문서들은 전부 attachments[] 로 원문을 제공함)."""
    if not isinstance(data, dict):
        return False
    body = data.get("body")
    articles = body.get("articles") if isinstance(body, dict) else None
    if not articles:
        return False
    return any(
        isinstance(a, dict) and not _looks_like_placeholder(str(a.get("content") or ""))
        for a in articles
    )


def has_document_attachment(data: Dict[str, Any]) -> bool:
    """행정규칙류 최상위 attachments[] 가 하나라도 있는지."""
    return bool(isinstance(data, dict) and data.get("attachments"))


def document_is_file_only(data: Dict[str, Any]) -> bool:
    """문서 본문 전체가 파일에만 존재하는지(별표 단위 is_file_only 와는 별개 축).

    본문 텍스트가 있으면 최상위 원문 파일은 절대 자동 전처리하지 않는다(중복 적재 방지).
    본문도 없고 원문 파일도 없으면 False — 그 경우는 실패/미처리로 기록하고 계속 진행한다
    (호출부인 pipeline.py 가 처리)."""
    return not body_has_meaningful_text(data) and has_document_attachment(data)


# 별표/별지/서식이 삭제된 경우 collector 는 content 에 "[라벨] 삭제"/"<삭제>" 류 안내문만 남긴다
# (실제 파일도 대개 빈 스텁이거나 없음). 실측(2026-07-29, admrul_data): "[별지 제1호서식] <삭제>",
# "삭제 <2026. 4. 15.>" 등. 라벨([별표 2]/[별지 제1호서식]/[별첨] 등)은 있어도 되고 없어도 되며,
# 뒤에 날짜([<2026. 4. 15.>] 류)가 붙어도 된다 — 그 외 실질 내용이 있으면 매치되지 않는다.
_DELETED_APPENDIX_RE = re.compile(
    r"^\s*(?:\[[^\]]{0,30}\]\s*)?[<(]?\s*삭제\s*[>)]?\s*(?:<[^>]{0,30}>)?\s*$"
)


def is_deleted_appendix_content(content: str) -> bool:
    """별표/별지/서식 content 가 "삭제됨" 안내문뿐인지 판정한다(§STEP2) — 실질 내용이 없는
    노이즈라 임베딩 대상에서 제외한다."""
    return bool(_DELETED_APPENDIX_RE.match(content or ""))


def find_pending_appendices(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """appendices[] 중 is_file_only=True 인 항목만 뽑는다 — Doc Parser 호출 트리거는 이 조건 하나뿐이다.

    실제 샘플 전수 확인 결과 이 필드는 항상 real JSON boolean(quote 없는 true/false)이라
    문자열 "true" 방어 코드는 두지 않는다 — is True 로 엄격 비교한다.

    행정규칙류 최상위 attachments[](원문 전체)는 일부러 여기서 다루지 않는다. attachments[] 에는
    is_file_only 필드 자체가 없어(payload_desc.md·실제 샘플 모두 확인) "is_file_only == True 일
    때만 전처리기 호출"이라는 요구사항의 트리거 조건에 해당하지 않는다. 본문 텍스트가 없는 문서를
    실을 방법이 필요하면 기존 index-files(디렉터리 크롤링)나 index-attachment-chunks(전처리된 청크
    JSON 핸드오프)를 그 목적에 맞게 별도로 쓰는 편이 안전하다(자동·조건부 트리거가 아니므로)."""
    return [ap for ap in (data.get("appendices") or []) if isinstance(ap, dict) and ap.get("is_file_only") is True]


# ── Doc Parser(첨부용 전처리기) HTTP 클라이언트 ──────────────────────────

TIMEOUT = "TIMEOUT"
CONNECTION_ERROR = "CONNECTION_ERROR"
HTTP_ERROR = "HTTP_ERROR"
APPLICATION_ERROR = "APPLICATION_ERROR"  # 응답은 받았지만 code != 0
EMPTY_RESULT = "EMPTY_RESULT"

# 재시도할 일시적 HTTP 상태(전처리기 재시작·과부하·레이트리밋). 그 외 4xx 는 결정적이라 즉시 중단.
_RETRYABLE_HTTP = (429, 500, 502, 503, 504)


def _retry_backoff(attempt: int) -> float:
    """지수 백오프(최대 8초) — 재시도 간 서버를 쉬게 한다(과부하 시 hammering 방지)."""
    return min(2.0 ** attempt, 8.0)


class DocParserError(Exception):
    """Doc Parser 호출 실패. code 로 timeout/연결실패/비정상응답/빈결과를 구분한다."""

    def __init__(self, code: str, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message


def is_permanent_parser_error(exc: DocParserError) -> bool:
    """같은 파일로 다시 불러도 결과가 같은 **결정오류**인지 판정한다.

    True(원천 파일 한계): DRM/손상/미지원(code!=0 = APPLICATION_ERROR), 빈 결과(EMPTY_RESULT),
    4xx(429 제외). 이런 파일은 재시도(문서 재색인·package 재소비)해도 똑같이 실패하므로,
    호출자는 실패 큐/package 오류가 아니라 '보류(원천 한계)'로 기록해야 한다 — 실패 큐에 넣으면
    재시도 목록이 영영 안 비고, package 오류로 넣으면 그 package 가 재시도 끝에 격리된다.
    False(일시 오류): timeout·연결 실패·5xx·429 — 재시도 가치가 있다.
    영구 차단 장부는 두지 않는다 — 문서가 개정되면 새 파일이 오므로 그때는 다시 시도해야 한다."""
    if exc.code in (APPLICATION_ERROR, EMPTY_RESULT):
        return True
    if exc.code == HTTP_ERROR:
        m = re.match(r"HTTP (\d{3})", exc.message or "")
        return bool(m and m.group(1).startswith("4") and m.group(1) != "429")
    return False


class DocParserClient:
    """첨부용 전처리기 HTTP 클라이언트. GET /healthcheck, POST /run."""

    def __init__(self, base_url: str, timeout: float = 60.0, max_retries: int = 2,
                 endpoint_path: str = "/run", api_key: Optional[str] = None, upload: bool = False):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        # genos 코드서빙은 /run 이 아니라 /preprocess_attachment(_upload) 이고 Bearer 인증이 필요하다.
        # 그 외(구 목업 전처리기)는 /run · 무인증 그대로.
        self.endpoint_path = "/" + endpoint_path.strip("/")
        self.api_key = api_key
        # upload=True 면 file_path(JSON) 대신 파일 바이트를 multipart 로 업로드한다(공유 볼륨 불필요).
        # genos 의 /preprocess_attachment_upload 라우트용. False 면 기존 file_path(JSON) 방식.
        self.upload = upload

    @staticmethod
    def _multipart_body(file_path: Union[Path, str], params_json: str) -> Tuple[bytes, str]:
        """file(업로드) + params(JSON 문자열) 를 multipart/form-data 로 조립한다(urllib 전용).

        전처리기는 확장자로 형식을 판단하므로 파일명은 확장자만 보존한 ASCII 이름을 쓴다(한글
        파일명 헤더 인코딩 이슈 회피 — 추출 텍스트는 파일 내용에서 나오지 그 이름과 무관)."""
        path = Path(file_path)
        filename = "attachment" + path.suffix.lower()
        boundary = "----lawindexer" + uuid.uuid4().hex
        b = boundary.encode()
        parts = [
            b"--", b, b"\r\n",
            b'Content-Disposition: form-data; name="file"; filename="', filename.encode(), b'"\r\n',
            b"Content-Type: application/octet-stream\r\n\r\n", path.read_bytes(), b"\r\n",
            b"--", b, b"\r\n",
            b'Content-Disposition: form-data; name="params"\r\n\r\n', params_json.encode("utf-8"), b"\r\n",
            b"--", b, b"--\r\n",
        ]
        return b"".join(parts), f"multipart/form-data; boundary={boundary}"

    def health_check(self) -> bool:
        """/healthcheck 가 2xx 로 응답하면 True. 예외는 모두 False로 취급(호출부가 로그로 남김)."""
        try:
            with urllib.request.urlopen(f"{self.base_url}/healthcheck", timeout=self.timeout) as resp:
                return 200 <= resp.status < 300
        except Exception:
            return False

    def run(self, file_path: Union[Path, str], chunk_size: int, chunk_overlap: int,
            endpoint_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """POST /run. code==0 이고 text가 비어있지 않은 청크만 반환한다.

        HTTP timeout·연결 실패·비정상 응답(code!=0)·빈 결과(data 없음 또는 전부 빈 text)를
        서로 다른 DocParserError.code 로 구분해서 낸다.

        재시도는 **일시적 네트워크 오류(TIMEOUT·CONNECTION_ERROR)에만** max_retries 횟수만큼
        한다. 결정적 오류(HTTP_ERROR·APPLICATION_ERROR·EMPTY_RESULT·응답 파싱 실패)는 같은
        입력이면 반복해도 결과가 같으므로 즉시 raise 한다 — 법령 본문이미지처럼 전처리기가
        못 읽는 파일(초소형 수식 글리프 등)에서 code!=0 을 3번씩 재시도하던 낭비 제거."""
        # genos 전처리기는 내부에서 청킹하므로 params 를 비워 보낸다. 구 /run 목업만 chunk_size/overlap 사용.
        # 호출별 엔드포인트 오버라이드(이미지=첨부용 · 문서=적재용). 없으면 기본 엔드포인트.
        endpoint = "/" + endpoint_path.strip("/") if endpoint_path else self.endpoint_path
        params = {"chunk_size": chunk_size, "chunk_overlap": chunk_overlap} if endpoint == "/run" else {}
        if self.upload:
            payload, content_type = self._multipart_body(file_path, json.dumps(params, ensure_ascii=False))
            headers = {"Content-Type": content_type}
        else:
            payload = json.dumps({"file_path": str(file_path), "params": params}).encode("utf-8")
            headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        url = f"{self.base_url}{endpoint}"

        last_error: Optional[DocParserError] = None
        for _attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    body = resp.read()
            except urllib.error.HTTPError as exc:
                last_error = DocParserError(HTTP_ERROR, f"HTTP {exc.code}: {exc.reason}")
                # 5xx/429 = 일시장애(재시작·과부하·레이트리밋) → 백오프 후 재시도.
                # 그 외 4xx = 결정적(같은 입력이면 반복해도 같음) → 즉시 중단.
                if exc.code in _RETRYABLE_HTTP and _attempt < self.max_retries:
                    time.sleep(_retry_backoff(_attempt))
                    continue
                break
            except urllib.error.URLError as exc:
                if isinstance(exc.reason, TimeoutError) or "timed out" in str(exc.reason).lower():
                    last_error = DocParserError(TIMEOUT, f"{url} timeout({self.timeout}s)")
                else:
                    last_error = DocParserError(CONNECTION_ERROR, f"{url} 연결 실패: {exc.reason}")
                if _attempt < self.max_retries:
                    time.sleep(_retry_backoff(_attempt))
                continue
            except TimeoutError:
                last_error = DocParserError(TIMEOUT, f"{url} timeout({self.timeout}s)")
                if _attempt < self.max_retries:
                    time.sleep(_retry_backoff(_attempt))
                continue

            try:
                result = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                last_error = DocParserError(HTTP_ERROR, f"응답 JSON 파싱 실패: {exc}")
                break  # 결정적 — 재시도 안 함

            if not isinstance(result, dict) or result.get("code") != 0:
                code = result.get("code") if isinstance(result, dict) else None
                err_msg = result.get("errMsg") if isinstance(result, dict) else str(body[:200])
                last_error = DocParserError(APPLICATION_ERROR, f"code={code} errMsg={err_msg}")
                break  # 결정적 — 재시도 안 함

            data = result.get("data")
            if not isinstance(data, list) or not data:
                last_error = DocParserError(EMPTY_RESULT, "응답 data 배열이 비어 있습니다")
                break  # 결정적 — 재시도 안 함

            chunks = [c for c in data if isinstance(c, dict) and str(c.get("text") or "").strip()]
            if not chunks:
                last_error = DocParserError(EMPTY_RESULT, "text가 있는 청크가 하나도 없습니다")
                break  # 결정적 — 재시도 안 함
            return chunks

        raise last_error


# 인라인 글리프 컷: 짧은 변이 이보다 작은 본문이미지는 문서가 아니라 수식·기호 조각이라
# Doc Parser 가 인식하지 못한다(실측: 법령 본문이미지 46x11 → code:1 실패). admrul 의 의미
# 있는 본문이미지(조직도·표·공고문)는 전부 이보다 큼(실측 최소 높이 72px) — 양쪽에 안전한
# 컷이며, 걸러진 자리는 [그림] 마커를 그대로 남긴다(조문 본문은 정상 색인).
MIN_ARTICLE_IMAGE_SIDE = 16


def image_min_side(path: Path) -> Optional[int]:
    """이미지의 짧은 변 길이(px). 못 읽으면 None(그 경우 컷하지 않고 Doc Parser 에 맡긴다)."""
    try:
        from PIL import Image
        with Image.open(path) as im:
            return min(im.size)
    except Exception:
        return None


# 실제 doc-parser-local 컨테이너로 확인(2026-07-29): 원본 .gif 를 그대로 넘기면
# "Partitioning is not supported for the FileType.UNK file type." 로 거부된다 — 확장자 목록엔
# gif 가 있다고 문서화돼 있지만 실제로는 지원하지 않는다. admrul_data 본문이미지는 전량(20,002건)
# .gif 라 이 변환이 없으면 STEP1 이미지 OCR이 사실상 전부 실패한다.
_UNSUPPORTED_IMAGE_SUFFIXES = {".gif"}

# 전처리기가 둘이다(genos 코드서빙, 같은 base 에 엔드포인트만 다름 · 둘 다 multipart 업로드):
#   · 첨부용(/preprocess_attachment_upload)  — hwp/hwpx/pdf/docx 등 문서. 기본 경로.
#   · 적재용(/preprocess_intelligent_upload) — **이미지**. 이미지에만 이걸 쓴다.
_IMAGE_SUFFIXES = {".gif", ".png", ".jpg", ".jpeg", ".bmp", ".webp", ".tif", ".tiff"}


def is_image_file(path: Path) -> bool:
    """이미지면 True — 이미지만 적재용(intelligent) 으로 보내고 나머지는 첨부용으로 보낸다."""
    return path.suffix.lower() in _IMAGE_SUFFIXES


def convert_image_for_doc_parser(local_path: Path) -> Optional[Path]:
    """Doc Parser 가 못 읽는 이미지 포맷을 PNG 로 변환해 별도 임시 파일로 만든다.

    data/ 는 읽기 전용이라 변환 결과를 원본 옆에 쓰지 않고 항상 새 임시 디렉터리에 만든다.
    변환이 필요 없는 포맷이면 None(호출부가 원본 경로를 그대로 쓰면 됨)."""
    if local_path.suffix.lower() not in _UNSUPPORTED_IMAGE_SUFFIXES:
        return None
    from PIL import Image  # 이 변환 용도로만 필요 — 다른 곳에서 임포트하지 않는다

    tmp_dir = Path(tempfile.mkdtemp(prefix="law_indexer_img_"))
    try:
        converted_path = tmp_dir / f"{local_path.stem}.png"
        with Image.open(local_path) as im:
            im.convert("RGB").save(converted_path, format="PNG")
        return converted_path
    except Exception:                        # 손상 이미지 등 변환 실패 시 임시 디렉터리 누수 방지
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


def cleanup_converted_image(converted_path: Optional[Path]) -> None:
    """convert_image_for_doc_parser 가 만든 임시 파일·디렉터리를 정리한다."""
    if converted_path is None:
        return
    try:
        converted_path.unlink(missing_ok=True)
        converted_path.parent.rmdir()
    except OSError:
        pass


def stage_for_doc_parser(local_path: Path, host_dir: Optional[Path],
                         container_dir: Optional[str] = None) -> Tuple[Path, str, bool]:
    """전처리기 컨테이너가 접근 가능한 공유 디렉터리로 파일을 복사한다.

    전처리기는 원본 파일 옆에 이미지·리소스 폴더를 만들 수 있어(원본 데이터 저장소 워킹트리를
    더럽히면 안 된다), 인덱서와 전처리기가 다른 환경이면 공유 볼륨으로 복사한 뒤 그 경로로
    /run 을 호출해야 한다.

    ⚠️ 2026-07-28 실제 doc-parser-local 컨테이너로 확인: 호스트 마운트 경로와 컨테이너 내부
    경로가 다르다(`/Users/.../doc-parser-test` → 컨테이너 `/data`). 호스트 절대경로를 그대로
    `/run` 에 넘기면 컨테이너가 그 경로를 못 찾는다("Read-only file system"/파일없음류 오류로
    나타남). 그래서 host_dir(실제 파일을 복사해 둘 호스트 경로)과 container_dir(그 마운트가
    컨테이너 안에서 보이는 경로)를 분리해서 받는다. host_dir 이 None 이면 원본 경로를 그대로
    쓴다(인덱서와 전처리기가 완전히 같은 파일시스템을 보는 경우).

    반환: (호스트에서의 실제 파일 경로(정리용), Doc Parser /run 에 보낼 경로(컨테이너 관점),
    staged 여부(정리 대상인지))."""
    if host_dir is None:
        return local_path, str(local_path), False
    host_dir = Path(host_dir)
    host_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{uuid.uuid4().hex}_{local_path.name}"
    staged_host_path = host_dir / fname
    shutil.copy2(local_path, staged_host_path)
    request_path = f"{container_dir.rstrip('/')}/{fname}" if container_dir else str(staged_host_path)
    return staged_host_path, request_path, True


def cleanup_staged_file(staged_host_path: Path, was_staged: bool, keep: bool) -> None:
    """staged 임시 파일을 호스트 경로 기준으로 정리한다. keep=True 면(디버깅용) 남긴다."""
    if was_staged and not keep:
        try:
            staged_host_path.unlink(missing_ok=True)
        except OSError:
            pass


def run_doc_parser(client: "DocParserClient", local_path: Path, chunk_size: int, chunk_overlap: int,
                   shared_host_dir: Optional[Path], shared_container_dir: Optional[str],
                   keep_temp_files: bool,
                   image_endpoint_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """공유 경로로 staging → Doc Parser /run → 정규화된 청크 목록(§ pipeline·package 공용).

    성공/실패와 무관하게 staged 임시 파일을 정리한다. .gif 처럼 Doc Parser 가 못 읽는 이미지
    포맷은 스테이징 전에 PNG 로 변환한다(원본 data/ 는 읽기 전용이라 변환은 항상 임시 파일로).
    호스트/컨테이너 마운트 경로가 다르면 컨테이너 관점의 경로로 /run 을 호출한다.

    image_endpoint_path 를 주면 **이미지에만** 그 엔드포인트를 쓴다(적재용/intelligent).
    문서(hwp/pdf 등)는 항상 client 기본 엔드포인트(첨부용)로 간다."""
    converted_path = convert_image_for_doc_parser(local_path)
    try:
        source_path = converted_path or local_path
        staged_host_path, request_path, was_staged = stage_for_doc_parser(
            source_path, shared_host_dir, shared_container_dir)
        # 이미지만 적재용(intelligent), 문서(hwp/pdf 등)는 첨부용(client 기본).
        endpoint = image_endpoint_path if is_image_file(local_path) else None
        try:
            # endpoint 가 없으면 기존 시그니처 그대로 부른다 — 엔드포인트를 모르는 client(테스트 fake 등)도 그대로 동작.
            raw_chunks = (client.run(request_path, chunk_size, chunk_overlap, endpoint_path=endpoint)
                          if endpoint else client.run(request_path, chunk_size, chunk_overlap))
        finally:
            cleanup_staged_file(staged_host_path, was_staged, keep_temp_files)
    finally:                                 # staging 이 실패해도 변환 임시파일은 항상 정리
        cleanup_converted_image(converted_path)
    return normalize_doc_parser_chunks(raw_chunks)


def normalize_doc_parser_chunks(raw_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Doc Parser 원시 응답 청크를 첨부 매핑(map_attachment_data/build_attachment_provisions)이
    기대하는 청크 형식으로 맞춘다. text가 비어있는 청크는 DocParserClient.run 에서 이미 제외됐다."""
    normalized = []
    for chunk in raw_chunks:
        normalized.append({
            "content": chunk.get("text", ""),
            "chunk_index": chunk.get("i_chunk_on_doc"),
            "page_no": chunk.get("i_page"),
            "start_page": chunk.get("i_page"),
            "end_page": chunk.get("e_page"),
            "chunk_bboxes": chunk.get("chunk_bboxes"),
            "media_files": chunk.get("media_files"),
            "guardrail_categories": chunk.get("guardrail_categories"),
            "n_char": chunk.get("n_char"),
            "n_word": chunk.get("n_word"),
            "n_line": chunk.get("n_line"),
            "parser_reg_date": chunk.get("reg_date"),
        })
    return normalized


def file_sha256(path: Path) -> Optional[str]:
    """원본 첨부파일 자체의 해시(청크 텍스트 해시인 content_hash 와는 다르다)."""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


# ── 기존 index-files(파일 크롤링) 명령의 전처리 엔트리 포인트 ──────────────
#
# preprocess_file 이 원래 지정된 "교체 지점"이었다(파일 경로 → index-attachment-chunks 입력
# dict). client 를 주면 실제 DocParserClient 를 호출하고, 안 주면(단위 테스트 등) 기존과
# 동일한 자리표시 청크를 만든다 — 기존 목업 동작과 완전히 호환된다.

def _mock_chunks(file_path: Path) -> List[Dict[str, Any]]:
    """DocParserClient 가 없을 때(단위 테스트 등) 쓰는 자리표시 청크."""
    return [{
        "page_no": 1,
        "chunk_index": 0,
        "content": f"[MOCK 전처리 미연결] {file_path.name} 의 추출 텍스트가 여기 들어온다.",
    }]


def preprocess_file(file_path: Path, parent: Optional[Dict[str, Any]] = None, *,
                    client: Optional[DocParserClient] = None,
                    chunk_size: int = 2000, chunk_overlap: int = 200,
                    shared_dir: Optional[Path] = None, shared_container_dir: Optional[str] = None,
                    keep_temp_files: bool = False) -> Dict[str, Any]:
    """첨부파일 1건 → index-attachment-chunks 가 먹는 dict.

    client 를 주면 실제 Doc Parser 로 전처리하고, 없으면 목업 청크를 만든다.
    """
    parent = parent or {}
    if client is not None:
        staged_host_path, request_path, was_staged = stage_for_doc_parser(
            file_path, shared_dir, shared_container_dir)
        try:
            raw_chunks = client.run(request_path, chunk_size, chunk_overlap)
        finally:
            cleanup_staged_file(staged_host_path, was_staged, keep_temp_files)
        chunks = normalize_doc_parser_chunks(raw_chunks)
    else:
        chunks = _mock_chunks(file_path)
    return {
        "version_uid": parent.get("version_uid"),
        "law_id": parent.get("law_id"),
        "law_name": parent.get("law_name"),
        "law_abbr": parent.get("law_abbr"),
        "law_type": parent.get("law_type"),
        "provision_id": match_appendix_provision_id(parent, file_path) if parent else None,
        "file_name": file_path.name,
        "file_url": None,
        "unit_title": file_path.stem,
        "source_file_path": str(file_path),
        "chunks": chunks,
    }
