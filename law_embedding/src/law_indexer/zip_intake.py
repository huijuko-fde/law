"""ZIP 수신 1단계(LAW-40) — 법령 ZIP 을 해석·검증해 JSONL package record 로 바꾼다.

상대 측(또는 DMZ 수집기 `pipeline/common/zipbatch.py`)이 보낸 ZIP 안의 문서 JSON 을 **폴더 이름이
아니라 JSON 필드**(law_id·version_uid·doc_target·repealed)로 식별하고, 별표·원문·본문이미지 파일을
문서에 연결한 뒤, 기존 package 소비자(package.consume_package)가 읽는 record 로 변환한다.

ZIP 구조에 대한 가정은 두 가지뿐이다.
  · 최상위 폴더 `law/`·`admrul/` = package source(수집기 zipbatch.document_path 와 같다). 학칙·공단정관·
    공공기관(doc_target=school/pi/public)도 수집기처럼 `admrul/` 아래로 온다.
  · 첨부 파일은 **가장 가까운 상위 폴더**의 문서 JSON 중 그 파일명을 첨부 목록(appendices·attachments·
    조문 images)에 가진 문서에 붙는다. 그 아래 폴더 깊이·이름(법종구분별 `law/법률/형법/`, 수집기식
    `law/형법/시행령/`)은 보지 않는다. 후보가 둘 이상이면 추측하지 않고 오류로 낸다.

record 형식은 수집기 생산자(temporal_law pipeline/common/package.py 의 build_package_records +
base64_file_resolver / file_transfer_resolver)와 같다. 어떤 첨부를 file record 로 싣는지도 생산자
iter_file_units 규칙(is_file_only 별표 · 본문 없는 문서의 원문 · `[그림]` 본문이미지)을 따른다.

ZIP 옆 manifest(내부 수집기 zipbatch 형식)를 read_zip 에 주면, zipbatch 가 같은 이름·다른 내용 첨부를
`{stem}__{sha12}{ext}` 로 바꿔 실은 것을 manifest 의 attachment_files 로 원래 이름·URL 에 되돌려 연결한다.

소비(consume) 연결·CLI·처리 이력은 zip_consume.py(LAW-41)에 있다.
"""
import base64
import hashlib
import io
import json
import os
import re
import stat
import unicodedata
import uuid
import zipfile
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Dict, Iterator, List, Optional, Set, Tuple

from .config import Settings
from .mapper import _synthesize_admrul_version_uid, _text, map_admrul_data, map_law_data
from .package import ARTICLE_IMAGE, DELETE, DOCUMENT, FILE, FOOTER, HEADER, PENDING_ATTACHMENT
from .preprocess import (
    SUPPORTED_DOC_PARSER_SUFFIXES, _FORMAT_DEDUP_PRIORITY, _appendix_label, _doc_family,
    _doc_kind_of, _ext_of, _pick_file, _safe_name, document_is_file_only, find_pending_appendices,
    fit_staged_name,
)

# 수집기 zipbatch._ALLOWED(DMZ)와 같은 첨부 확장자 = preprocess.ATTACHMENT_SUFFIXES 에서 zip 을 뺀 것.
ZIP_ATTACHMENT_SUFFIXES = frozenset({
    ".hwp", ".hwpx", ".pdf", ".doc", ".docx", ".png", ".jpg", ".jpeg", ".gif",
})
# 최상위 폴더 = package source. 수집기 zipbatch.document_path 가 쓰는 두 값뿐이다.
ZIP_SOURCES = ("law", "admrul")
# JSON 이 가리키는 컬렉션 source → 그 문서가 실릴 package source(학칙공단은 admrul package 에 섞여 온다).
_PACKAGE_SOURCE = {"law": "law", "admrul": "admrul", "schlpub": "admrul"}
_LAW_TARGETS = ("eflaw", "law", "법령")                 # Settings.source_for_doc 의 법령 target 과 같다
_MANIFEST_NAMES = ("_manifest.json", "manifest.json")    # + `*.manifest.json`(zipbatch 사이드카)
_OS_JUNK_DIRS = ("__MACOSX",)
_OS_JUNK_NAMES = (".DS_Store", "Thumbs.db")
_UTF8_FLAG = 0x800                                       # ZIP 일반 플래그 bit 11 = 파일명 UTF-8
_READ_CHUNK = 1024 * 1024
APPENDIX = "APPENDIX"
DOCUMENT_FILE = "DOCUMENT_FILE"
# 멤버를 읽다 나는 오류 — 하나라도 나면 ZIP 이 깨진 것으로 보고 전체 실패시킨다.
_READ_ERRORS = (zipfile.BadZipFile, zlib.error, EOFError, OSError, RuntimeError,
                NotImplementedError, ValueError)
# 내부 수집기 zipbatch.stage_document 가 같은 경로·다른 내용 첨부에 붙이는 이름 `{stem}__{sha256[:12]}{ext}`.
_RENAMED_ATTACHMENT = re.compile(r"(?P<stem>.+)__(?P<sha>[0-9a-f]{12})(?P<ext>\.[^./]+)")
# 수집기 doc_uid = {doc_domain}:{doc_target}:{doc_id} (temporal_law_dmz pipeline/common/models.py
# make_doc_uid). doc_id 가 law_id 와 같은 것은 법령(law:eflaw)뿐이다 — 행정규칙류 doc_id 는 수집기
# 사본마다 다르다(DMZ: 연도 뺀 이름 키 `_group_key`, 내부: 행정규칙ID)라 law_id 로 쓸 수 없다.
_LAW_DOC_UID = re.compile(r"law:eflaw:([^:\s]+)")


class ZipIntakeError(Exception):
    """ZIP 자체를 읽을 수 없다(깨짐·CRC 불일치·암호화·미지원 압축) — 일부만 적재하지 않고 전체 실패."""


@dataclass
class Issue:
    """검증 결과 한 건. level 은 error(그 대상을 싣지 않음) / warning(싣되 알림)."""

    level: str
    code: str
    message: str
    path: Optional[str] = None          # ZIP 안 경로(정규화)
    law_id: Optional[str] = None


@dataclass
class ZipFileEntry:
    """ZIP 안의 첨부 파일 하나(아직 바이트는 읽지 않는다 — 변환 때 하나씩 읽는다)."""

    path: str                           # 정규화 경로(NFC, '/' 구분)
    member: str                         # ZipFile 이 아는 원래 이름(읽을 때 쓴다)
    name: str                           # 파일명(basename). manifest 가 이름을 바꿨다고 알려 주면 원래 이름
    size: int
    url: Optional[str] = None           # manifest(attachment_files)가 알려 준 원본 URL — 같은 이름 첨부 구분용


@dataclass
class FileUnit:
    """package 에 실을 파일 단위 — 수집기 package.iter_file_units 원소와 같은 뜻."""

    unit_type: str                      # APPENDIX | DOCUMENT_FILE | ARTICLE_IMAGE
    provision_id: Optional[str]
    file_name: Optional[str]
    file_url: Optional[str]
    names: Set[str]                     # ZIP 파일명과 맞춰 볼 후보
    article_no: Optional[str] = None    # ARTICLE_IMAGE 만 — 소비자가 `[그림]` 치환 위치를 찾는다
    image_seq: Optional[int] = None
    file: Optional[ZipFileEntry] = None
    file_id: str = ""
    unsupported_reason: Optional[str] = None
    urls: Set[str] = field(default_factory=set)   # JSON 의 원본 URL — manifest URL 과 맞춰 같은 이름 파일을 가른다


@dataclass
class ZipDocument:
    """ZIP 안 문서 JSON 하나의 식별·검증 결과. payload 는 들고 있지 않는다(변환 때 다시 읽는다)."""

    path: str
    member: str
    source: Optional[str]               # package source(최상위 폴더). 판정 불가면 None
    law_id: Optional[str] = None
    law_name: Optional[str] = None
    law_type: Optional[str] = None
    version_uid: Optional[str] = None   # 실효 version_uid — 행정규칙류는 mapper 와 같이 adm_uid 로 합성
    repealed: bool = False
    content_sha256: Optional[str] = None
    git_path: Optional[str] = None
    files: List[ZipFileEntry] = field(default_factory=list)   # 이 문서에 연결된 ZIP 파일 전부
    units: List[FileUnit] = field(default_factory=list)       # 그중 package 에 실을 단위
    errors: List[Issue] = field(default_factory=list)
    warnings: List[Issue] = field(default_factory=list)
    duplicate_of: Optional[str] = None  # 완전히 같은 JSON 이 먼저 있으면 그 경로(변환에서 뺀다)
    declared: Set[str] = field(default_factory=set, repr=False)   # JSON 이 가진 첨부 파일명 전부
    raw_version_uid: str = field(default="", repr=False)          # file_id 계산용(생산자와 같은 입력)

    @property
    def accepted(self) -> bool:
        return not self.errors and self.duplicate_of is None

    def error(self, code: str, message: str, path: Optional[str] = None) -> None:
        self.errors.append(Issue("error", code, message, path or self.path, self.law_id))

    def warn(self, code: str, message: str, path: Optional[str] = None) -> None:
        self.warnings.append(Issue("warning", code, message, path or self.path, self.law_id))


@dataclass
class ZipIntakeResult:
    """read_zip 결과. errors/warnings 는 문서에 귀속되지 않는 것(고아 첨부·경로·인코딩 등)이다."""

    zip_path: Path
    documents: List[ZipDocument] = field(default_factory=list)
    errors: List[Issue] = field(default_factory=list)
    warnings: List[Issue] = field(default_factory=list)

    def accepted(self, source: Optional[str] = None) -> List[ZipDocument]:
        """변환 대상 문서(오류 없고 중복 아님). source 를 주면 그 package source 것만."""
        return [d for d in self.documents if d.accepted and (source is None or d.source == source)]

    def summary(self) -> dict:
        accepted = self.accepted()
        return {
            "documents": len(self.documents),
            "accepted": len(accepted),
            "upserts": sum(1 for d in accepted if not d.repealed),
            "deletes": sum(1 for d in accepted if d.repealed),
            "rejected": sum(1 for d in self.documents if d.errors),
            "duplicates": sum(1 for d in self.documents if d.duplicate_of and not d.errors),
            "errors": len(self.errors) + sum(len(d.errors) for d in self.documents),
            "warnings": len(self.warnings) + sum(len(d.warnings) for d in self.documents),
        }


# ── 1. 해석·검증 ────────────────────────────────────────────────────────

def read_zip(zip_path: Path, *, manifest: Optional[dict] = None) -> ZipIntakeResult:
    """ZIP 을 해석·검증한다(아무것도 쓰지 않는다).

    문서 단위 오류는 결과에 모아 돌려주고 나머지 문서는 계속 처리한다. ZIP 자체를 못 읽으면
    (깨짐·CRC 불일치 등) ZipIntakeError — 깨진 묶음은 일부만 적재하지 않고 재전송 대상으로 둔다.
    첨부는 CRC 확인을 위해 끝까지 읽되 메모리에 모으지 않는다.

    manifest(ZIP 옆 `*.manifest.json` 을 읽은 dict)를 주면 그 attachment_files 로 이름이 바뀐 첨부를
    원래 이름에 되돌려 연결한다(대조 자체는 호출자 몫 — zip_consume)."""
    result = ZipIntakeResult(zip_path=Path(zip_path))
    listed = _manifest_attachments(manifest)
    try:
        archive = zipfile.ZipFile(zip_path)
    except (zipfile.BadZipFile, OSError, UnicodeDecodeError, ValueError) as exc:
        raise ZipIntakeError(f"ZIP 을 열 수 없습니다: {exc}") from exc
    with archive:
        json_members, file_members = _scan_members(archive, result)
        docs = [_read_document(archive, path, info, source) for path, info, source in json_members]
        files = []
        for path, info in file_members:
            _verify_member(archive, info)
            meta = listed.get(path) or {}
            url = meta.get("url")
            files.append(ZipFileEntry(path=path, member=info.filename,
                                      name=_original_name(path.rsplit("/", 1)[-1], meta),
                                      size=info.file_size,
                                      url=url.strip() if isinstance(url, str) and url.strip() else None))
    _check_law_id_groups(docs)
    _link_files(docs, files, result)
    _resolve_units(docs)
    _reject_incomplete_groups(docs)
    result.documents = docs
    return result


def _manifest_attachments(manifest: Optional[dict]) -> Dict[str, dict]:
    """manifest 의 changes[].attachment_files → {ZIP 안 경로(NFC): {url, path, sha256}}.

    내부 수집기 zipbatch.stage_document 가 문서마다 남기는 목록이다(DMZ 사본 manifest 에는 없다 —
    그때는 빈 dict 라 기존 이름 규칙만 쓴다)."""
    listed: Dict[str, dict] = {}
    if not isinstance(manifest, dict):
        return listed
    for change in _list(manifest.get("changes")):
        if not isinstance(change, dict):
            continue
        for item in _list(change.get("attachment_files")):
            if isinstance(item, dict) and isinstance(item.get("path"), str):
                listed[unicodedata.normalize("NFC", item["path"].replace("\\", "/"))] = item
    return listed


def _original_name(name: str, meta: dict) -> str:
    """manifest 가 이 파일을 `{stem}__{sha12}{ext}` 로 바꿔 실었다고 알려 주면 원래 이름, 아니면 그대로.

    파일 내용의 sha256 앞 12자리가 이름 꼬리와 같을 때만 되돌린다(zipbatch 가 이름을 만든 규칙 그대로)."""
    match = _RENAMED_ATTACHMENT.fullmatch(name)
    sha256 = meta.get("sha256")
    if match and isinstance(sha256, str) and sha256[:12].lower() == match.group("sha"):
        return match.group("stem") + match.group("ext")
    return name


_MAX_NESTED_ZIP_DEPTH = 3


def _nested_target(folder: str, name: str, taken: Set[str]) -> str:
    """펼친 멤버가 같은 디렉터리에서 겹치지 않는 경로. 파일명 255바이트 제한도 여기서 지킨다."""
    name = fit_staged_name(name)
    stem, suffix = PurePosixPath(name).stem, PurePosixPath(name).suffix
    candidate, seq = name, 0
    while (f"{folder}/{candidate}" if folder else candidate) in taken:
        seq += 1
        candidate = fit_staged_name(f"{stem}_{seq}{suffix}")
    path = f"{folder}/{candidate}" if folder else candidate
    taken.add(path)
    return path


def _expand_nested(blob: bytes, folder: str, stem: str, writer: zipfile.ZipFile,
                   taken: Set[str], depth: int) -> int:
    """중첩 ZIP 바이트를 열어 멤버를 writer 에 평탄하게 쓴다. 쓴 개수를 돌려준다."""
    if depth > _MAX_NESTED_ZIP_DEPTH:
        return 0
    written = 0
    with zipfile.ZipFile(io.BytesIO(blob)) as inner:
        for info in inner.infolist():
            if info.is_dir():
                continue
            name, problem = _member_path(info)
            if problem:                                  # 인코딩 불명·절대경로·zip slip
                continue
            parts = name.split("/")
            base = parts[-1]
            if not base or parts[0] in _OS_JUNK_DIRS or base in _OS_JUNK_NAMES:
                continue
            suffix = PurePosixPath(base).suffix.lower()
            if suffix == ".json":
                # 첨부 ZIP 안의 JSON 은 문서가 아니다. 넣으면 _scan_members 가 문서로 오인해
                # manifest 문서 수 대조가 틀어진다.
                continue
            if suffix == ".zip":
                written += _expand_nested(inner.read(info), folder,
                                          f"{stem}__{PurePosixPath(base).stem}", writer,
                                          taken, depth + 1)
                continue
            writer.writestr(_nested_target(folder, f"{stem}__{base}", taken), inner.read(info))
            written += 1
    return written


def flatten_nested_zips(zip_path: Path, out_path: Path) -> Optional[Path]:
    """배달 ZIP 안의 **첨부 ZIP** 을 풀어 같은 디렉터리에 평탄화한 새 ZIP 을 만든다.

    법령 사이트가 별표 묶음을 ZIP 으로 주는 경우가 있다(운영 실측: admrul 6건 안에 hwp/hwpx
    28건, 2단 중첩 2건 포함). `_scan_members` 는 `.zip` 멤버를 unsupported_extension 으로
    거부하므로 그대로 두면 내용물이 통째로 유실된다.

    zip_intake 는 멤버를 **원본 ZIP 안의 경로 문자열**로 식별하고 `iter_package_records` 가
    ZIP 을 다시 열어 `getinfo` 로 찾는다. 중첩 멤버는 그 체계에 끼워 넣을 수 없어서, 대신
    `read_zip` 에 넘기기 전에 펼쳐 평탄한 ZIP 하나로 만든다 — zip_intake 본체는 그대로 둔다.

        법령명/첨부파일/전문.zip 안의 `[별지 1] 평가서.hwp`
          → 법령명/첨부파일/전문__[별지 1] 평가서.hwp

    같은 디렉터리에 두므로 "가장 가까운 상위 폴더의 문서에 연결" 규칙이 유지된다. 첨부 ZIP 안의
    `.json` 은 문서가 아니라 제외한다(넣으면 문서 수 대조가 틀어진다).

    중첩 ZIP 이 없으면 None — 원본을 그대로 쓰라는 뜻이고 추가 I/O 가 없다. 못 읽는 중첩 ZIP 은
    원래 `.zip` 멤버를 그 자리에 남겨 `_scan_members` 가 평소대로 오류로 보고하게 둔다."""
    with zipfile.ZipFile(zip_path) as archive:
        nested = []
        for info in archive.infolist():
            if info.is_dir():
                continue
            name, problem = _member_path(info)
            if not problem and PurePosixPath(name).suffix.lower() == ".zip":
                nested.append((info, name))
        if not nested:
            return None
        nested_ids = {info.filename for info, _ in nested}
        out_path.parent.mkdir(parents=True, exist_ok=True)
        taken: Set[str] = set()
        with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as writer:
            for info in archive.infolist():              # 중첩이 아닌 멤버는 그대로 옮긴다
                if info.is_dir() or info.filename in nested_ids:
                    continue
                name, problem = _member_path(info)
                if problem:                              # 원본 그대로 남겨 _scan_members 가 보고하게 둔다
                    writer.writestr(info.filename, archive.read(info))
                    continue
                taken.add(name)
                writer.writestr(name, archive.read(info))
            for info, name in nested:                    # 중첩 ZIP 은 그 자리에 풀어 넣는다
                folder = name.rsplit("/", 1)[0] if "/" in name else ""
                blob = archive.read(info)
                try:
                    _expand_nested(blob, folder, PurePosixPath(name).stem, writer, taken, 1)
                except _READ_ERRORS:
                    writer.writestr(name, blob)          # 못 풀면 원래 멤버를 남긴다
    return out_path


def _member_path(info: zipfile.ZipInfo) -> Tuple[str, Optional[Tuple[str, str]]]:
    """ZipInfo → 정규화 경로(NFC·'/' 구분). 받을 수 없는 멤버면 (code, message) 를 함께 돌려준다.

    UTF-8 플래그가 없는 이름은 zipfile 이 cp437 로 읽어 한글이 깨진다 — 원래 바이트로 되돌려
    UTF-8(맥·리눅스 zip) → CP949(한국어 윈도우 압축기) 순으로 다시 읽는다. 맥에서 만든 이름은
    NFD(자모 분리)라 JSON 의 파일명(NFC)과 비교되도록 NFC 로 맞춘다."""
    name = info.filename
    if not info.flag_bits & _UTF8_FLAG:
        raw = name.encode("cp437")
        for encoding in ("utf-8", "cp949"):
            try:
                name = raw.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            return raw.decode("utf-8", "replace"), (
                "filename_encoding", "파일명 인코딩을 판별할 수 없습니다(UTF-8·CP949 아님)")
    name = unicodedata.normalize("NFC", name.replace("\\", "/"))
    if name.startswith("/") or re.match(r"[A-Za-z]:", name):
        return name, ("path_unsafe", "절대경로 멤버는 받지 않습니다")
    parts = [p for p in name.split("/") if p not in ("", ".")]
    if ".." in parts:
        return name, ("path_unsafe", "상위 경로(..)로 빠져나가는 멤버는 받지 않습니다")
    if stat.S_ISLNK(info.external_attr >> 16):
        return name, ("path_unsafe", "심볼릭 링크 멤버는 받지 않습니다")
    if not parts:
        return name, ("path_unsafe", "빈 경로 멤버는 받지 않습니다")
    return "/".join(parts), None


def _scan_members(archive: zipfile.ZipFile, result: ZipIntakeResult):
    """멤버를 문서 JSON·첨부로 나눈다. 경로·인코딩·확장자 문제는 결과 오류로 남기고 건너뛴다."""
    by_path: Dict[str, List[zipfile.ZipInfo]] = defaultdict(list)
    junk = 0
    for info in archive.infolist():
        if info.is_dir():
            continue
        path, problem = _member_path(info)
        if problem:
            result.errors.append(Issue("error", problem[0], problem[1], path=path))
            continue
        parts = path.split("/")
        if parts[0] in _OS_JUNK_DIRS or parts[-1] in _OS_JUNK_NAMES:
            junk += 1
            continue
        by_path[path].append(info)
    if junk:
        result.warnings.append(Issue("warning", "os_metadata_ignored",
                                     f"OS 메타 파일 {junk}건(__MACOSX·.DS_Store 등)을 무시했습니다"))

    json_members, file_members = [], []
    for path in sorted(by_path):
        infos = by_path[path]
        if len(infos) > 1:
            # 같은 경로가 둘이면 어느 쪽이 맞는지 모른다 — 둘 다 받지 않는다.
            result.errors.append(Issue("error", "path_duplicate",
                                       f"같은 경로의 멤버가 {len(infos)}개 있어 어느 것인지 정할 수 없습니다",
                                       path=path))
            continue
        name = path.rsplit("/", 1)[-1]
        suffix = PurePosixPath(name).suffix.lower()
        if suffix == ".json":
            if name in _MANIFEST_NAMES or name.endswith(".manifest.json"):
                result.warnings.append(Issue("warning", "manifest_ignored",
                                             "manifest 파일은 문서가 아니라 건너뜁니다", path=path))
                continue
            top = path.split("/", 1)[0] if "/" in path else None
            json_members.append((path, infos[0], top if top in ZIP_SOURCES else None))
        elif suffix in ZIP_ATTACHMENT_SUFFIXES:
            file_members.append((path, infos[0]))
        else:
            result.errors.append(Issue(
                "error", "unsupported_extension",
                f"지원하지 않는 확장자({suffix or '없음'}) — 첨부는 "
                f"{', '.join(sorted(s.lstrip('.') for s in ZIP_ATTACHMENT_SUFFIXES))} 만 받습니다",
                path=path))
    return json_members, file_members


def _read_bytes(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    try:
        return archive.read(info)
    except _READ_ERRORS as exc:
        raise ZipIntakeError(f"ZIP 멤버를 읽을 수 없습니다({info.filename}): {exc}") from exc


def _verify_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> None:
    """끝까지 읽어 CRC 를 확인한다(조각 단위 — 큰 첨부도 메모리에 올리지 않는다)."""
    try:
        with archive.open(info) as handle:
            while handle.read(_READ_CHUNK):
                pass
    except _READ_ERRORS as exc:
        raise ZipIntakeError(f"ZIP 멤버를 읽을 수 없습니다({info.filename}): {exc}") from exc


def _read_document(archive: zipfile.ZipFile, path: str, info: zipfile.ZipInfo,
                   source: Optional[str]) -> ZipDocument:
    """문서 JSON 하나를 읽어 JSON 필드로 식별하고 문서 단위 검증을 한다."""
    doc = ZipDocument(path=path, member=info.filename, source=source)
    try:
        payload = json.loads(_read_bytes(archive, info).decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        doc.error("json_invalid", f"JSON 파싱 실패: {exc}")
        return doc
    if not isinstance(payload, dict):
        doc.error("json_not_object", "JSON 최상위가 객체가 아닙니다")
        return doc

    # 거부될 문서라도 자기 첨부는 가져가게 한다 — 안 그러면 그 첨부가 고아 오류로 한 번 더 잡힌다.
    doc.declared = _declared_names(payload)
    doc.law_name = _text(payload.get("law_name"))
    doc.law_type = _text(payload.get("law_type"))
    doc.repealed = payload.get("repealed") is True
    doc.content_sha256 = _canonical_sha256(payload)

    law_id = payload.get("law_id")
    from_doc_uid = False
    if _blank(law_id) and doc.repealed:
        # 본문을 저장하지 않는 수집기(MANIFEST_ONLY)의 폐지 JSON 은 law_id 없이 doc_uid 만 있다
        # (temporal_law_dmz pipeline/common/activities.py git_remove_repealed).
        law_id = _law_id_from_doc_uid(payload.get("doc_uid"))
        from_doc_uid = law_id is not None
    if _blank(law_id):
        hint = ""
        if payload.get("doc_uid") is not None:
            hint = (f" — doc_uid({payload.get('doc_uid')!r})로도 정할 수 없습니다(폐지 JSON 의 doc_uid 는 "
                    "법령 `law:eflaw:<law_id>` 만 law_id 로 바꿉니다)")
        doc.error("law_id_missing", "law_id 가 없거나 비어 있습니다" + hint)
        return doc
    if not isinstance(law_id, str):
        # 숫자로 오면 앞자리 0(예: 001692)이 사라져 기존 청크와 다른 문서가 된다.
        doc.error("law_id_not_string", f"law_id 가 문자열이 아닙니다({type(law_id).__name__}: {law_id!r})")
        return doc
    doc.law_id = law_id.strip()

    if source is None:
        doc.error("source_unknown", "최상위 폴더가 law/ 또는 admrul/ 이 아니라 package source 를 정할 수 없습니다")
        return doc
    if from_doc_uid:
        if source != "law":
            doc.error("source_mismatch",
                      f"최상위 폴더 {source}/ 와 doc_uid({payload.get('doc_uid')!r})가 가리키는 source(law)가 다릅니다")
            return doc
        doc.warn("law_id_from_doc_uid",
                 f"law_id 없는 폐지 JSON 이라 doc_uid({payload.get('doc_uid')!r})에서 law_id 를 꺼내 삭제로 싣습니다")
        return doc                                       # 폐지 — delete record 만 싣는다
    implied = _json_source(payload, doc.law_id)
    if implied == "?":
        doc.error("doc_target_unknown", f"알 수 없는 doc_target 입니다({payload.get('doc_target')!r})")
        return doc
    if implied and _PACKAGE_SOURCE[implied] != source:
        doc.error("source_mismatch",
                  f"최상위 폴더 {source}/ 와 JSON(doc_target={payload.get('doc_target')!r})이 "
                  f"가리키는 source({implied})가 다릅니다")
        return doc
    if doc.repealed:
        return doc                                       # 폐지는 delete record 만 — law_id 면 충분

    doc.raw_version_uid = str(payload.get("version_uid") or "")
    doc.version_uid = _text(payload.get("version_uid")) or (
        _synthesize_admrul_version_uid(payload) if source == "admrul" else None)
    doc.git_path = _git_path(payload)
    # 소비자 mapper 를 그대로 한 번 돌려 본다(임베딩 없음). 여기서 깨질 문서를 package 에 실으면
    # 소비 때 그 package 전체가 오류로 재시도·격리된다 — 문서 하나 때문에 묶음을 막지 않게 미리 거른다.
    mapper_fn = map_law_data if source == "law" else map_admrul_data
    try:
        mapper_fn(payload, Path(doc.git_path or f"package:{doc.law_id}"))
    except Exception as exc:
        doc.error("mapping_failed", f"소비자 mapper 가 이 JSON 을 적재 형식으로 바꾸지 못합니다: {exc}")
        return doc
    doc.units = _file_units(payload)
    return doc


def _blank(value) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _law_id_from_doc_uid(doc_uid) -> Optional[str]:
    """수집기 doc_uid `law:eflaw:<law_id>` → law_id. 다른 형식(행정규칙류 포함)이면 None."""
    if not isinstance(doc_uid, str):
        return None
    match = _LAW_DOC_UID.fullmatch(doc_uid.strip())
    return match.group(1) if match else None


def _json_source(payload: dict, law_id: Optional[str]) -> Optional[str]:
    """JSON 필드가 가리키는 컬렉션 source(law/admrul/schlpub) — Settings.source_for_doc 과 같은 분류.

    doc_target 이 우선이고 없으면 law_id 접두("school:…")를 본다. 신호가 없으면 None(모순 판정 안 함),
    모르는 doc_target 이면 "?" — source_for_doc 은 모르는 값을 기본값으로 접지만 수신 검증에서는
    어느 컬렉션인지 확정할 수 없으므로 오류로 낸다. law_type 은 판정 규칙이 기존 코드에 없어 보지 않는다."""
    target = _text(payload.get("doc_target"))
    if target:
        if target in Settings.SCHLPUB_TARGETS:
            return "schlpub"
        if target in _LAW_TARGETS:
            return "law"
        if target == "admrul":
            return "admrul"
        return "?"
    if law_id and law_id.startswith(Settings._SCHLPUB_ID_PREFIXES):
        return "schlpub"
    return None


def _canonical_sha256(payload: dict) -> str:
    """들여쓰기·키 순서와 무관한 내용 해시 — 같은 버전 JSON 이 중복인지 충돌인지 가른다."""
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _git_path(payload: dict) -> Optional[str]:
    """document record 의 git_path(데이터 레포 상대경로).

    생산자(payloads.repo_rel_json_path)처럼 payload 에 git_path 가 있으면 그대로, 없으면 현재 레포
    레이아웃(preprocess.document_dir_candidates 의 첫 후보)으로 만든다 — 법령 `{패밀리}/{법률|시행령|
    시행규칙}/{문서}.json`, 행정규칙류 `{문서명}/{문서명}.json`. ZIP 안 경로는 레포 레이아웃이 아니라
    쓰지 않는다(미러가 켜져 있으면 엉뚱한 위치에 쓰게 된다)."""
    given = payload.get("git_path")
    if isinstance(given, str) and given.strip():
        return given.strip()
    name = _text(payload.get("law_name"))
    if not name:
        return None
    file_name = f"{_safe_name(name)}.json"
    if payload.get("doc_target") in ("admrul", *Settings.SCHLPUB_TARGETS):
        return f"{_safe_name(name)}/{file_name}"
    return f"{_safe_name(_doc_family(name))}/{_doc_kind_of(name)}/{file_name}"


# ── 첨부 파일명 (수집기 저장 규칙 — preprocess 의 역산 함수 재사용) ───────────

def _name_variants(value) -> Set[str]:
    """JSON 파일명 필드 → ZIP 파일명과 비교할 후보(값 그대로 + 수집기 저장 규칙 _safe 적용, NFC)."""
    if not isinstance(value, str) or not value.strip():
        return set()
    return {unicodedata.normalize("NFC", v) for v in (value.strip(), _safe_name(value))}


def _appendix_names(appendix: dict) -> Set[str]:
    """별표/별지/서식 파일명 — `filename`(수집기 enrich_file_meta 가 심은 저장 이름)을 쓰고, 없을 때만
    수집기와 같은 규칙({종류}{번호}_{제목}.{확장자})으로 다시 만든다(appendix_local_path 폴백과 같다)."""
    names = _name_variants(appendix.get("filename"))
    if not names:
        picked = _pick_file([f for f in appendix.get("files") or [] if isinstance(f, dict)])
        if picked:
            names = _name_variants(f"{_appendix_label(appendix)}.{_ext_of(picked)}")
    return names


def _attachment_names(attachment: dict) -> Set[str]:
    """원문 첨부 — zipbatch 는 name(없으면 filename)을 _safe 해 둔다. 둘 다 후보로 본다."""
    return _name_variants(attachment.get("name")) | _name_variants(attachment.get("filename"))


def _image_names(article: dict, seq: int, image: dict) -> Set[str]:
    """본문이미지 — `filename` 우선, 없으면 수집기 image_fname 규칙({조문번호}_{순번}.{확장자})."""
    names = _name_variants(image.get("filename"))
    if not names:
        base = _safe_name(str(article.get("article_no") or "본문").replace(" ", ""))
        names = _name_variants(f"{base}_{seq}.{_ext_of(image, 'gif')}")
    return names


def _list(value) -> list:
    return value if isinstance(value, list) else []


def _articles(payload: dict) -> List[dict]:
    body = payload.get("body")
    return [a for a in _list(body.get("articles") if isinstance(body, dict) else None)
            if isinstance(a, dict)]


def _declared_names(payload: dict) -> Set[str]:
    """JSON 이 언급하는 첨부 파일명 전부 — 첨부를 문서에 연결하는 기준이다(싣는지와는 별개)."""
    names: Set[str] = set()
    for appendix in _list(payload.get("appendices")):
        if isinstance(appendix, dict):
            names |= _appendix_names(appendix)
    for attachment in _list(payload.get("attachments")):
        if isinstance(attachment, dict):
            names |= _attachment_names(attachment)
    for article in _articles(payload):
        for seq, image in enumerate(_list(article.get("images")), 1):
            if isinstance(image, dict):
                names |= _image_names(article, seq, image)
    return names


def _dedup_document_attachments(attachments: list) -> List[dict]:
    """같은 basename 의 형식 재수출(X.hwp + X.pdf)은 hwpx>hwp>docx>pdf 하나만 남긴다 — 수집기
    package._dedup_document_attachments·소비자 resolve_pending_document_files 와 같은 우선순위."""
    groups: Dict[str, dict] = {}
    others: List[dict] = []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        name = str(attachment.get("filename") or attachment.get("name") or "")
        ext = PurePosixPath(name).suffix.lower()
        if ext not in _FORMAT_DEDUP_PRIORITY:
            others.append(attachment)
            continue
        key = PurePosixPath(name).stem.lower()
        kept = groups.get(key)
        if kept is not None:
            kept_name = str(kept.get("filename") or kept.get("name") or "")
            kept_ext = PurePosixPath(kept_name).suffix.lower()
            if _FORMAT_DEDUP_PRIORITY.index(ext) >= _FORMAT_DEDUP_PRIORITY.index(kept_ext):
                continue
        groups[key] = attachment
    return list(groups.values()) + others


def _file_units(payload: dict) -> List[FileUnit]:
    """package 에 file record 로 실을 단위 — 수집기 package.iter_file_units 와 같은 규칙.

    ① is_file_only 별표/별지/서식 ② 본문 텍스트가 없는 문서의 원문 attachments(형식 재수출은 하나)
    ③ `[그림]` 마커가 있는 조문의 본문이미지. 나머지 파일(본문 텍스트가 있는 별표 등)은 JSON 에 이미
    내용이 있어 싣지 않는다 — 실으면 같은 내용이 FILE 청크로 한 번 더 들어간다."""
    units = []
    for appendix in find_pending_appendices(payload):
        units.append(FileUnit(APPENDIX, appendix.get("provision_id"), appendix.get("filename"),
                              appendix.get("file_url"), _appendix_names(appendix),
                              urls=_urls(appendix.get("file_url"),
                                         *(f.get("url") for f in _list(appendix.get("files"))
                                           if isinstance(f, dict)))))
    if document_is_file_only(payload):
        for attachment in _dedup_document_attachments(_list(payload.get("attachments"))):
            units.append(FileUnit(DOCUMENT_FILE, None,
                                  attachment.get("filename") or attachment.get("name"),
                                  attachment.get("url"), _attachment_names(attachment),
                                  urls=_urls(attachment.get("url"), attachment.get("file_url"))))
    for article in _articles(payload):
        images = _list(article.get("images"))
        if not images or "[그림]" not in str(article.get("content") or ""):
            continue                                     # 마커 없는 이미지는 치환 대상이 아니다
        article_no = str(article.get("article_no") or "")
        for seq, image in enumerate(images, 1):
            if isinstance(image, dict):
                units.append(FileUnit(ARTICLE_IMAGE, article.get("provision_id"), image.get("filename"),
                                      image.get("url"), _image_names(article, seq, image),
                                      article_no=article_no, image_seq=seq, urls=_urls(image.get("url"))))
    return units


def _urls(*values) -> Set[str]:
    """첨부 단위의 원본 URL 후보 — zipbatch 가 manifest 에 남기는 url(대표 파일 url 또는 file_url)과 맞춘다."""
    return {v.strip() for v in values if isinstance(v, str) and v.strip()}


def _file_id(raw_version_uid: str, file_name: Optional[str], provision_id: Optional[str]) -> str:
    """수집기 package._file_id 와 같은 결정적 file_id — 같은 문서면 어느 경로로 와도 청크 id 가 같다."""
    basis = "\x1f".join(str(x or "") for x in (raw_version_uid, provision_id, file_name))
    return "f" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]


# ── law_id 묶음 · 첨부 연결 ─────────────────────────────────────────────

def _groups(docs: List[ZipDocument]) -> Dict[str, List[ZipDocument]]:
    groups: Dict[str, List[ZipDocument]] = defaultdict(list)
    for doc in docs:
        if doc.law_id:
            groups[doc.law_id].append(doc)
    return groups


def _check_law_id_groups(docs: List[ZipDocument]) -> None:
    """같은 law_id 가 여러 JSON 에 나올 때: 같은 version_uid 면 내용이 같을 때 중복(하나만), 다를 때
    충돌로 거부한다. 폐지·비폐지가 섞이면 어느 쪽인지 모른다.

    version_uid 가 다르면(현행 + 시행예정 등) **그 law_id 전체를 거부한다(임시 정책)**. 소비자는 law_id
    당 한 버전만 보관한다(package._LawBuffer.document 하나 · _flush_law 의 keep_vuids 정리) — 둘을 한
    package 에 실으면 마지막 버전만 남고 나머지는 지워진다. 현행·시행예정본을 함께 보관할지는 기획
    결정 대기라, 그 전까지 어느 한 버전을 골라 싣지 않고 사유를 남겨 뺀다."""
    for group in _groups(docs).values():
        if any(doc.errors for doc in group):
            continue                     # 이미 거부된 JSON 이 있으면 law_id 전체가 빠진다(_reject_incomplete_groups)
        sources = {doc.source for doc in group}
        if len(sources) > 1:
            for doc in group:
                doc.error("law_id_multi_source",
                          f"같은 law_id 가 여러 source({', '.join(sorted(str(s) for s in sources))})에 있습니다")
            continue
        repealed = [doc for doc in group if doc.repealed]
        if repealed and len(repealed) < len(group):
            for doc in group:
                doc.error("repeal_conflict",
                          "같은 law_id 에 폐지 JSON 과 일반 JSON 이 함께 있어 삭제인지 적재인지 정할 수 없습니다")
            continue
        if repealed:
            for doc in repealed[1:]:
                doc.duplicate_of = repealed[0].path
                doc.warn("duplicate", f"같은 law_id 의 폐지 JSON 이 이미 있습니다({repealed[0].path}) — 삭제는 한 번만 싣습니다")
            continue
        by_version: Dict[Optional[str], List[ZipDocument]] = defaultdict(list)
        for doc in group:
            by_version[doc.version_uid].append(doc)
        if len(by_version) > 1:
            versions = ", ".join(sorted(str(v) for v in by_version))
            for doc in group:
                doc.error("multi_version_unsupported",
                          f"같은 law_id 의 버전 {len(by_version)}개({versions})가 한 ZIP 에 있습니다 — 적재기는 "
                          "law_id 당 한 버전만 보관합니다. 현행·시행예정 동시 보관 여부 기획 결정 전이라 "
                          "이 law_id 는 적재하지 않습니다")
            continue
        for version_uid, same in by_version.items():
            first = same[0]
            if any(doc.content_sha256 != first.content_sha256 for doc in same[1:]):
                for doc in same:
                    doc.error("version_conflict",
                              f"같은 law_id·version_uid({version_uid}) 인데 내용이 다른 JSON 이 {len(same)}개 있습니다")
                continue
            for doc in same[1:]:
                doc.duplicate_of = first.path
                doc.warn("duplicate", f"완전히 같은 JSON 이 이미 있습니다({first.path}) — 한 번만 싣습니다")


def _ancestors(path: str) -> Iterator[str]:
    """'a/b/f.hwp' → 'a/b', 'a', '' (가까운 폴더부터)."""
    parts = path.split("/")[:-1]
    for end in range(len(parts), -1, -1):
        yield "/".join(parts[:end])


def _link_files(docs: List[ZipDocument], files: List[ZipFileEntry], result: ZipIntakeResult) -> None:
    """첨부 파일을 문서에 연결한다 — 가장 가까운 상위 폴더의 문서 JSON 중 그 파일명을 첨부 목록에
    가진 문서. 한 폴더에 여러 문서(수집기 구조의 시행규칙 여럿 등)가 있어도 JSON 목록이 가른다.
    목록에 가진 문서가 없으면 고아, 서로 다른 문서 둘 이상이면 모호 — 어느 쪽도 추측하지 않는다."""
    by_dir: Dict[str, List[ZipDocument]] = defaultdict(list)
    for doc in docs:
        by_dir[doc.path.rsplit("/", 1)[0] if "/" in doc.path else ""].append(doc)
    for entry in files:
        folder = next((p for p in _ancestors(entry.path) if p in by_dir), None)
        if folder is None:
            result.errors.append(Issue("error", "attachment_orphan",
                                       "상위 폴더에 문서 JSON 이 없어 연결할 문서가 없습니다", path=entry.path))
            continue
        candidates = [doc for doc in by_dir[folder] if entry.name in doc.declared]
        if not candidates:
            listed = ", ".join(doc.path.rsplit("/", 1)[-1] for doc in by_dir[folder])
            result.errors.append(Issue(
                "error", "attachment_orphan",
                f"가까운 문서 폴더({folder or '/'})의 JSON({listed}) 첨부 목록에 이 파일명이 없습니다",
                path=entry.path))
            continue
        # 내용까지 같은 JSON(중복)끼리는 소속이 같다 — 서로 다른 문서일 때만 모호하다.
        if len({(doc.law_id, doc.content_sha256) for doc in candidates}) > 1:
            listed = ", ".join(doc.path for doc in candidates)
            for doc in candidates:
                doc.error("attachment_ambiguous",
                          f"첨부 {entry.path} 를 여러 문서({listed})가 첨부 목록에 갖고 있어 소속을 정할 수 없습니다",
                          path=entry.path)
            continue
        candidates[0].files.append(entry)


def _resolve_units(docs: List[ZipDocument]) -> None:
    """문서의 파일 단위마다 연결된 ZIP 파일을 찾는다. 없으면 pending_attachment 로 실린다(경고)."""
    for doc in docs:
        if doc.duplicate_of:
            continue                                     # 싣지 않는 중복 JSON
        if doc.repealed:
            if doc.files:
                doc.warn("repealed_with_files",
                         f"폐지 문서에 첨부 {len(doc.files)}건이 함께 왔습니다 — 삭제만 싣고 첨부는 싣지 않습니다")
            continue
        by_name: Dict[str, List[ZipFileEntry]] = defaultdict(list)
        for entry in doc.files:
            by_name[entry.name].append(entry)
        for unit in doc.units:
            matches = {e.path: e for name in unit.names for e in by_name.get(name, [])}
            if len(matches) > 1 and unit.urls:
                # 내부 수집기 zipbatch 가 같은 이름·다른 내용 첨부를 `{stem}__{sha12}` 로 바꿔 실었다 —
                # 원래 이름이 같은 파일들 중 manifest 가 알려 준 URL 이 이 단위의 URL 인 것을 고른다.
                by_url = {path: e for path, e in matches.items() if e.url in unit.urls}
                if len(by_url) == 1:
                    matches = by_url
            if len(matches) > 1:
                doc.error("attachment_ambiguous",
                          f"첨부 {unit.file_name or unit.provision_id} 에 맞는 파일이 ZIP 안 여러 경로"
                          f"({', '.join(sorted(matches))})에 있습니다")
                continue
            unit.file = next(iter(matches.values()), None)
            unit.file_name = unit.file_name or (unit.file.name if unit.file else None)
            if not unit.file_name and unit.unit_type == ARTICLE_IMAGE:
                unit.file_name = f"{unit.article_no}_{unit.image_seq}.gif"     # 생산자 기본값과 같다
            unit.file_id = _file_id(doc.raw_version_uid, unit.file_name, unit.provision_id)
            ext = PurePosixPath(unit.file_name or "").suffix.lower()
            if unit.unit_type != ARTICLE_IMAGE and ext and ext not in SUPPORTED_DOC_PARSER_SUFFIXES:
                # 생산자와 같은 사유 문자열 — 전처리기가 못 읽는 형식(.doc 등)은 보류로 남긴다.
                unit.unsupported_reason = f"unsupported_file_extension:{ext.lstrip('.')}"
                doc.warn("attachment_unsupported",
                         f"첨부 {unit.file_name} 는 전처리기가 읽지 못하는 형식이라 pending_attachment 로 싣습니다")
            elif unit.file is None:
                doc.warn("attachment_missing",
                         f"JSON 의 파일 전용 첨부({unit.file_name or unit.provision_id})가 ZIP 에 없습니다 "
                         "— pending_attachment 로 싣습니다")


def _reject_incomplete_groups(docs: List[ZipDocument]) -> None:
    """한 law_id 의 JSON 중 하나라도 거부되면 그 law_id 를 통째로 뺀다.

    소비자는 package 에 온 버전 외의 옛 버전 청크를 지운다(_flush_law → delete_stale_law_chunks).
    일부 버전만 실으면 빠진 버전이 VDB 에서 지워지므로, law_id 단위로 전부 싣거나 전부 뺀다."""
    for group in _groups(docs).values():
        bad = [doc for doc in group if doc.errors]
        if not bad:
            continue
        for doc in group:
            if not doc.errors:
                doc.error("group_rejected",
                          f"같은 law_id 의 다른 JSON({', '.join(b.path for b in bad)})이 거부돼 함께 제외합니다")


# ── 2. package 변환 ─────────────────────────────────────────────────────

def _stamp(source: str) -> Tuple[str, str]:
    """package_id·generated_at — 생산자 handoff._stamp 와 같은 형식(같은 초 충돌 방지 무작위 접미)."""
    now = datetime.now(timezone.utc)
    return (f"{source}-{now.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}",
            now.strftime("%Y-%m-%dT%H:%M:%SZ"))


def iter_package_records(result: ZipIntakeResult, source: str, *,
                         package_id: Optional[str] = None, generated_at: Optional[str] = None,
                         file_stage_dir: Optional[Path] = None) -> Iterator[dict]:
    """검증을 통과한 source 문서를 package record 로 하나씩 낸다.

    순서는 생산자 build_package_records 와 같다: header → (document + 그 문서의 file/pending)* →
    delete* → footer. footer.record_count = header/footer 를 뺀 record 수(소비자가 대조한다).
    첨부는 기본 content_b64 인라인(생산자 base64 모드 — JSONL 자기완결), file_stage_dir 를 주면 그
    폴더에 `{file_id}{확장자}` 로 풀고 transfer_name 을 싣는다(file_transfer 모드 — 소비자
    PACKAGE_FILE_INBOX_DIR 와 같은 폴더여야 한다). payload·첨부는 하나씩 ZIP 에서 다시 읽는다."""
    if source not in ZIP_SOURCES:
        raise ValueError(f"source 는 {ZIP_SOURCES} 중 하나여야 합니다: {source!r}")
    if package_id is None or generated_at is None:
        stamped_id, stamped_at = _stamp(source)
        package_id, generated_at = package_id or stamped_id, generated_at or stamped_at
    stage = Path(file_stage_dir) if file_stage_dir else None
    if stage is not None:
        stage.mkdir(parents=True, exist_ok=True)
    docs = result.accepted(source)
    yield {"record_type": HEADER, "package_id": package_id, "source": source, "mode": "delta",
           "strategy": "file_transfer" if stage is not None else "base64",
           "generated_at": generated_at}
    count = 0
    try:
        archive = zipfile.ZipFile(result.zip_path)
    except (zipfile.BadZipFile, OSError, UnicodeDecodeError, ValueError) as exc:
        raise ZipIntakeError(f"ZIP 을 열 수 없습니다: {exc}") from exc
    with archive:
        for doc in docs:
            if doc.repealed:
                continue
            payload = _load_payload(archive, doc)
            record = {"record_type": DOCUMENT, "op": "upsert", "law_id": doc.law_id,
                      "version_uid": payload.get("version_uid"), "payload": payload}
            if doc.git_path:
                record["git_path"] = doc.git_path
            yield record
            count += 1
            for unit in doc.units:
                yield _unit_record(archive, doc, unit, stage)
                count += 1
    for doc in docs:
        if doc.repealed:
            yield {"record_type": DELETE, "law_id": doc.law_id}
            count += 1
    yield {"record_type": FOOTER, "package_id": package_id, "record_count": count}


def _load_payload(archive: zipfile.ZipFile, doc: ZipDocument) -> dict:
    try:
        info = archive.getinfo(doc.member)
    except KeyError as exc:
        raise ZipIntakeError(f"검증 후 ZIP 이 바뀌었습니다(멤버 없음: {doc.path})") from exc
    payload = json.loads(_read_bytes(archive, info).decode("utf-8-sig"))
    if _canonical_sha256(payload) != doc.content_sha256:
        raise ZipIntakeError(f"검증 후 ZIP 이 바뀌었습니다(내용 불일치: {doc.path}) — read_zip 부터 다시 하세요")
    return payload


def _unit_record(archive: zipfile.ZipFile, doc: ZipDocument, unit: FileUnit,
                 stage: Optional[Path]) -> dict:
    """파일 단위 → file record(파일 있음) 또는 pending_attachment(없음·미지원). 필드는 생산자와 같다."""
    locator = ({"article_no": unit.article_no, "image_seq": unit.image_seq}
               if unit.unit_type == ARTICLE_IMAGE else {})
    if unit.unsupported_reason or unit.file is None:
        reason = unit.unsupported_reason or (
            "file_not_found_for_transfer" if stage is not None else "file_not_found_for_base64")
        return {"record_type": PENDING_ATTACHMENT, "law_id": doc.law_id, "file_id": unit.file_id,
                "file_name": unit.file_name, "file_url": unit.file_url,
                "provision_id": unit.provision_id, "unit_type": unit.unit_type, "reason": reason,
                **locator}
    info = archive.getinfo(unit.file.member)
    if stage is None:
        data = _read_bytes(archive, info)
        return {"record_type": FILE, "law_id": doc.law_id, "file_id": unit.file_id,
                "file_name": unit.file_name,
                "content_b64": base64.b64encode(data).decode("ascii"),
                "sha256": hashlib.sha256(data).hexdigest(),
                "provision_id": unit.provision_id, "unit_type": unit.unit_type,
                "file_url": unit.file_url, **locator}
    transfer_name = f"{unit.file_id}{PurePosixPath(unit.file_name or unit.file.name).suffix}"
    sha256 = _copy_member(archive, info, stage / transfer_name)
    return {"record_type": FILE, "law_id": doc.law_id, "file_id": unit.file_id,
            "file_name": unit.file_name, "transfer_name": transfer_name,
            "provision_id": unit.provision_id, "unit_type": unit.unit_type,
            "file_url": unit.file_url, "sha256": sha256, **locator}


def _copy_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo, target: Path) -> str:
    """멤버를 조각 단위로 풀어 쓰고 sha256 을 돌려준다(.tmp → 이름 바꾸기 — 반쯤 쓴 파일 노출 방지)."""
    digest = hashlib.sha256()
    tmp = target.with_name(target.name + ".tmp")
    try:
        with archive.open(info) as src, open(tmp, "wb") as dst:
            for chunk in iter(lambda: src.read(_READ_CHUNK), b""):
                digest.update(chunk)
                dst.write(chunk)
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return digest.hexdigest()


def write_packages(result: ZipIntakeResult, out_dir: Path, *,
                   file_stage_dir: Optional[Path] = None) -> List[dict]:
    """검증 결과를 source 별 JSONL package 파일로 쓴다 — law·admrul 이 섞인 ZIP 이면 파일 두 개.

    생산자 write_jsonl 처럼 `.tmp` 에 쓰고 fsync 후 이름을 바꾼다(소비자의 *.jsonl glob 에 반쯤 쓴
    파일이 안 걸린다). record 를 하나씩 써서 첨부를 한꺼번에 메모리에 올리지 않는다. 실을 문서가
    없는 source 는 만들지 않는다. 반환: source 별 {source, path, package_id, 건수}."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for source in ZIP_SOURCES:
        if not result.accepted(source):
            continue
        package_id, generated_at = _stamp(source)
        path = out_dir / f"{package_id}.jsonl"
        tmp = path.with_name(path.name + ".tmp")
        counts: Dict[str, int] = defaultdict(int)
        try:
            with open(tmp, "w", encoding="utf-8") as handle:
                for record in iter_package_records(result, source, package_id=package_id,
                                                   generated_at=generated_at,
                                                   file_stage_dir=file_stage_dir):
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                    counts[record["record_type"]] += 1
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)
        written.append({"source": source, "path": str(path), "package_id": package_id,
                        "documents": counts[DOCUMENT], "deletes": counts[DELETE],
                        "files": counts[FILE], "pending_attachments": counts[PENDING_ATTACHMENT],
                        "records": sum(counts.values())})
    return written
