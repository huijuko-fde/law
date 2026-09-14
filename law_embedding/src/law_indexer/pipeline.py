import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, List, Optional

from .config import Settings
from .embedder import LocalEmbedder
from .mapper import (
    build_attachment_provisions, load_law_json, map_admrul_data,
    map_attachment_data, map_law_data, object_uuid,
)
from .models import LegalProvision
from .package import (_RoutedStores, consume_package, is_package_file,
                      peek_package_law_ids, peek_package_source)
from .preprocess import (
    ATTACHMENT_SUFFIXES, DocParserClient, DocParserError, appendix_local_path,
    is_permanent_parser_error,
    document_is_file_only, file_sha256, find_parent_law, find_pending_appendices,
    preprocess_file, resolve_article_images, resolve_pending_document_files, run_doc_parser,
)
from .weaviate_store import WeaviateStore

logger = logging.getLogger("law_indexer.pipeline")

# 수집기 미러에 섞여 있는, 조문이 아닌 JSON (스킵)
SKIP_JSON_NAMES = {"_manifest.json"}


def _new_totals() -> dict:
    """파일/객체/첨부 처리 성공·실패 수를 누적할 기본 결과 dict를 만든다."""
    return {
        "files_success": 0, "files_failed": 0, "files_skipped": 0,
        "objects_success": 0, "objects_failed": 0,
        "attachments_success": 0, "attachments_failed": 0, "attachments_skipped": 0,
        "images_success": 0, "images_failed": 0,
        "vectors_reused": 0,
        "errors": [],
    }


def discover(path: Path, recursive: bool, limit: Optional[int],
             suffixes: Iterable[str], skip_names: Iterable[str] = ()) -> List[Path]:
    """입력 파일 또는 디렉터리에서 색인 대상 확장자 파일 목록을 찾는다."""
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(f"입력 경로가 없습니다: {path}")
    suffixes, skip_names = set(suffixes), set(skip_names)
    it = path.rglob("*") if recursive else path.glob("*")
    files = sorted(p for p in it if p.is_file() and p.suffix.lower() in suffixes and p.name not in skip_names)
    return files[:limit] if limit is not None else files


class VectorCache:
    """migrate_collection export(parquet: uuid·vector·props)를 임베딩 재사용 캐시로 쓴다.

    같은 chunk(uuid)이고 search_text 가 그대로면(해시 일치) 임베딩 호출 없이 옛 벡터를
    재사용한다 — 재적재에서 안 바뀐 조문의 임베딩 비용을 0 으로. search_text 가 다르면
    (개정·매핑 규칙 변화) 미스 → 새로 임베딩. **속성·meta 는 항상 새로 쓰므로**(upsert)
    relation enrich 등 meta 갱신은 캐시 히트와 무관하게 반영된다."""

    def __init__(self, path: Path):
        import hashlib
        import numpy as np
        import pyarrow.parquet as pq
        # ⚠ 스트리밍 로드 — 종전엔 vector 컬럼을 to_pylist() 로 통째 파이썬 리스트로 펼쳤는데,
        #   37.5만×1024 float 이 파이썬 객체로 ~12GB+ 가 되어 16GiB 컨테이너(cgroup)에서
        #   **OOM SIGKILL 로 조용히 죽었다**(실측 — 트레이스백조차 없음, do_export OOM 과 동일 패턴).
        #   배치 단위로 읽고 벡터는 arrow→numpy 로 직행(파이썬 리스트 미경유), props 는 해시만
        #   남기고 버린다. 상주 메모리 = float32 행렬 ~1.5GB + 해시/행맵 수십 MB.
        pf = pq.ParquetFile(str(path))
        self._row: dict = {}
        self._hash: list = []
        vec_parts: list = []
        dim = 0
        i = 0
        for batch in pf.iter_batches(columns=["uuid", "vector", "props"], batch_size=8192):
            uuids = batch.column(0).to_pylist()
            props = batch.column(2).to_pylist()
            n = len(uuids)
            flat = batch.column(1).flatten()            # list<float> → 값 배열(오프셋 반영)
            arr = np.asarray(flat.to_numpy(zero_copy_only=False), dtype=np.float32)
            if n and not dim:
                dim = arr.size // n
            if n:
                vec_parts.append(arr.reshape(n, dim))
            for u, pj in zip(uuids, props):
                self._row[str(u)] = i
                try:
                    st = (json.loads(pj) or {}).get("search_text") or ""
                except (TypeError, json.JSONDecodeError):
                    st = ""
                self._hash.append(hashlib.sha256(st.encode("utf-8")).digest())
                i += 1
        self._dim = dim
        self._vecs = (np.concatenate(vec_parts, axis=0) if vec_parts
                      else np.zeros((0, 0), dtype=np.float32))
        logger.info("벡터 캐시 로드: %s (%d청크, %d차원)", path, len(self._row), self._dim)

    def get(self, uuid: str, search_text: str) -> Optional[List[float]]:
        import hashlib
        i = self._row.get(uuid)
        if i is None:
            return None
        if self._hash[i] != hashlib.sha256((search_text or "").encode("utf-8")).digest():
            return None
        return self._vecs[i].tolist()


def _embed_and_store(store: WeaviateStore, embedder: LocalEmbedder, objects: List[LegalProvision],
                     collection: str, cache: Optional[VectorCache] = None) -> dict:
    """LegalProvision 목록에 임베딩 벡터를 채운 뒤 지정한 컬렉션에 upsert한다.
    cache 가 있으면 (uuid, search_text) 일치 청크는 임베딩을 건너뛰고 옛 벡터를 재사용한다."""
    misses = list(objects)
    reused = 0
    if cache is not None:
        misses = []
        for obj in objects:
            vec = cache.get(object_uuid(obj.chunk_id), obj.search_text)
            if vec is not None and len(vec) == embedder.dimension:
                obj.vector = vec
                obj.embedding_model = embedder.model_name
                obj.embedding_dimension = embedder.dimension
                reused += 1
            else:
                misses.append(obj)
    if misses:
        vectors = embedder.embed_documents([obj.search_text for obj in misses])
        for obj, vector in zip(misses, vectors):
            obj.vector = vector
            obj.embedding_model = embedder.model_name
            obj.embedding_dimension = embedder.dimension
    result = store.upsert(objects, embedder.dimension, embedder.model_name, collection)
    result["vectors_reused"] = reused
    return result


def _run(store: WeaviateStore, embedder: LocalEmbedder, sources: List[Path],
         load: Callable[[Path], List[LegalProvision]], collection: str) -> dict:
    """공통 실행 루프: 소스마다 load → 임베딩 → upsert. 실패는 건너뛰고 계속."""
    totals = _new_totals()
    for source in sources:
        try:
            objects = load(source)
            if objects:
                result = _embed_and_store(store, embedder, objects, collection)
                totals["objects_success"] += result["success"]
                totals["objects_failed"] += result["failed"]
                if result["failed"]:
                    totals["errors"].append({"file": str(source), "chunk_ids": result["failed_ids"]})
            totals["files_success"] += 1
        except Exception as exc:
            totals["files_failed"] += 1
            totals["errors"].append({"file": str(source), "error": str(exc)})
    return totals


def index_paths(store: WeaviateStore, embedder: LocalEmbedder, path: Path, collection: str,
                recursive: bool = False, limit: Optional[int] = None,
                loader: Callable[[Path], List[LegalProvision]] = load_law_json) -> dict:
    """조문 JSON(또는 전처리된 첨부 청크 JSON)을 순회 적재한다. collection: 적재할 Weaviate 컬렉션명."""
    sources = discover(path, recursive, limit, {".json"}, skip_names=SKIP_JSON_NAMES)
    return _run(store, embedder, sources, loader, collection)


def index_attachment_files(store: WeaviateStore, embedder: LocalEmbedder, path: Path, collection: str,
                           recursive: bool = False, limit: Optional[int] = None,
                           preprocessor: Callable = preprocess_file,
                           settings: Optional[Settings] = None) -> dict:
    """첨부파일(hwp/pdf/…)을 순회하며 전처리기로 청크를 얻어 적재한다(기존 index-files 명령).

    JSON 쪽에서 미리 어떤 파일이 is_file_only 인지 아는 index_documents() 와 달리, 여기는
    디렉터리를 직접 크롤링해 파일 단위로 처리한다(전처리 API가 이미 준비된 청크 결과물이
    아니라 원본 파일 자체를 순회해서 넣고 싶을 때 쓴다).
    """
    sources = discover(path, recursive, limit, ATTACHMENT_SUFFIXES)
    doc_parser = None
    if preprocessor is preprocess_file and settings is not None:
        if not settings.doc_parser_base_url:
            raise ValueError("index-files 는 실제 첨부 전처리기(DOC_PARSER_BASE_URL)가 필요합니다. "
                             "목업 청크 적재를 막기 위해 실행을 중단합니다.")
        doc_parser = DocParserClient(
            settings.doc_parser_base_url, settings.doc_parser_timeout, settings.doc_parser_max_retries,
            settings.doc_parser_endpoint_path, settings.doc_parser_api_key, settings.doc_parser_upload)

    def load(file_path: Path) -> List[LegalProvision]:
        parent = find_parent_law(file_path)
        if doc_parser is not None:
            data = preprocessor(
                file_path, parent, client=doc_parser,
                chunk_size=settings.doc_parser_chunk_size,
                chunk_overlap=settings.doc_parser_chunk_overlap,
                shared_dir=settings.doc_parser_shared_host_dir,
                shared_container_dir=settings.doc_parser_shared_container_dir,
                keep_temp_files=settings.doc_parser_keep_temp_files)
        else:
            data = preprocessor(file_path, parent)
        return map_attachment_data(data, file_path)

    return _run(store, embedder, sources, load, collection)


def index_changeset(store: WeaviateStore, embedder: LocalEmbedder, settings: Settings,
                    changeset_path: Path, source: str, data_root: Optional[Path] = None) -> dict:
    """증분(JSONL)을 소비해 **변경분만** 재적재한다. 입력 첫 줄로 두 계약을 자동감지한다:

    - 첫 줄이 `package_header` 면 **§6 JSONL package** 소비자로 위임한다(consume_package) —
      record_type(document/normalized_chunk/preprocessed_chunk/file/pending_attachment/delete)을
      전부 이해해 9개 subcase 를 흡수한다(핸드오프 문서 §6). source 는 header 를 우선한다.
    - 아니면 **1세대 change-set** 으로 처리한다(아래). 한 줄 = 한 변경:
        {"op":"upsert","source":"law|admrul","law_id":..,"payload":{..현재 payload..},"git_path":..?}
        {"op":"delete","source":"law|admrul","law_id":..}

    1세대 upsert 는 law_id 단위로 옛 청크를 먼저 지우고(delete_by_law_id — 개정 시 version_uid 가
    바뀌어 chunk UUID 가 통째로 달라지므로) payload 를 매핑·임베딩해 다시 넣는다(멱등). delete 는
    폐지 문서의 청크를 law_id 단위로 지운다. payload 가 없으면 git_path 를 data_root 기준으로 읽어
    채운다(git 모드 폴백). 한 줄이 실패해도 건너뛰고 계속한다."""
    if is_package_file(changeset_path):
        return consume_package(store, embedder, settings, changeset_path, source_override=source)

    if source not in ("law", "admrul", "schlpub"):
        raise ValueError(f"source 는 'law'/'admrul'/'schlpub' 중 하나여야 합니다: {source!r}")
    root = Path(data_root) if data_root else Path(settings.input_data_path)

    totals = _new_totals()
    totals["deleted_docs"] = 0
    # 문서별 컬렉션 라우팅 — admrul change-set 에 학칙공단 문서(doc_target/law_id 접두)가 섞여
    # 오면 schlpub 컬렉션으로 보낸다(§6 package 소비와 같은 규칙).
    routed = _RoutedStores(settings, store, source)
    try:
        with open(Path(changeset_path), encoding="utf-8") as handle:
            for lineno, raw in enumerate(handle, 1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    totals["errors"].append({"line": lineno, "error": f"JSONL 파싱 실패: {exc}"})
                    continue

                law_id = record.get("law_id") or record.get("doc_id")
                record_source = record.get("source") or source
                if record.get("op") == "delete":
                    if not law_id:                          # law_id 없는 delete = 전량삭제 사고 방지
                        totals["errors"].append({"error": "law_id 없는 delete 레코드 건너뜀"})
                        continue
                    eff = settings.source_for_doc(None, law_id, default=record_source)
                    try:
                        routed.get(eff).delete_by_law_id(law_id, settings.collection_for(eff))
                        totals["deleted_docs"] += 1
                    except Exception as exc:
                        totals["errors"].append({"law_id": law_id, "error": f"삭제 실패: {exc}"})
                    continue

                try:
                    payload = record.get("payload")
                    git_path = record.get("git_path")
                    if payload is None:
                        if not git_path:
                            raise ValueError("payload 도 git_path 도 없어 재적재할 내용이 없습니다")
                        source_file = root / git_path
                        payload = json.loads(source_file.read_text(encoding="utf-8"))
                        map_path = source_file.resolve()
                    else:
                        map_path = Path(git_path) if git_path else Path(f"changeset:{law_id}")
                    eff = settings.source_for_doc(payload.get("doc_target"), law_id,
                                                  default=record_source)
                    mapper_fn = map_law_data if eff == "law" else map_admrul_data
                    collection = settings.collection_for(eff)
                    eff_store = routed.get(eff)
                    objects = mapper_fn(payload, map_path,
                                        source_repository=record.get("source_repository"))
                    # ⚠️ upsert 먼저(새 version_uid → 새 UUID) → **전부 성공(failed==0) 했을 때만** 옛 버전
                    #    청크 정리. upsert 는 partial 실패에 예외를 안 던지고 {success, failed} 만 주므로,
                    #    하나라도 실패했는데 옛 청크를 지우면 새 청크 일부가 빠진 채 옛 것까지 없어져 그 법이
                    #    검색에서 사라진다. 실패면 옛 청크를 남겨 둔다(delete-먼저 방식보다 안전).
                    if objects:
                        result = _embed_and_store(eff_store, embedder, objects, collection)
                        totals["objects_success"] += result["success"]
                        totals["objects_failed"] += result["failed"]
                        if result["failed"]:
                            totals["errors"].append({"law_id": law_id, "chunk_ids": result["failed_ids"]})
                        keep_vuids = {getattr(o, "version_uid", None) for o in objects}
                        keep_vuids.discard(None)
                        if keep_vuids and result["failed"] == 0:
                            eff_store.delete_stale_law_chunks(law_id, list(keep_vuids), collection)
                    totals["files_success"] += 1
                except Exception as exc:
                    totals["files_failed"] += 1
                    totals["errors"].append({"law_id": law_id, "error": str(exc)})
    finally:
        routed.close()
    return totals


def _relative_to_repo(local_path: Path, repo_root: Path) -> Optional[str]:
    """local_path 를 repo_root 기준 상대경로 문자열로 만든다(다른 서버에서도 재현 가능하도록).
    repo_root 밖이면(예상 밖 상황) None — 절대경로(source_file_path)만 남는다."""
    try:
        return local_path.resolve().relative_to(Path(repo_root).resolve()).as_posix()
    except ValueError:
        return None


def _pending_label(item: dict) -> str:
    """실패 로그용 첨부 항목 식별 라벨(provision_id > title 순)."""
    return str(item.get("provision_id") or item.get("title") or "?")


def _run_doc_parser(doc_parser: DocParserClient, settings: Settings, local_path: Path) -> list:
    """settings 를 풀어 preprocess.run_doc_parser(공용) 로 위임한다(appendix·document-level
    첨부 처리에서 공통으로 쓴다). 실제 staging·gif 변환·정리 로직은 preprocess 에 있다."""
    return run_doc_parser(
        doc_parser, local_path, settings.doc_parser_chunk_size, settings.doc_parser_chunk_overlap,
        settings.doc_parser_shared_host_dir, settings.doc_parser_shared_container_dir,
        settings.doc_parser_keep_temp_files,
        image_endpoint_path=settings.doc_parser_image_endpoint_path)


# ── 폴더 폴링 소비 (스트리밍 소비자: 건별 package 안전 소비 + 성공/재시도/실패 격리) ──

_MAX_CONSUME_ATTEMPTS = 5          # 일시장애 package 를 이만큼 재시도한 뒤 failed/ 로 격리
# '재시도해도 안 낫는' 결정적 오류 표식 — 이게 들어있으면 즉시 failed/(재시도 안 함).
# 연결/서버/타임아웃/OOM 등은 여기 없어 transient 로 간주 → 재시도.
_PERMANENT_ERROR_MARKERS = ("파싱 실패", "source 미결정", "source 를 결정", "law_id 없는",
                            "계약 위반", "record_count")


def _errors_are_permanent(errors) -> bool:
    """errors 가 결정적 오류(파싱·source·계약·footer 불일치)를 포함하면 True(즉시 격리).
    그 외(연결·서버·타임아웃·OOM·알 수 없음)는 False → 재시도 대상(dead-letter 로 안 사라지게)."""
    for e in errors or []:
        msg = str((e or {}).get("error") or (e or {}).get("reason") or e)
        if any(m in msg for m in _PERMANENT_ERROR_MARKERS):
            return True
    return False


def _attempts_path(name: str, folder: Path) -> Path:
    return folder / (name + ".attempts")


def _read_attempts(name: str, folder: Path) -> int:
    try:
        return int(_attempts_path(name, folder).read_text())
    except (OSError, ValueError):
        return 0


def _bump_attempts(name: str, folder: Path) -> int:
    n = _read_attempts(name, folder) + 1
    try:
        _attempts_path(name, folder).write_text(str(n))
    except OSError:
        # 주의: 기록 실패(디스크 가득 등)를 삼키면 attempts 가 영영 0에 머물러 격리 임계에
        # 닿지 못하고 **같은 package 를 무한 재시도**하게 된다. 기록을 못 하면 이번 판정값을
        # 임계 초과로 돌려 격리 쪽으로 기울인다 — 무한 루프보다 격리가 낫다(격리는 복구 가능).
        logger.warning("attempts 기록 실패(%s) — 격리 임계로 처리", name)
        return _MAX_CONSUME_ATTEMPTS + n
    return n


def _clear_attempts(name: str, folder: Path) -> None:
    _attempts_path(name, folder).unlink(missing_ok=True)


def _archive(src: Path, processed_dir: Optional[Path], name: str) -> None:
    """성공 package 를 processed/ 로 이동(processed_dir 없으면 삭제 — 재소비 방지)."""
    if not processed_dir:
        src.unlink(missing_ok=True)
        return
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(processed_dir / name))


def _write_nack(src: Path, name: str, errors, *, permanent: bool,
                nack_dir: Optional[Path], source_hint: Optional[str]) -> None:
    """격리 직전, 생산자가 읽을 nack 파일을 남긴다.

    **왜 필요한가.** 생산자는 sink 전달이 성공한 순간 그 문서들을 '발송 완료' 로 마킹한다.
    여기서 package 를 조용히 격리해 버리면 그 문서들은 **다음에 개정될 때까지 영영 재발송되지
    않는다** — 벡터DB 에 구멍이 남는다. 어떤 문서가 못 들어갔는지 되돌려줘야 생산자의 self-heal
    스윕이 다시 잡는다. kind 로 되돌림 방식을 가른다:
      · transient_exhausted → 같은 package 를 재발송(소비 쪽이 아팠던 것)
      · permanent           → 문서를 재수집 대상으로(같은 payload 를 다시 보내도 같은 결과)
    best-effort — nack 을 못 써도 격리 자체는 진행한다(쓰기 실패가 소비 루프를 막으면 안 된다)."""
    if not nack_dir:
        return
    try:
        source, law_ids = peek_package_law_ids(src)
    except Exception:                                  # noqa: BLE001 — 격리를 막지 않는다
        source, law_ids = source_hint, []
    if not law_ids:
        return                                         # 되돌릴 문서가 없으면 남길 것도 없다
    record = {
        "record_type": "nack",
        "package_id": name[:-6] if name.endswith(".jsonl") else name,
        "source": source or source_hint,
        "kind": "permanent" if permanent else "transient_exhausted",
        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "law_ids": law_ids,
        "errors": [str((e or {}).get("error") or e)[:300] for e in (errors or [])][:10],
    }
    try:
        nack_dir = Path(nack_dir)
        nack_dir.mkdir(parents=True, exist_ok=True)
        # tmp+rename — 생산자가 반쯤 쓰인 nack 을 읽고 문서를 절반만 되돌리는 일이 없게.
        tmp = nack_dir / (record["package_id"] + ".nack.json.tmp")
        tmp.write_text(json.dumps(record, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, nack_dir / (record["package_id"] + ".nack.json"))
        logger.warning("[nack] %s 문서 %d건을 생산자에 되돌림 요청(kind=%s)",
                       record["package_id"], len(law_ids), record["kind"])
    except OSError as exc:
        logger.warning("[nack] 기록 실패(격리는 계속): %s", exc)


def _quarantine(src: Path, failed_dir: Optional[Path], name: str, errors, *,
                permanent: bool = True, nack_dir: Optional[Path] = None,
                source_hint: Optional[str] = None) -> None:
    """실패 package 를 failed/ 로 격리 + errors.json(failed_dir 없으면 삭제만).
    격리 전에 nack 을 남겨 생산자가 그 문서들을 다시 태우게 한다(조용한 유실 방지)."""
    _write_nack(src, name, errors, permanent=permanent, nack_dir=nack_dir,
                source_hint=source_hint)
    if not failed_dir:
        src.unlink(missing_ok=True)
        return
    failed_dir = Path(failed_dir)
    failed_dir.mkdir(parents=True, exist_ok=True)
    try:
        (failed_dir / (name + ".errors.json")).write_text(
            json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass
    shutil.move(str(src), str(failed_dir / name))


def _fail_or_retry(claimed: Path, original: Path, name: str, folder: Path,
                   failed_dir: Optional[Path], errors, summary: dict, *, transient: bool,
                   nack_dir: Optional[Path] = None, source_hint: Optional[str] = None) -> None:
    """실패 처리: transient 면 원래 이름으로 되돌려 다음 sweep 이 재시도(MAX 초과 시 격리),
    결정적 오류면 즉시 격리. 격리할 때는 nack 을 남겨 생산자가 되돌리게 한다."""
    if transient and _bump_attempts(name, folder) < _MAX_CONSUME_ATTEMPTS:
        try:
            os.replace(str(claimed), str(original))    # 되돌림 → 다음 sweep 재시도
        except OSError:
            pass
        summary["retry"] += 1
        summary["packages"].append({"package": name, "status": "retry",
                                    "attempt": _read_attempts(name, folder), "errors": len(errors)})
        return
    _clear_attempts(name, folder)
    # transient 인데 여기까지 왔다 = 재시도 소진. permanent 와 되돌림 방식이 다르다.
    _quarantine(claimed, failed_dir, name, errors, permanent=not transient,
                nack_dir=nack_dir, source_hint=source_hint)
    summary["failed"] += 1
    summary["packages"].append({"package": name, "status": "failed", "errors": len(errors)})


def consume_folder(embedder: LocalEmbedder, settings: Settings, folder: Path, *,
                   processed_dir: Optional[Path] = None, failed_dir: Optional[Path] = None,
                   source_override: Optional[str] = None, pattern: str = "*.jsonl",
                   nack_dir: Optional[Path] = None) -> dict:
    """폴더에 도착한 package(*.jsonl)들을 오래된 순서로 **안전하게** 소비한다(스트리밍 소비자).

    안전장치:
      · **폴더 존재 검증** — 없는/오타 경로면 조용한 성공(0건) 대신 즉시 에러(monitoring 이 알아채게).
      · **원자적 클레임** — 소비 전 `<pkg>.processing` 로 rename 해 잡는다. 두 소비자가 같은 package 를
        동시에 집거나 이동(move)이 경합해 sweep 전체가 죽는 것을 막는다. 크래시로 남은 `.processing`
        는 다음 실행 시작에 원래 이름으로 되돌려(reclaim) 재소비한다.
      · **transient 자동 재시도** — 연결/서버/타임아웃/OOM 등 일시장애는 failed/ 로 보내지 않고 원래
        이름으로 되돌려 다음 sweep 이 재시도한다(`<pkg>.attempts` 로 횟수 추적, MAX 초과 시 격리).
        결정적 오류(파싱·source·계약·footer 불일치)만 즉시 failed/. → 일시 outage 로 package 가
        dead-letter 에 영구히 갇히지 않는다(사람 없이 복구).
      · **격리 시 nack** — 격리하는 package 의 law_id 목록을 nack_dir 에 남겨 생산자가 그
        문서들을 재발송(일시장애 소진) 또는 재수집(결정적 오류) 대상으로 되돌리게 한다.
        이게 없으면 생산자는 이미 '발송 완료' 로 마킹해 둔 상태라 그 문서들이 **다음 개정까지
        영영 색인되지 않는다**(조용한 유실). nack_dir 을 안 주면 설정값(PACKAGE_NACK_DIR)을 쓴다.
      · 무오류 소비 → processed/ (또는 PACKAGE_DELETE_CONSUMED_PACKAGE 면 consume 가 이미 삭제).
    한 package 실패가 다음 package 소비를 막지 않는다. 재실행 멱등."""
    folder = Path(folder)
    # 기본은 소비 폴더 밑 nack/ — folder sink 구성에서 생산자 기본값(PACKAGE_OUT_DIR/nack)과
    #   같은 경로가 되어 무설정으로 되돌림 경로가 성립한다.
    nack_dir = nack_dir or settings.package_nack_dir or (Path(folder) / "nack")
    if not folder.is_dir():
        raise FileNotFoundError(f"consume-folder 대상 폴더가 없습니다(경로 확인): {folder}")

    # 크래시로 남은 claimed(.processing) 되돌리기(reclaim) — 다음 소비에서 재시도되게.
    for stale in folder.glob(pattern + ".processing"):
        try:
            os.replace(str(stale), str(stale.with_suffix("")))
        except OSError:
            pass

    packages = sorted(p for p in folder.glob(pattern) if p.is_file())
    summary = {"folder": str(folder), "total": len(packages), "ok": 0, "failed": 0,
               "retry": 0, "packages": []}
    stores: dict = {}                                  # source -> 열린 WeaviateStore(연결 재사용)
    try:
        for pkg in packages:
            name = pkg.name
            claimed = pkg.with_name(name + ".processing")
            try:
                os.replace(str(pkg), str(claimed))     # 원자적 클레임(동시소비·이동경합 방지)
            except OSError:
                continue                                # 다른 소비자가 이미 가져감/사라짐

            try:
                source = source_override or peek_package_source(claimed)
            except Exception:
                source = source_override
            if source not in ("law", "admrul", "schlpub"):
                _clear_attempts(name, folder)
                # source 미결정 = 결정적 오류지만, law_id 를 못 읽으면 nack 도 못 쓴다(그때는
                #   격리만 — 사람이 failed/ 를 보고 판단해야 하는 계약 위반 케이스다).
                _quarantine(claimed, failed_dir, name, [{"error": f"source 미결정: {source!r}"}],
                            permanent=True, nack_dir=nack_dir, source_hint=source_override)
                summary["failed"] += 1
                summary["packages"].append({"package": name, "status": "failed", "reason": "source 미결정"})
                continue

            store = stores.get(source)
            if store is None:
                try:
                    store = WeaviateStore(settings, source)
                    store.__enter__()
                    stores[source] = store
                except Exception as exc:                # 연결 실패 = 일시장애 → 이 package 재시도
                    _fail_or_retry(claimed, pkg, name, folder, failed_dir,
                                   [{"error": f"Weaviate 연결 실패: {exc}"}], summary, transient=True,
                                   nack_dir=nack_dir, source_hint=source)
                    continue

            try:
                result = consume_package(store, embedder, settings, claimed, source_override=source)
            except Exception as exc:                    # 예상외 예외 = 일시장애로 간주 → 재시도
                _fail_or_retry(claimed, pkg, name, folder, failed_dir,
                               [{"error": str(exc)}], summary, transient=True,
                               nack_dir=nack_dir, source_hint=source)
                continue

            errors = result.get("errors") or []
            if errors:
                _fail_or_retry(claimed, pkg, name, folder, failed_dir, errors, summary,
                               transient=not _errors_are_permanent(errors),
                               nack_dir=nack_dir, source_hint=source)
            else:
                _clear_attempts(name, folder)
                if not result.get("package_deleted"):   # delete 옵션이 이미 지웠으면 이동 안 함
                    _archive(claimed, processed_dir, name)
                summary["ok"] += 1
                summary["packages"].append({"package": name, "status": "ok",
                                            "documents": result.get("documents"),
                                            "objects": result.get("objects_success")})
    finally:
        for store in stores.values():
            try:
                store.__exit__(None, None, None)
            except Exception:
                pass
    return summary


class _BulkBuffer:
    """문서 경계를 넘어 청크를 모아 **큰 배치**로 임베딩·업서트한다.

    원격 임베딩 게이트웨이는 호출당 고정비가 커서(실측: 20건 4~15s vs 128건 2~17s — 처리량은
    배치가 클수록 유리) 문서당 소배치(평균 ~20청크) 호출이 전체 색인을 지배했다. 여기서는
    여러 문서의 청크를 min_flush(기본 256) 이상 모아 한 번에 흘린다 — embed_documents 가
    내부에서 EMBEDDING_BATCH_SIZE 단위로 다시 자르므로 게이트웨이 요청 크기는 안전하다.

    문서별 후처리(고아 청크 정리·성공/실패 집계)는 flush 시점에 문서 단위로 한다.
    실패 청크가 있는 문서는 정리를 건너뛴다(부분 실패 시 옛 청크 보존 원칙 유지)."""

    def __init__(self, store, embedder, collection: str, totals: dict, min_flush: int = 256,
                 cache: Optional[VectorCache] = None):
        self.store, self.embedder, self.collection = store, embedder, collection
        self.totals, self.min_flush, self.cache = totals, min_flush, cache
        self.docs: list = []                     # (source_str, objects, file_cleanups, json_cleanups)
        self.pending = 0

    def add(self, source: str, objects: list, file_cleanups: list, json_cleanups: list) -> None:
        self.docs.append((source, objects, file_cleanups, json_cleanups))
        self.pending += len(objects)
        if self.pending >= self.min_flush:
            self.flush()

    def flush(self) -> None:
        docs, self.docs, self.pending = self.docs, [], 0
        all_objs = [o for _, objs, _, _ in docs for o in objs]
        if not all_objs:
            for src, _, _, _ in docs:
                self.totals["files_success"] += 1
            return
        try:
            result = _embed_and_store(self.store, self.embedder, all_objs, self.collection,
                                      cache=self.cache)
        except Exception as exc:                 # 배치 전체 실패 — 문서별로 실패 집계(재색인 대상)
            for src, _, _, _ in docs:
                self.totals["files_failed"] += 1
                self.totals["errors"].append({"file": src, "error": str(exc)})
            return
        self.totals["objects_success"] += result["success"]
        self.totals["objects_failed"] += result["failed"]
        self.totals["vectors_reused"] += result.get("vectors_reused", 0)
        failed = set(result.get("failed_ids") or [])
        for src, objs, file_cleanups, json_cleanups in docs:
            doc_failed = [o.chunk_id for o in objs if o.chunk_id in failed]
            if doc_failed:
                self.totals["files_failed"] += 1
                self.totals["errors"].append({"file": src, "chunk_ids": doc_failed})
                continue
            try:
                for file_id, chunk_ids in file_cleanups:
                    self.store.delete_orphan_file_chunks(file_id, chunk_ids, self.collection)
                for law_id, version_uid, chunk_ids in json_cleanups:
                    self.store.delete_orphan_json_chunks(law_id, version_uid, chunk_ids, self.collection)
                self.totals["files_success"] += 1
            except Exception as exc:
                self.totals["files_failed"] += 1
                self.totals["errors"].append({"file": src, "error": str(exc)})
        logger.info("bulk flush: 청크 %d개 (누적 성공 %d · 실패 %d)",
                    len(all_objs), self.totals["objects_success"], self.totals["objects_failed"])


def index_documents(store: WeaviateStore, embedder: LocalEmbedder, settings: Settings, path: Path,
                    doc_source: str, repo_root: Path, git_commit: Optional[str],
                    source_repository: Optional[str], recursive: bool = False,
                    limit: Optional[int] = None, doc_parser: Optional[DocParserClient] = None,
                    skip_existing: bool = False, paths: Optional[List[Path]] = None,
                    files_only: bool = False, preprocess_files: Optional[bool] = None,
                    embed_flush: Optional[int] = None,
                    vector_cache: Optional[VectorCache] = None, scope=None) -> dict:
    """법령/행정규칙 JSON을 문서 단위로 순회하며 본문(JSON) + 파일 전처리(Doc Parser) 대상을
    모두 적재한다. 전처리 대상은 두 종류이며 서로 다른 조건으로 판단한다(§3):
      1) appendices[].is_file_only == True 인 별표/별지/서식
      2) body 에 의미 있는 텍스트가 없고 최상위 attachments[] 만 있는 문서(document_is_file_only)
    본문 텍스트가 있으면 최상위 attachments[] 는 절대 자동 전처리하지 않는다(중복 적재 방지).

    문서 하나, 첨부 하나가 실패해도 나머지는 계속 처리한다(파일 단위·첨부 단위 모두 예외 격리).
    doc_source: "law"/"admrul"/"schlpub" — 어느 mapper·어느 컬렉션을 쓸지 결정한다
    (schlpub 은 admrul 과 같은 mapper, 컬렉션만 다르다 — 레포당 컬렉션 하나).

    paths 를 주면 discover(경로 순회) 대신 그 JSON 경로 목록만 처리한다(대상 문서만 콕 집을 때).
    files_only=True 면 본문(JSON) 매핑을 건너뛰고 파일 전처리 결과만 적재한다 — 본문 청크는
    이미 있고 첨부만 뒤늦게 채울 때 쓴다(본문 재임베딩·중복 없음). is_file_only 별표는 mapper 가
    본문으로 만들지 않으므로(중복 없음), 새로 붙는 APPENDIX/FILE 청크만 추가된다.
    """
    if doc_source not in ("law", "admrul", "schlpub"):
        raise ValueError(f"doc_source 는 'law'/'admrul'/'schlpub' 중 하나여야 합니다: {doc_source!r}")

    mapper_fn = map_law_data if doc_source == "law" else map_admrul_data
    collection = settings.collection_for(doc_source)
    # 첨부 전처리 스위치 — 끄면(또는 DOC_PARSER_BASE_URL 이 비면) 첨부를 **건너뛴다**.
    #   예전엔 URL 이 비어도 무조건 클라이언트를 만들어 첨부마다 ValueError 로 실패 집계됐고,
    #   운영 지침("벌크는 docparser 끄고 → 대상만 타겟 재색인")을 코드로 실행할 수 없었다.
    # 호출 단위 오버라이드(Temporal 요청 등)가 env(INDEX_PREPROCESS_FILES)보다 우선한다 —
    # 같은 워커/환경에서 "1단계 본문만 → 2단계 전처리 타겟" 을 요청만 바꿔 돌릴 수 있게.
    want_preprocess = (preprocess_files if preprocess_files is not None
                       else getattr(settings, "index_preprocess_files", True))
    if doc_parser is None:
        if want_preprocess and settings.doc_parser_base_url:
            doc_parser = DocParserClient(
                settings.doc_parser_base_url, settings.doc_parser_timeout, settings.doc_parser_max_retries,
                endpoint_path=settings.doc_parser_endpoint_path, api_key=settings.doc_parser_api_key,
                upload=settings.doc_parser_upload)
        else:
            logger.info("첨부 전처리 비활성 — 본문 JSON 만 색인합니다"
                        " (INDEX_PREPROCESS_FILES=%s, DOC_PARSER_BASE_URL=%s)",
                        want_preprocess,
                        "설정됨" if settings.doc_parser_base_url else "없음")

    if paths is not None:
        sources = [p for p in paths if p.name not in SKIP_JSON_NAMES]
    else:
        sources = discover(path, recursive, limit, {".json"}, skip_names=SKIP_JSON_NAMES)
    totals = _new_totals()
    bulk = _BulkBuffer(store, embedder, collection, totals,
                       min_flush=(embed_flush if embed_flush is not None
                                  else int(os.environ.get("INDEX_EMBED_FLUSH", "256"))),
                       cache=vector_cache)

    if scope is not None:
        totals.update(scope_skipped=0, scope_matched=0,
                      scope_prompt_id=scope.prompt_id, scope_revision_id=scope.revision_id)

    for source in sources:
        try:
            raw = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            totals["files_failed"] += 1
            totals["errors"].append({"file": str(source), "error": f"JSON 읽기 실패: {exc}"})
            continue

        if scope is not None:
            try:
                selected = scope.matches(raw)
            except ValueError as exc:
                totals["files_failed"] += 1
                totals["errors"].append({"file": str(source), "error": str(exc)})
                continue
            if not selected:
                totals["scope_skipped"] += 1
                continue
            totals["scope_matched"] += 1

        # 이어받기(--skip-existing): skip = **완전한 문서만**. 예전 정의(청크 1개라도 있으면
        # skip)는 업서트 부분 실패로 반쯤 들어간 문서(실측: 민법 제1~394조 400청크 누락)가
        # 영영 안 메워졌다. 기대 chunk_id 는 매핑이 결정적으로 만들므로(version_uid·단위·순번
        # 기반, 내용 비의존) 임베딩 없이 산출해 전부 존재하는지 ID 일괄조회로 대조한다.
        #   · JSON 청크만 대조 — FILE 청크는 전처리를 돌려야 개수가 나온다.
        #   · '기대 ⊆ 실제'면 skip — 초과분(2단계 OCR 로 분할이 늘어난 청크·FILE 청크)은
        #     건드리지 않는다(재색인하면 OCR 결과를 마커본으로 되돌릴 위험).
        #   · 매핑 실패·JSON 청크 0(파일전용 문서)이면 skip 하지 않고 본 처리로 넘긴다
        #     (같은 오류가 정식 집계에 잡히고, 파일전용은 FILE 청크 유무를 알 수 없으므로).
        # files_only 는 본문 청크가 이미 있는 채로 첨부만 채우는 것이라 skip_existing 을 적용하지 않는다.
        if skip_existing and not files_only:
            try:
                expected = [object_uuid(o.chunk_id)
                            for o in mapper_fn(raw, source, source_repository=source_repository)
                            if o.source_type == "JSON"]
            except Exception:
                expected = []
            if expected and not store.missing_ids(expected, collection):
                totals["files_skipped"] += 1
                continue

        # STEP1 (행정규칙류 전용 — 학칙공단 포함): 조문 content 의 [그림] 마커를 본문이미지 OCR
        # 텍스트로 치환한다. 법령(law)은 건너뛴다 — 실측(2026-07-29, 표본 476/476 조문)상 표·수식이
        # 이미 조문 본문에 텍스트(박스드로잉 표 포함)로 들어 있어 [그림] OCR 이 내용상 중복이고,
        # 법령 본문이미지는 대부분 수식 조각이라 OCR 품질도 낮다(비용만 큼). admrul/schlpub 은
        # 조직도·공고문처럼 본문 텍스트에 없는 이미지가 있어 유지한다. 매핑 전에 raw 를 직접
        # 고쳐야 search_text·content 에 반영된다.
        body = raw.get("body") if isinstance(raw, dict) else None
        # doc_parser 가 없으면(벌크 = INDEX_PREPROCESS_FILES=false) STEP1 자체를 건너뛴다 —
        #   [그림] 마커는 원문에 남고, 2단계(그림 문서만 전처리 켜고 재색인)가 제자리 교체한다.
        articles = ((body.get("articles") if isinstance(body, dict) else None)
                    if (doc_source != "law" and not files_only and doc_parser is not None) else None)
        for article in articles or []:
            if not isinstance(article, dict) or "[그림]" not in str(article.get("content") or ""):
                continue
            try:
                new_content, success, failed, transient_failed = resolve_article_images(
                    doc_parser, settings, repo_root, raw, article, doc_dir=source.parent)
            except Exception as exc:
                totals["errors"].append({
                    "file": str(source), "article": str(article.get("article_no")),
                    "error": f"본문이미지 처리 실패(원문 [그림] 마커 유지): {exc}",
                })
                continue
            totals["images_success"] += success
            totals["images_failed"] += failed
            if transient_failed:
                # 일시 오류(전처리기 다운 등)는 오류로 남겨 문서가 실패 큐(--failed-out)에
                # 들어가게 한다 — 재실행이 OCR 텍스트를 회복한다. 결정 실패는 마커 유지가 최종.
                totals["errors"].append({
                    "file": str(source), "article": str(article.get("article_no")),
                    "error": f"본문이미지 OCR 일시 오류 {transient_failed}건 — 재시도 대상",
                })
            if new_content is not None:
                article["content"] = new_content

        if files_only:
            objects = []  # 본문(JSON) 매핑 생략 — 이미 적재된 본문 청크는 그대로 두고 첨부만 채운다
        else:
            # git_path 는 **데이터 레포 상대경로**로 통일한다. 증분(package) 경로는 생산자가 준
            # 레포 상대경로를 그대로 쓰는데 여기(초기 전량 색인)만 절대경로를 넣으면, 같은 필드가
            # 적재 경로에 따라 다른 뜻이 되고 색인한 머신의 절대경로가 Weaviate 에 박힌다.
            # 레포 밖 파일(임시 검증 등)이면 어쩔 수 없이 절대경로로 남긴다.
            map_path = Path(_relative_to_repo(source, repo_root) or source.resolve())
            try:
                objects = mapper_fn(raw, map_path, source_repository=source_repository)
            except Exception as exc:
                totals["files_failed"] += 1
                totals["errors"].append({"file": str(source), "error": str(exc)})
                continue

        file_cleanups = []  # [(file_id, [chunk_id, ...]), ...] — 업서트 성공 후 고아 청크 정리용(§23-7)

        if isinstance(raw, dict):
            for item in find_pending_appendices(raw):
                label = _pending_label(item)
                try:
                    local_path = appendix_local_path(repo_root, raw, item, doc_dir=source.parent)
                    if local_path is None:
                        raise FileNotFoundError(f"로컬 첨부파일을 찾을 수 없습니다 (is_file_only): {label}")
                    if doc_parser is None:
                        totals["attachments_skipped"] = totals.get("attachments_skipped", 0) + 1
                        continue
                    chunks = _run_doc_parser(doc_parser, settings, local_path)
                    attachment_objects = build_attachment_provisions(
                        raw, source, doc_source, source_repository, chunks,
                        provision_id=item.get("provision_id"), unit_type="APPENDIX",
                        file_name=local_path.name, file_url=item.get("file_url"),
                        source_file_path=str(local_path), appendix=item,
                        source_relative_path=_relative_to_repo(local_path, repo_root),
                        file_hash=file_sha256(local_path),
                    )
                    objects.extend(attachment_objects)
                    if attachment_objects:
                        file_cleanups.append(
                            (attachment_objects[0].file_id, [o.chunk_id for o in attachment_objects]))
                    totals["attachments_success"] += 1
                except FileNotFoundError as exc:
                    totals["attachments_failed"] += 1
                    totals["errors"].append({"file": str(source), "attachment": label, "error": str(exc)})
                except DocParserError as exc:
                    # 결정오류(DRM/손상/빈 결과 — 원천 파일 한계)는 실패 큐(errors→failed-out)가
                    # 아니라 별도 보류 목록에 남긴다. 재시도해도 같은 결과라 실패 큐에 넣으면
                    # 재시도 목록이 영영 안 빈다. 문서가 개정되면 새 파일로 자동 재시도된다.
                    if is_permanent_parser_error(exc):
                        totals["attachments_unsupported"] = totals.get("attachments_unsupported", 0) + 1
                        totals.setdefault("unsupported", []).append(
                            {"file": str(source), "attachment": label, "error": f"[{exc.code}] {exc.message}"})
                    else:
                        totals["attachments_failed"] += 1
                        totals["errors"].append(
                            {"file": str(source), "attachment": label, "error": f"[{exc.code}] {exc.message}"})
                except Exception as exc:  # 첨부 하나의 예상 밖 실패로 문서 전체가 막히지 않게
                    totals["attachments_failed"] += 1
                    totals["errors"].append({"file": str(source), "attachment": label, "error": str(exc)})

            # STEP3(§2 공통 규칙): 조문0(본문=파일) 문서는 attachments[] 중 §2 규칙(미지원 확장자
            # skip + 같은 basename 형식중복만 hwpx>hwp>docx>pdf 로 1개) 을 통과한 파일 전부를
            # 처리한다 — role(원문/이유서 등) 구분 없이. 하나만 골라 버리면 서로 다른 실제 문서가
            # 섞여 있을 때(실측 다수 확인) 내용이 통째로 유실된다.
            pending_files = resolve_pending_document_files(repo_root, raw, doc_dir=source.parent)
            if document_is_file_only(raw) and not pending_files:
                totals["attachments_failed"] += 1
                totals["errors"].append({
                    "file": str(source), "attachment": "?",
                    "error": "문서 전체 원문 로컬 첨부파일을 찾을 수 없습니다 (document_is_file_only)",
                })
            for pending in pending_files:
                doc_attachment, local_path = pending["attachment"], pending["local_path"]
                label = doc_attachment.get("filename") or doc_attachment.get("name") or local_path.name
                try:
                    if doc_parser is None:
                        totals["attachments_skipped"] = totals.get("attachments_skipped", 0) + 1
                        continue
                    chunks = _run_doc_parser(doc_parser, settings, local_path)
                    attachment_objects = build_attachment_provisions(
                        raw, source, doc_source, source_repository, chunks,
                        provision_id=None, unit_type="FILE",
                        file_name=local_path.name, file_url=doc_attachment.get("url"),
                        source_file_path=str(local_path),
                        source_relative_path=_relative_to_repo(local_path, repo_root),
                        file_hash=file_sha256(local_path),
                    )
                    objects.extend(attachment_objects)
                    if attachment_objects:
                        file_cleanups.append(
                            (attachment_objects[0].file_id, [o.chunk_id for o in attachment_objects]))
                    totals["attachments_success"] += 1
                except DocParserError as exc:
                    if is_permanent_parser_error(exc):    # 결정오류 = 원천 한계 보류(위와 동일)
                        totals["attachments_unsupported"] = totals.get("attachments_unsupported", 0) + 1
                        totals.setdefault("unsupported", []).append(
                            {"file": str(source), "attachment": label, "error": f"[{exc.code}] {exc.message}"})
                    else:
                        totals["attachments_failed"] += 1
                        totals["errors"].append(
                            {"file": str(source), "attachment": label, "error": f"[{exc.code}] {exc.message}"})
                except Exception as exc:
                    totals["attachments_failed"] += 1
                    totals["errors"].append({"file": str(source), "attachment": label, "error": str(exc)})

        for obj in objects:
            obj.git_commit = obj.git_commit or git_commit

        # 문서 단위 즉시 임베딩 대신 버퍼에 쌓는다 — 성공/실패 집계와 고아 청크 정리
        # (§23-7 file_id 정리, OCR 등으로 줄어든 JSON 청크 정리)는 flush 가 문서 단위로 수행.
        json_cleanups = []
        if objects and not files_only:
            json_chunk_ids = [o.chunk_id for o in objects if o.source_type == "JSON"]
            first_json = next((o for o in objects if o.source_type == "JSON"), None)
            law_id = first_json.law_id if first_json else None
            version_uid = first_json.version_uid if first_json else None
            if law_id and version_uid and json_chunk_ids:
                json_cleanups.append((law_id, version_uid, json_chunk_ids))
        bulk.add(str(source), objects, file_cleanups, json_cleanups)

    bulk.flush()
    return totals


def find_preprocess_targets(path: Path, doc_source: str, recursive: bool = True,
                            limit: Optional[int] = None) -> dict:
    """2단계(전처리 켜고 타겟 재색인) 대상 문서를 원본 데이터에서 결정적으로 추출한다.

    대상 = ① 본문 [그림] 마커(admrul/schlpub — 법령은 OCR off 정책 §4) ② is_file_only
    별표/별지/서식 ③ 문서 전체가 파일인 문서(document_is_file_only). Weaviate 조회 없이
    원본 JSON 만 보므로 적재 전·후 어느 시점이든 같은 결과를 준다(예전엔 대상 목록을 손으로
    만들었다). 반환 paths 는 `index --paths-file` 입력으로 그대로 쓴다."""
    sources = discover(path, recursive, limit, {".json"}, skip_names=SKIP_JSON_NAMES)
    out = {"paths": [], "image_docs": 0, "file_only_appendix_docs": 0,
           "document_file_only_docs": 0, "scanned": 0, "read_errors": 0}
    for source in sources:
        try:
            raw = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            out["read_errors"] += 1
            continue
        if not isinstance(raw, dict):
            continue
        out["scanned"] += 1
        has_image = doc_source != "law" and any(
            "[그림]" in str(a.get("content") or "")
            for a in ((raw.get("body") or {}).get("articles") or []) if isinstance(a, dict))
        has_file_only = any(isinstance(ap, dict) and ap.get("is_file_only")
                            for ap in (raw.get("appendices") or []))
        doc_file_only = bool(document_is_file_only(raw))
        if has_image:
            out["image_docs"] += 1
        if has_file_only:
            out["file_only_appendix_docs"] += 1
        if doc_file_only:
            out["document_file_only_docs"] += 1
        if has_image or has_file_only or doc_file_only:
            out["paths"].append(str(source))
    return out
