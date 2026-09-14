"""대량 백필 드라이버: paths-file을 chunk 단위로 처리하고 실패 상세를 JSONL로 남긴다.

사용:
  python scripts/run_backfill_with_errors.py <paths_file> <files_only:0|1> [chunk_size]

files_only=1 은 이미 들어간 본문은 건드리지 않고 FILE 청크만 추가한다.
files_only=0 은 일반 재색인 경로라 행정규칙 본문 [그림] OCR을 조문에 합쳐 다시 적재한다.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from law_indexer import git_sync
from law_indexer.cli import _embedder, _repo_config
from law_indexer.config import Settings
from law_indexer.pipeline import index_documents
from law_indexer.weaviate_store import WeaviateStore


def _write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(text)


def _append_jsonl(path: str, record: dict) -> None:
    with open(path, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(record, ensure_ascii=False) + "\n")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("사용: run_backfill_with_errors.py <paths_file> <files_only:0|1> [chunk_size]")

    paths_file = sys.argv[1]
    files_only = sys.argv[2] == "1"
    chunk = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    src = "admrul"
    progress_file = paths_file + ".progress"
    errors_file = paths_file + ".errors.jsonl"
    chunk_errors_file = paths_file + ".chunk_errors.jsonl"

    settings = Settings.from_env()
    all_paths = [Path(x.strip()) for x in open(paths_file, encoding="utf-8") if x.strip()]
    total = len(all_paths)
    total_chunks = -(-total // chunk)

    try:
        start_chunk = int(open(progress_file, encoding="utf-8").read().strip())
    except Exception:
        start_chunk = 0

    source_repository, repo_path, _branch, collection = _repo_config(settings, src)
    git_commit = git_sync.current_commit(repo_path)
    embedder = _embedder(settings)

    cum_ok = cum_fail = cum_docs = cum_dfail = cum_obj_ok = cum_obj_fail = 0
    t0 = time.time()
    print(
        f"=== 시작: {total}개 문서 · chunk={chunk}({total_chunks}개) · files_only={files_only} · "
        f"resume={start_chunk} · errors={errors_file} ===",
        flush=True,
    )

    with WeaviateStore(settings, src) as store:
        i = start_chunk
        while i * chunk < total:
            batch = all_paths[i * chunk:(i + 1) * chunk]
            chunk_no = i + 1
            try:
                result = index_documents(
                    store, embedder, settings, repo_path, src, repo_path, git_commit,
                    source_repository, paths=batch, files_only=files_only,
                )
            except Exception as exc:
                _append_jsonl(chunk_errors_file, {
                    "ts": _now(),
                    "chunk": chunk_no,
                    "total_chunks": total_chunks,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "paths": [str(p) for p in batch],
                })
                print(
                    f"[chunk {chunk_no}/{total_chunks}] 통째 예외: "
                    f"{type(exc).__name__}: {str(exc)[:200]} | details={chunk_errors_file}",
                    flush=True,
                )
                i += 1
                _write_text(progress_file, str(i))
                continue

            for error in result.get("errors", []):
                _append_jsonl(errors_file, {
                    "ts": _now(),
                    "chunk": chunk_no,
                    "total_chunks": total_chunks,
                    "files_only": files_only,
                    **(error if isinstance(error, dict) else {"error": str(error)}),
                })

            cum_ok += result["attachments_success"]
            cum_fail += result["attachments_failed"]
            cum_docs += result["files_success"]
            cum_dfail += result.get("files_failed", 0)
            cum_obj_ok += result.get("objects_success", 0)
            cum_obj_fail += result.get("objects_failed", 0)

            elapsed = int(time.time() - t0)
            done_chunks = i + 1 - start_chunk
            rate = done_chunks / max(elapsed, 1) * chunk * 60
            eta = int((total - (i + 1) * chunk) / max(rate / 60, 0.001)) if rate else 0
            print(
                f"[chunk {chunk_no}/{total_chunks}] +docs{result['files_success']} "
                f"+att_ok{result['attachments_success']} +att_fail{result['attachments_failed']} "
                f"+img{result.get('images_success', 0)} +obj_ok{result.get('objects_success', 0)} "
                f"+obj_fail{result.get('objects_failed', 0)} | 누적 docs={cum_docs} "
                f"att_ok={cum_ok} att_fail={cum_fail} docfail={cum_dfail} "
                f"obj_ok={cum_obj_ok} obj_fail={cum_obj_fail} | {elapsed}s ETA~{eta//60}m",
                flush=True,
            )
            i += 1
            _write_text(progress_file, str(i))

    print(
        f"=== 완료: docs={cum_docs}(fail {cum_dfail}) att_ok={cum_ok} att_fail={cum_fail} "
        f"obj_ok={cum_obj_ok} obj_fail={cum_obj_fail} | {int(time.time()-t0)}s ===",
        flush=True,
    )


if __name__ == "__main__":
    main()
