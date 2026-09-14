import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from . import git_sync
from .config import Settings
from .embedder import make_embedder
from .mapper import load_attachment_json
from .package import peek_package_source
from .pipeline import (consume_folder, find_preprocess_targets, index_attachment_files,
                       index_changeset, index_documents, index_paths)
from .weaviate_store import WeaviateStore

# 본문/첨부 순회 적재 명령들 (임베딩 모델 필요)
INDEX_COMMANDS = {"index", "index-files", "index-attachment-chunks", "index-changeset", "consume-folder"}

_SOURCES = ("law", "admrul", "schlpub")
# --source 선택지: 개별 3종 + 전부("both" 는 2컬렉션 시절 이름의 하위호환 별칭 — "all" 과 동일)
_SOURCE_CHOICES = _SOURCES + ("both", "all")


def _repo_config(settings: Settings, source: str):
    """source("law"|"admrul"|"schlpub")에 맞는 (repo_url, repo_path, branch, collection) 튜플."""
    if source == "law":
        return settings.law_repo_url, settings.law_repo_path, settings.law_repo_branch, settings.law_collection
    if source == "schlpub":
        return (settings.schlpub_repo_url, settings.schlpub_repo_path or settings.admrul_repo_path,
                settings.schlpub_repo_branch, settings.schlpub_collection)
    return settings.admrul_repo_url, settings.admrul_repo_path, settings.admrul_repo_branch, settings.admrul_collection


def _sources_for(value: str) -> List[str]:
    """--source all|both|law|admrul|schlpub 을 순회할 source 목록으로 바꾼다."""
    return list(_SOURCES) if value in ("both", "all") else [value]


def _failure_code(result) -> int:
    """색인/소비 결과에 실패·에러가 있으면 2(부분실패), 없으면 0.
    cron/CI 가 '전부 성공'과 '일부 failed/·격리'를 exit 코드로 구분하게 한다(0=clean, 2=부분실패,
    1=하드에러는 main 의 예외 핸들러). consume-folder 의 retry(일시장애 재시도 대기)는 실패로 치지
    않는다 — 다음 sweep 이 처리한다."""
    if not isinstance(result, dict):
        return 0
    if result.get("failed"):                       # consume-folder 요약
        return 2
    for k in ("files_failed", "objects_failed", "attachments_failed", "laws_failed"):
        if result.get(k):
            return 2
    if result.get("errors"):
        return 2
    for sub in (result.get("by_source") or {}).values():   # index 의 combined 중첩
        if _failure_code(sub):
            return 2
    return 0


def _infer_source_from_path(path: Path) -> Optional[str]:
    """--input 으로 넘어온 경로가 어느 레포 밑인지 폴더명으로 판별한다.

    레포 3개(LAW/ADMRUL/SCHLPUBRUL) = 컬렉션 3개(law/admrul/schlpub).
    옛 이름(law_data/admrul_data)도 그대로 인식해 기존 미러가 깨지지 않게 한다."""
    parts = set(path.resolve().parts)
    in_law = bool(parts & {"LAW", "law_data"})
    in_admrul = bool(parts & {"ADMRUL", "admrul_data"})
    in_schlpub = "SCHLPUBRUL" in parts
    matched = [s for s, hit in (("law", in_law), ("admrul", in_admrul), ("schlpub", in_schlpub)) if hit]
    return matched[0] if len(matched) == 1 else None


def _embedder(settings):
    """CLI 명령에서 쓸 임베더(local 로컬 로드 / remote genos 서빙)를 만든다."""
    print(f"임베딩 백엔드={settings.embedding_backend} 모델={settings.embedding_model}", file=sys.stderr)
    return make_embedder(settings)


def parser() -> argparse.ArgumentParser:
    """law_indexer CLI 서브커맨드와 옵션을 정의한다."""
    root = argparse.ArgumentParser(prog="python -m law_indexer")
    sub = root.add_subparsers(dest="command", required=True)
    sub.add_parser("health")

    # Git 저장소 동기화(clone 없으면·있으면 fetch+reset) — 법령/행정규칙 독립 관리
    sync_cmd = sub.add_parser("sync", help="LAW/ADMRUL/SCHLPUBRUL 저장소를 clone 또는 fetch로 최신화")
    sync_cmd.add_argument("--source", choices=_SOURCE_CHOICES, default="all")

    create = sub.add_parser("create-collection")
    create.add_argument("--recreate", action="store_true")
    create.add_argument("--source", choices=_SOURCE_CHOICES, default="all")

    # 조문 JSON + is_file_only 첨부(Doc Parser) 적재: --input 생략 시 각 source 의 repo 경로를 순회
    index = sub.add_parser("index", help="법령/행정규칙/학칙공단 JSON + is_file_only 첨부 적재 (기본: 세 저장소 전체)")
    index.add_argument("--input", type=Path, help="파일 또는 디렉터리 (생략 시 각 source 의 REPO_PATH)")
    index.add_argument("--recursive", action="store_true")
    index.add_argument("--limit", type=int)
    index.add_argument("--skip-existing", action="store_true",
                       help="이미 청크가 있는 법은 건너뜀(이어받기 — 실패/미처리분만 재색인)")
    index.add_argument("--paths-file", type=Path,
                       help="줄바꿈으로 구분된 JSON 경로 목록만 처리(대상 문서만 콕 집을 때). --input 대신 사용")
    index.add_argument("--files-only", action="store_true",
                       help="본문(JSON) 매핑을 건너뛰고 첨부 전처리 결과만 적재(본문 청크는 그대로, 뒤늦게 첨부만 채울 때)")
    index.add_argument("--source", choices=_SOURCE_CHOICES, default=None,
                       help="--input 생략 시 기본 all(세 저장소). --input 지정 시 생략하면 경로로 자동판별 시도")
    index.add_argument("--vector-cache", type=Path, default=None,
                       help="migrate_collection export parquet 를 임베딩 재사용 캐시로 — "
                            "같은 chunk + 같은 search_text 면 임베딩 호출 없이 옛 벡터 재사용(재적재 가속)")
    index.add_argument("--failed-out", type=Path, default=Path("index_failed_paths.txt"),
                       help="실패 문서 경로를 이 파일에 저장(재처리 큐 — `index --paths-file <파일>` 로 재실행). "
                            "실패가 없으면 만들지 않는다. 기본 ./index_failed_paths.txt")

    # 2단계 대상 추출: [그림] 마커·파일전용 별표·문서전용 파일 문서를 원본 JSON 에서 결정적으로 뽑는다
    pt = sub.add_parser("preprocess-targets",
                        help="전처리(2단계) 대상 문서 목록 추출 → --paths-file 입력용")
    pt.add_argument("--source", choices=_SOURCES, required=True)
    pt.add_argument("--input", type=Path, help="파일 또는 디렉터리 (생략 시 해당 source 의 REPO_PATH)")
    pt.add_argument("--limit", type=int)
    pt.add_argument("--out", type=Path, default=Path("preprocess_targets.txt"),
                    help="대상 경로 목록 저장 파일(기본 ./preprocess_targets.txt)")

    # 첨부파일 적재(파일 크롤링): hwp/pdf 등을 순회하며 전처리기로 청크화 후 적재
    files = sub.add_parser("index-files", help="첨부파일 적재(디렉터리 크롤링, 기본: source 의 repo 전체)")
    files.add_argument("--input", type=Path, help="파일 또는 디렉터리 (생략 시 해당 source 의 repo 경로)")
    files.add_argument("--recursive", action="store_true")
    files.add_argument("--limit", type=int)
    files.add_argument("--source", choices=_SOURCES, required=True)

    # 이미 전처리된 첨부 청크 JSON 을 그대로 적재(외부 전처리 API 결과 핸드오프)
    attachment = sub.add_parser("index-attachment-chunks", help="전처리된 청크 JSON 적재")
    attachment.add_argument("--input", type=Path, required=True)
    attachment.add_argument("--recursive", action="store_true")
    attachment.add_argument("--limit", type=int)
    attachment.add_argument("--source", choices=_SOURCES, required=True)

    # 증분(JSONL) 소비 — 첫 줄이 package_header 면 §6 package, 아니면 1세대 change-set(자동감지)
    changeset = sub.add_parser("index-changeset", help="증분 JSONL 재적재(§6 package 자동감지 / 1세대 change-set)")
    changeset.add_argument("--input", type=Path, required=True, help="증분 JSONL 경로(package 또는 change-set)")
    changeset.add_argument("--source", choices=_SOURCES, default=None,
                           help="§6 package 는 header 로 자동 결정(생략 가능). 1세대 change-set 은 필수.")
    changeset.add_argument("--data-root", type=Path,
                           help="payload 생략 시 git_path 를 읽을 루트(기본 INPUT_DATA_PATH)")

    cf = sub.add_parser("consume-folder",
                        help="폴더에 도착한 package(*.jsonl)들을 순차 소비 + 성공/실패 격리(스트리밍 소비자)")
    cf.add_argument("--dir", type=Path, required=True, help="package 들이 도착하는 폴더")
    cf.add_argument("--processed", type=Path, help="성공한 package 이동 폴더(기본 <dir>/processed)")
    cf.add_argument("--failed", type=Path, help="실패한 package 격리 폴더(기본 <dir>/failed)")
    cf.add_argument("--nack", type=Path,
                    help="격리 시 생산자에 되돌릴 목록을 남길 폴더(기본 PACKAGE_NACK_DIR 또는 <dir>/nack). "
                         "수집기의 PACKAGE_NACK_DIR 과 같은 경로여야 재발송/재수집이 걸린다")
    cf.add_argument("--source", choices=_SOURCES, default=None, help="header 에 source 없을 때만")

    search = sub.add_parser("search")
    search.add_argument("--query", required=True)
    search.add_argument("--limit", type=int, default=5)
    search.add_argument("--source", choices=_SOURCES, default="law")

    # Temporal 워커(genos 코드서빙/색인 자동화) — TEMPORAL_*·INDEX_TASK_QUEUE env 로 설정
    sub.add_parser("worker", help="Temporal 워커 실행(초기색인·§6 package 소비 activity 제공)")
    return root


def _repos_for(settings: Settings, sources: List[str]) -> List[tuple]:
    """동기화·색인할 (라벨, url, path, branch) 목록.

    **레포 3개(LAW/ADMRUL/SCHLPUBRUL) = 컬렉션 3개(law/admrul/schlpub), source 1:1.**
    SCHLPUB 경로가 admrul 과 같으면(옛 2레포 폴백 구성) 중복 clone/색인하지 않는다.
    """
    out, seen = [], set()

    def _add(label, url, path, branch):
        key = str(Path(path).resolve()) if path else ""
        if not key or key in seen:
            return
        seen.add(key)
        out.append((label, url, path, branch))

    for source in sources:
        if source == "law":
            _add("law", settings.law_repo_url, settings.law_repo_path, settings.law_repo_branch)
        elif source == "admrul":
            _add("admrul", settings.admrul_repo_url, settings.admrul_repo_path,
                 settings.admrul_repo_branch)
        elif source == "schlpub":
            _add("schlpub", settings.schlpub_repo_url, settings.schlpub_repo_path,
                 settings.schlpub_repo_branch)
    return out


def _index_targets(settings: Settings, source: str, input_path: Optional[Path],
                   paths: Optional[List[Path]]):
    """색인 대상을 (라벨, repo_url, repo_root, input, paths) 단위로 쪼갠다.

    레포 3개(LAW·ADMRUL·SCHLPUBRUL)와 source 가 1:1 이라 source 하나 = 레포 하나다.
    (2컬렉션 시절엔 `--source admrul` 이 ADMRUL+SCHLPUBRUL 둘을 덮었다 — 이제 학칙공단은
    `--source schlpub` 로 자기 컬렉션에 들어간다.)

    `--input`/`--paths-file` 로 대상을 콕 집었으면 그 경로를 **품고 있는 레포**를 repo_root 로
    삼는다. repo_root 가 틀리면 git_path(레포 상대경로)와 git_commit 이 어긋난다.
    경로가 **다른 source 의 레포** 밑이면(예: --source admrul 인데 SCHLPUBRUL 밑 경로) 조용히
    엉뚱한 컬렉션에 넣지 않도록 즉시 에러를 낸다."""
    repos = _repos_for(settings, [source])
    if input_path is None and paths is None:
        # input 자리는 None 으로 둔다 — 호출부가 "input 없음 = 레포 전체 재귀 순회" 로 판정한다.
        #   여기에 레포 경로를 채우면 recursive 휴리스틱(target_input is None)이 꺼져 최상위
        #   glob 만 돌아 0건이 된다(실측 회귀: --input 생략 index 가 아무것도 색인 안 함).
        return [(label, url, Path(path), None, None) for label, url, path, _b in repos]

    other_repos = [(lb, Path(p)) for lb, _u, p, _b in _repos_for(settings, list(_SOURCES))
                   if lb != source]

    def _owner(target: Path):
        t = Path(target).resolve()
        for label, url, path, _b in repos:
            try:
                t.relative_to(Path(path).resolve())
                return label, url, Path(path)
            except ValueError:
                continue
        for label, path in other_repos:                 # 다른 source 레포 소속 = 지정 실수
            try:
                t.relative_to(path.resolve())
            except ValueError:
                continue
            raise ValueError(
                f"경로 {target} 는 --source {source} 의 레포가 아니라 {label} 레포 밑입니다. "
                f"--source {label} 로 지정하세요.")
        return None

    if paths is not None:                       # --paths-file: 레포별로 묶는다
        grouped, unowned = {}, []
        for one in paths:
            owner = _owner(one)
            if owner is None:
                unowned.append(one)
                continue
            grouped.setdefault(owner, []).append(one)
        if unowned:                             # 레포 밖 경로는 대표 레포로(기존 동작 유지)
            label, url, path, _b = repos[0]
            grouped.setdefault((label, url, Path(path)), []).extend(unowned)
        return [(label, url, root, None, group) for (label, url, root), group in grouped.items()]

    owner = _owner(input_path) or (repos[0][0], repos[0][1], Path(repos[0][2]))
    return [(owner[0], owner[1], owner[2], input_path, None)]



def _run_sync(settings: Settings, sources: List[str]) -> bool:
    """요청한 레포들을 각각 독립적으로 clone/fetch한다. 하나가 실패해도 나머지는 계속."""
    ok = True
    for label, url, path, branch in _repos_for(settings, sources):
        try:
            result = git_sync.sync_repo(url, path, branch, timeout=settings.git_sync_timeout)
            print(f"[{label}] {result['action']} -> {result['path']} (commit {result['commit']})")
        except git_sync.GitSyncError as exc:
            ok = False
            print(f"[{label}] 동기화 실패: {exc}", file=sys.stderr)
    return ok


def main(argv=None):
    """CLI 입력을 해석해 health/sync/create/index/search 작업을 실행한다."""
    args = parser().parse_args(argv)
    settings = Settings.from_env()

    if args.command == "sync":
        ok = _run_sync(settings, _sources_for(args.source))
        raise SystemExit(0 if ok else 1)

    if args.command == "worker":
        import asyncio

        from . import worker as worker_module  # temporalio 는 워커 실행 시에만 import
        asyncio.run(worker_module.main())
        return

    try:
        # 연결은 **source 별로** 연다 — genos 는 컬렉션마다 API 키가 달라(RBAC) 한 연결로
        # 법령·행정규칙을 같이 다룰 수 없다.
        if args.command == "health":
            with WeaviateStore(settings) as store:
                ready = store.health()
            print("ready" if ready else "not ready")
            raise SystemExit(0 if ready else 1)

        if args.command == "create-collection":
            for source in _sources_for(args.source):
                _url, _path, _branch, collection = _repo_config(settings, source)
                with WeaviateStore(settings, source) as store:
                    created = store.create_collection(collection, args.recreate)
                print(f"[{source}] {collection}: " + ("컬렉션 생성 완료" if created else "이미 존재합니다"))
            return

        if args.command == "preprocess-targets":
            # 임베더·Weaviate 없이 원본 JSON 만 스캔 — 어느 시점(적재 전·후)이든 같은 결과.
            _url, repo_path, _branch, _coll = _repo_config(settings, args.source)
            input_path = Path(args.input) if args.input else Path(repo_path)
            result = find_preprocess_targets(input_path, args.source, recursive=True, limit=args.limit)
            paths = result.pop("paths")
            result["targets"] = len(paths)
            if paths:
                args.out.write_text("\n".join(paths) + "\n", encoding="utf-8")
                result["out"] = str(args.out)
                print(f"[2단계] 전처리 켜고: index --source {args.source} --paths-file {args.out}",
                      file=sys.stderr)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return

        if args.command in INDEX_COMMANDS:
            embedder = _embedder(settings)

            if args.command == "index":
                paths = None
                if args.paths_file:
                    paths = [Path(ln) for ln in args.paths_file.read_text(encoding="utf-8").splitlines()
                             if ln.strip()]
                source = args.source
                if source is None:
                    if paths:
                        source = _infer_source_from_path(paths[0])
                    else:
                        source = "all" if args.input is None else (_infer_source_from_path(args.input) or None)
                    if source is None:
                        print("오류: --input/--paths-file 경로가 LAW/ADMRUL/SCHLPUBRUL 어느 레포인지 판별할 수 "
                              "없습니다. --source law|admrul|schlpub 을 지정하세요.", file=sys.stderr)
                        raise SystemExit(1)
                if paths is not None and source in ("both", "all"):
                    print("오류: --paths-file 은 단일 source(law/admrul/schlpub)에서만 씁니다. --source 를 지정하세요.",
                          file=sys.stderr)
                    raise SystemExit(1)
                vector_cache = None
                if args.vector_cache:
                    from .pipeline import VectorCache
                    vector_cache = VectorCache(args.vector_cache)   # 1회 로드, 전 source 공유
                combined = {"by_source": {}}
                for src in _sources_for(source):
                    collection = _repo_config(settings, src)[3]
                    targets = _index_targets(settings, src, args.input, paths)
                    with WeaviateStore(settings, src) as store:
                        for label, source_repository, repo_path, target_input, target_paths in targets:
                            input_path = target_input or repo_path
                            recursive = args.recursive or target_input is None
                            git_commit = git_sync.current_commit(repo_path)
                            result = index_documents(
                                store, embedder, settings, input_path, src, repo_path, git_commit,
                                source_repository, recursive, args.limit,
                                skip_existing=args.skip_existing,
                                paths=target_paths, files_only=args.files_only,
                                vector_cache=vector_cache,
                            )
                            result["collection_count"] = store.count(collection)
                            combined["by_source"][label] = result
                # 실패 큐: 실패 문서 경로를 파일로 남겨 `--paths-file` 재실행에 바로 쓴다.
                #   (예전엔 errors 가 결과 JSON 안에만 있어 재처리 대상화가 수작업이었다.)
                failed_paths = sorted({e["file"] for r in combined["by_source"].values()
                                       for e in r.get("errors", [])
                                       if isinstance(e, dict) and str(e.get("file", "")).endswith(".json")})
                if failed_paths and args.failed_out:
                    args.failed_out.write_text("\n".join(failed_paths) + "\n", encoding="utf-8")
                    combined["failed_paths_file"] = str(args.failed_out)
                    print(f"[재처리 큐] 실패 문서 {len(failed_paths)}건 → {args.failed_out} "
                          f"(재실행: index --source {source} --paths-file {args.failed_out})", file=sys.stderr)
                print(json.dumps(combined, ensure_ascii=False, indent=2))
                raise SystemExit(_failure_code(combined))

            if args.command == "index-changeset":
                # §6 package 는 header 에 source 가 있다 — 연결을 열기 전에 먼저 확인해야
                # 맞는 키로 붙는다(1세대 change-set 은 --source 필수).
                source = args.source or peek_package_source(args.input)
                with WeaviateStore(settings, source) as store:
                    result = index_changeset(store, embedder, settings, args.input, args.source, args.data_root)
                    resolved = result.get("source") or source
                    if resolved:
                        result["collection_count"] = store.count(_repo_config(settings, resolved)[3])
                print(json.dumps(result, ensure_ascii=False, indent=2))
                raise SystemExit(_failure_code(result))

            if args.command == "consume-folder":
                # 스트리밍 소비자: 폴더의 package 들을 순차 소비(연결은 consume_folder 가 source 별로 연다).
                processed = args.processed or (args.dir / "processed")
                failed = args.failed or (args.dir / "failed")
                result = consume_folder(embedder, settings, args.dir, processed_dir=processed,
                                        failed_dir=failed, source_override=args.source,
                                        nack_dir=args.nack)
                print(json.dumps(result, ensure_ascii=False, indent=2))
                raise SystemExit(_failure_code(result))

            # index-files / index-attachment-chunks: --source 로 컬렉션·repo 확정(source=레포 1:1)
            collection = _repo_config(settings, args.source)[3]
            targets = _index_targets(settings, args.source, getattr(args, "input", None), None)
            results = {}
            with WeaviateStore(settings, args.source) as store:
                for label, _url, repo_path, target_input, _paths in targets:
                    input_path = target_input if target_input is not None else repo_path
                    recursive = args.recursive or target_input is None
                    if args.command == "index-files":
                        one = index_attachment_files(
                            store, embedder, input_path, collection, recursive, args.limit, settings=settings)
                    else:  # index-attachment-chunks
                        one = index_paths(store, embedder, input_path, collection, recursive, args.limit,
                                          loader=load_attachment_json)
                    one["collection_count"] = store.count(collection)
                    results[label] = one
            # 레포가 둘이면 by_source 로 감싼다 — _failure_code 가 중첩 결과를 그렇게 읽는다.
            result = {"by_source": results} if len(results) > 1 else next(iter(results.values()))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            raise SystemExit(_failure_code(result))

        embedder = _embedder(settings)
        vector = embedder.embed_query(args.query)
        _url, _path, _branch, collection = _repo_config(settings, args.source)
        with WeaviateStore(settings, args.source) as store:
            for rank, obj in enumerate(store.search(vector, collection, args.limit), 1):
                p, m = obj.properties, obj.metadata
                print(f"[{rank}] distance={m.distance:.6f}" if m.distance is not None else f"[{rank}]")
                for key in ("law_name", "law_type", "unit_type", "unit_no", "unit_title", "content", "provision_id", "version_uid", "git_path"):
                    print(f"{key}: {p.get(key)}")
                print()
    except Exception as exc:
        print(f"오류: {exc}", file=sys.stderr)
        raise SystemExit(1)
