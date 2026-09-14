"""law_embedding 임베딩 파이프라인 Temporal 워커.

genos '코드 서빙'에 등록해 genos temporal 로 색인을 돌리기 위한 워커. 로컬 직접 실행:
    uv run python -m law_indexer.worker

환경변수(genos 는 플랫폼이 주입):
    TEMPORAL_ADDRESS   기본 localhost:7233
    TEMPORAL_NAMESPACE 기본 default
    TEMPORAL_TLS       기본 false (genos in-cluster 는 보통 평문)
    INDEX_TASK_QUEUE   기본 law-embedding
    (Weaviate·전처리기·모델 설정은 기존 Settings = .env/env 그대로:
     WEAVIATE_HTTP_HOST/PORT·WEAVIATE_API_KEY·DOC_PARSER_BASE_URL·EMBEDDING_MODEL …)

activity 는 동기(sentence-transformers·weaviate)라 ThreadPoolExecutor 에서 실행한다. 임베딩
모델은 워커 프로세스당 1회 로드(전역 캐시)해 activity 간 재사용한다. 워크플로 정의는
worker_workflows.py 에 분리(결정성·sandbox) — 여기선 activity 와 실행만.
"""
import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from .heartbeat import heartbeating

from . import git_sync
from .config import Settings
from .embedder import make_embedder
from .package import consume_package, peek_package_source
from .pipeline import index_documents
from .weaviate_store import WeaviateStore
from .worker_workflows import (
    CONSUME_PACKAGE_ACTIVITY, INITIAL_INDEX_ACTIVITY, ConsumePackageWorkflow,
    IndexRequest, InitialIndexWorkflow, PackageRequest,
)

log = logging.getLogger("law_embedding.worker")

# 워커 프로세스당 1회만 만드는 무거운 자원(모델·설정). activity 마다 모델을 다시 로드하지 않도록.
_settings: Optional[Settings] = None
_embedder = None


def _get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings


def _get_embedder():
    global _embedder
    if _embedder is None:
        _embedder = make_embedder(_get_settings())
    return _embedder


def _collection_for(settings: Settings, source: str) -> str:
    return settings.collection_for(source)


# 벡터 캐시(수 GB)는 요청마다 다시 읽지 않도록 경로별 1회 로드해 재사용한다.
_vector_caches: dict = {}


def _get_vector_cache(path: str):
    from .pipeline import VectorCache
    if path not in _vector_caches:
        _vector_caches[path] = VectorCache(Path(path))
    return _vector_caches[path]


@activity.defn(name=INITIAL_INDEX_ACTIVITY)
def initial_index(req: IndexRequest) -> dict:
    """초기 full 색인 — index_documents(조문 JSON + is_file_only 첨부 Doc Parser)를 그대로 돈다.

    전처리기(DOC_PARSER_BASE_URL)가 붙어 있으면 is_file_only 별표까지 처리하고, 없으면 그 첨부만
    실패로 남기고 본문은 계속 색인한다('전처리 없이' 벌크 = docparser 서비스 미연결)."""
    settings = _get_settings()
    from .prompt_scope import load_prompt_scope
    scope = load_prompt_scope()
    embedder = _get_embedder()
    source = req.source
    if source == "law":
        repo_url = settings.law_repo_url
    elif source == "schlpub":
        repo_url = settings.schlpub_repo_url
    else:
        repo_url = settings.admrul_repo_url
    repo_path = settings.repo_for(None, source)      # 배치 단위 기준 레포(문서별 라우팅은 미러가 한다)
    input_path = Path(req.input_path) if req.input_path else repo_path
    git_commit = git_sync.current_commit(repo_path)
    # 생존신호 — 워크플로의 heartbeat_timeout 과 짝이다. 없으면 워커가 죽었을 때
    #   start_to_close(24시간) 가 다 지나야 재시도된다(무인 운영에서 하루를 통째로 잃는다).
    with heartbeating(), WeaviateStore(settings, source) as store:
        cache = _get_vector_cache(req.vector_cache) if req.vector_cache else None
        result = index_documents(
            store, embedder, settings, input_path, source, repo_path, git_commit,
            repo_url, req.recursive, req.limit,
            skip_existing=req.skip_existing,
            preprocess_files=req.preprocess_files,
            embed_flush=req.embed_flush,
            vector_cache=cache, scope=scope)
        result["collection_count"] = store.count(_collection_for(settings, source))
    log.info("initial_index(source=%s) objects=%s", source, result.get("objects_success"))
    return result


@activity.defn(name=CONSUME_PACKAGE_ACTIVITY)
def consume_package_activity(req: PackageRequest) -> dict:
    """§6 JSONL package(또는 1세대 change-set) 소비 — consume_package 자동감지 경로.

    파일 record 를 내부 전처리기로 돌릴지는 PACKAGE_PREPROCESS_FILES 설정을 따른다(genos 는
    llmops-preprocess-api 를 DOC_PARSER_BASE_URL 로)."""
    settings = _get_settings()
    embedder = _get_embedder()
    # 컬렉션마다 API 키가 달라 연결 전에 source 를 확정한다(package_header 우선).
    source = req.source or peek_package_source(Path(req.package_path))
    with heartbeating(), WeaviateStore(settings, source) as store:
        result = consume_package(
            store, embedder, settings, Path(req.package_path), source_override=req.source)
    log.info("consume_package(%s) laws_indexed=%s deleted=%s",
             req.package_path, result.get("laws_indexed"), result.get("deleted_docs"))
    return result


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    # .env 를 먼저 읽는다 — Settings.from_env() 안의 load_dotenv() 가 아래 os.getenv 보다 늦으면
    # TEMPORAL_* 가 .env 에 있어도 기본값(localhost:7233/default)으로 붙어 기동이 실패한다.
    _get_settings()
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue = os.getenv("INDEX_TASK_QUEUE", "law-embedding")
    tls = (os.getenv("TEMPORAL_TLS", "false").strip().lower() in {"1", "true", "yes", "on"})

    # 모델을 먼저 로드해 첫 activity 지연·모델 오류를 워커 기동 시점에 드러낸다.
    _get_embedder()
    client = await Client.connect(address, namespace=namespace, tls=tls)

    # 동기 activity(임베딩·weaviate)는 스레드풀에서 실행. 색인은 CPU·I/O 혼합이라 소수 워커면 충분.
    # ⚠ max_concurrent_activities 를 **스레드풀 크기와 같게** 못 박는다. 안 주면 기본 100 이라
    #   워커가 최대 100건을 물어와 executor 큐에 쌓는데, start_to_close 시계는 큐에서 대기하는
    #   동안에도 돈다 — 실행도 못 해 보고 타임아웃 나는 activity 가 생긴다.
    workers = int(os.getenv("INDEX_ACTIVITY_WORKERS", "2"))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[InitialIndexWorkflow, ConsumePackageWorkflow],
            activities=[initial_index, consume_package_activity],
            activity_executor=executor,
            max_concurrent_activities=workers,
        )
        log.info("[law-embedding worker] task_queue=%s @ %s (ns=%s, tls=%s) 대기 중…",
                 task_queue, address, namespace, tls)
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
