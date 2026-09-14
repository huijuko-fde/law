"""law_embedding 임베딩 파이프라인의 Temporal 워크플로 정의.

genos '코드 서빙'(worker 등록 → genos temporal 로 실행)에 얹기 위한 워크플로다. 워크플로 코드는
**결정적**이어야 하고 temporalio 가 sandbox 로 재import 하므로, 여기서는 무거운 모듈
(weaviate/torch/sentence-transformers/law_indexer)을 절대 import 하지 않는다 — 실제 색인은
worker.py 의 activity 가 하고, 워크플로는 activity 를 **이름 문자열**로만 호출한다.

- InitialIndexWorkflow  : 초기 full 색인(index_documents). genon-2 에서 git pull 후 1회.
- ConsumePackageWorkflow: §6 JSONL package 소비(index-changeset 자동감지). 매 증분.

activity 입력 dataclass 도 여기 둔다(순수 dataclass 라 sandbox 안전) — worker.py 의 activity 와
워크플로가 같은 타입을 공유한다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

# activity 등록 이름 — 워크플로는 이 문자열로 activity 를 호출(모듈 import 결합 회피).
INITIAL_INDEX_ACTIVITY = "law_embedding_initial_index"
CONSUME_PACKAGE_ACTIVITY = "law_embedding_consume_package"


@dataclass
class IndexRequest:
    """초기 full 색인 요청.

    아래 셋은 **요청 단위 오버라이드** — 워커의 env(.env)를 바꾸거나 재시작하지 않고
    요청마다 다르게 돌릴 수 있다(None/기본이면 env 그대로). 어떤 환경(genos·VM·로컬)에
    워커가 떠 있든 같은 요청으로 같은 동작을 보장하기 위한 축이다.
      - skip_existing   : 청크가 이미 있는 문서는 건너뜀(중단 후 이어받기).
      - preprocess_files: 첨부/[그림] 전처리 on/off (None=env INDEX_PREPROCESS_FILES).
                          1단계 본문만(false) → 2단계 전처리 대상만(true) 전략을 요청으로 표현.
      - embed_flush     : 임베딩 벌크 플러시 크기 (None=env INDEX_EMBED_FLUSH, 기본 256).
    """

    source: str = "law"                 # "law" | "admrul" | "schlpub"
    input_path: Optional[str] = None    # 생략 시 해당 source 의 repo 경로 전체
    recursive: bool = True
    limit: Optional[int] = None
    skip_existing: bool = False
    preprocess_files: Optional[bool] = None
    embed_flush: Optional[int] = None
    vector_cache: Optional[str] = None  # migrate export parquet 경로 — 임베딩 재사용(재적재 가속)


@dataclass
class PackageRequest:
    """§6 JSONL package(또는 1세대 change-set) 소비 요청."""

    package_path: str
    source: Optional[str] = None         # None 이면 package_header 로 자동 결정(1세대는 필수)


# 생존신호가 이 시간 동안 안 오면 워커가 죽은 것으로 보고 즉시 재시도한다.
# activity 쪽 heartbeat 주기는 30초(heartbeat.INTERVAL) — 그 10배로 잡아 일시적 지연·GC 로
# 인한 오발동(정상 activity 취소)을 피하면서, 크래시 복구는 start_to_close(6~24시간)가 아니라
# 이 시간 안에 일어나게 한다.
# INDEX_HEARTBEAT_TIMEOUT_MIN=0 이면 끔 — 오발동(정상 activity 취소) 시 현장 스위치.
_HB_MIN = int(os.getenv("INDEX_HEARTBEAT_TIMEOUT_MIN", "5"))
_HB = {"heartbeat_timeout": timedelta(minutes=_HB_MIN)} if _HB_MIN > 0 else {}


@workflow.defn
class InitialIndexWorkflow:
    """초기 full 색인 — 색인 자체가 멱등이라 재시도해도 안전(중복은 UUID 교체)."""

    @workflow.run
    async def run(self, req: IndexRequest) -> dict:
        return await workflow.execute_activity(
            INITIAL_INDEX_ACTIVITY, req,
            start_to_close_timeout=timedelta(hours=24),
            **_HB,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )


@workflow.defn
class ConsumePackageWorkflow:
    """§6 package 소비 — 같은 청크는 결정적 UUID 로 upsert 되고, 새 버전 성공 후 옛 버전만 정리해 재시도 안전(멱등)."""

    @workflow.run
    async def run(self, req: PackageRequest) -> dict:
        return await workflow.execute_activity(
            CONSUME_PACKAGE_ACTIVITY, req,
            start_to_close_timeout=timedelta(hours=6),
            **_HB,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
