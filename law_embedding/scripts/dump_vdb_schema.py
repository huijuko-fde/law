#!/usr/bin/env python3
"""납품용 벡터DB 정의서(genos_vdb_schema_*.json) 3개를 코드에서 뽑는다.

`docs/schema.md` 가 "정의의 유일한 출처는 코드 상수 weaviate_store.SCHEMA" 라고 못박고
정의서 JSON 3개를 참조하는데, 정작 그 JSON 과 생성 수단이 레포에 없었다. 손으로 관리하면
코드와 어긋나므로 여기서 항상 다시 뽑는다.

  python scripts/dump_vdb_schema.py            # 레포 루트에 3개 파일 기록
  python scripts/dump_vdb_schema.py --check    # 기록하지 않고 현재 파일과 일치하는지만 검사(CI)

`--check` 는 어긋나면 종료코드 1 — 스키마를 바꾸고 정의서를 안 갱신한 채 머지되는 걸 막는다.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from law_indexer.weaviate_store import SCHEMA  # noqa: E402

# 컬렉션 3종 — (파일 접미, 기본 클래스명, 설명 앞머리)
COLLECTIONS = [
    ("law", "LegalProvisionIndex",
     "법령(법률·시행령·시행규칙 = LAW 레포)"),
    ("admrul", "AdmrulProvisionIndex",
     "행정규칙(고시·훈령·예규 = ADMRUL 레포)"),
    ("schlpubrul", "SchlPubRulProvisionIndex",
     "학칙공단(학칙·공단정관·공공기관 규정 = SCHLPUBRUL 레포)"),
]

_DTYPE = {"text": "text", "text[]": "text[]", "int": "int", "date": "date", "boolean": "boolean"}


def _description(head: str) -> str:
    n_filter = sum(1 for s in SCHEMA if s["filterable"])
    n_search = sum(1 for s in SCHEMA if s["searchable"])
    n_show = len(SCHEMA) - n_filter - n_search
    return (
        f"{head} 조문/부칙/별표/개정문/파일 청크. self-provided vector(외부 임베딩 주입, "
        f"vectorizer none) · HNSW cosine · BM25(b=0.75, k1=1.2)는 search_text 단일 속성"
        f"(kagome_kr 한국어 형태소 — Weaviate 에 ENABLE_TOKENIZER_KAGOME_KR 모듈 필요) · "
        f"필터(indexFilterable) {n_filter} · 키워드검색 {n_search} · 표시 {n_show} · "
        f"총 {len(SCHEMA)} 속성. 세 컬렉션(법령/행정규칙/학칙공단)이 같은 속성을 공유하고 "
        f"클래스 이름만 다르다(데이터 레포당 컬렉션 하나)."
    )


def _property(spec: dict) -> dict:
    """SCHEMA 항목 하나 → Weaviate 속성 정의(REST /v1/schema 형식)."""
    return {
        "name": spec["name"],
        "description": spec["description"],
        "dataType": [_DTYPE[spec["type"]]],
        "indexFilterable": bool(spec["filterable"]),
        "indexSearchable": bool(spec["searchable"]),
        "indexRangeFilters": bool(spec.get("range_filters")),
        # text 계열이 아니면 토크나이저 개념이 없다(Weaviate 가 null 로 돌려준다).
        "tokenization": (spec.get("tokenization") or ("word" if spec["type"] in ("text", "text[]") else None)),
        "moduleConfig": {"none": {}},
    }


def build(class_name: str, head: str) -> dict:
    """create_collection 이 실제로 만드는 것과 같은 정의를 문서 형식으로 조립한다.

    복제 계수·샤드 수는 **노드 수를 넘길 수 없어** 코드에서 env 로 뺐다
    (WEAVIATE_REPLICATION_FACTOR / WEAVIATE_SHARD_COUNT). 정의서에는 운영 목표값 3 을 적는다."""
    return {
        "class": class_name,
        "description": _description(head),
        "vectorizer": "none",
        "vectorIndexType": "hnsw",
        "vectorIndexConfig": {
            "distanceMetric": "cosine",
            "efConstruction": 128,
            "maxConnections": 32,
            "filterStrategy": "acorn",
            "cleanupIntervalSeconds": 300,
            "dynamicEfMin": 100, "dynamicEfMax": 500, "dynamicEfFactor": 8, "ef": -1,
            "flatSearchCutoff": 40000, "skip": False,
            "vectorCacheMaxObjects": 1000000000000,
        },
        "invertedIndexConfig": {
            "bm25": {"b": 0.75, "k1": 1.2},
            "cleanupIntervalSeconds": 60,
            "indexNullState": False,
            "indexPropertyLength": False,
            "indexTimestamps": False,
            "stopwords": {"preset": "en"},
        },
        "replicationConfig": {
            "factor": 3, "asyncEnabled": True, "deletionStrategy": "TimeBasedResolution",
        },
        "shardingConfig": {"desiredCount": 3, "virtualPerPhysical": 128},
        "multiTenancyConfig": {"enabled": False, "autoTenantCreation": False,
                               "autoTenantActivation": False},
        "properties": [_property(s) for s in SCHEMA],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="기록하지 않고 현재 파일과 일치하는지만 검사한다(어긋나면 종료코드 1)")
    args = ap.parse_args()

    drift = []
    for suffix, class_name, head in COLLECTIONS:
        path = ROOT / f"genos_vdb_schema_{suffix}.json"
        body = json.dumps(build(class_name, head), ensure_ascii=False, indent=2) + "\n"
        if args.check:
            current = path.read_text(encoding="utf-8") if path.exists() else ""
            state = "일치" if current == body else ("없음" if not current else "불일치")
            print(f"  {path.name:34s} {state}")
            if state != "일치":
                drift.append(path.name)
        else:
            path.write_text(body, encoding="utf-8")
            print(f"  {path.name:34s} 기록 ({len(SCHEMA)} 속성)")

    if drift:
        print(f"\n정의서가 코드({len(SCHEMA)}속성)와 어긋납니다: {', '.join(drift)}")
        print("  python scripts/dump_vdb_schema.py 로 다시 뽑으세요.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
