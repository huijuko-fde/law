#!/usr/bin/env python3
"""적재 범위(scope) 동작 확인용 테스트 package 를 만든다.

**왜 필요한가.** 수집기(서버1)와 적재기(서버2)를 둘 다 띄우기 전에, "관리 화면 목록에 있는
법령만 임베딩된다"를 한 대에서 확인하려면 생산자 산출물을 흉내 낸 package 가 있어야 한다.
수집기의 `build_package_records`(temporal_law/pipeline/common/package.py:370-413)가 내보내는
것과 같은 3단 구조로 만든다 — package_header → document → package_footer.

  python scripts/make_test_packages.py --dir ./data
      # 법령 10건을 package 10개로. 기본값은 법령 5건 + 행정규칙 5건

  python scripts/make_test_packages.py --dir ./data --count 10 --one-file
      # 10건을 package **한 개**에 담는다(law_id 버퍼가 여러 건을 도는 경로 확인용)

만든 뒤 관리 화면 '데이터 > 법 목록 관리'에 **절반만** 등록하고 돌리면, totals 의
`scope_matched`/`scope_skipped` 로 걸러졌는지 바로 보인다.

  PYTHONPATH=src python -m law_indexer consume-folder --dir ./data

⚠ 테스트 데이터다. 실제 법령 내용이 아니므로 검색 품질 평가에는 쓰지 말 것.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

# 생산자(temporal_law/pipeline/common/package.py)가 쓰는 record_type 과 같아야 한다.
HEADER = "package_header"
DOCUMENT = "document"
FOOTER = "package_footer"

# (law_id, 법령명, source, 소관부처) — source 가 컬렉션·매퍼를 가른다.
#   law_id 는 실제 체계를 흉내 낸다: 법령은 6자리 zero-padding, 행정규칙은 패딩 없는 7자리.
#   (둘이 같은 컬렉션에 들어가도 안 섞이는지 보는 것이 이 테스트의 부수 목적이다.)
SAMPLES = [
    ("090001", "테스트 가사소송규칙", "law", "법원행정처"),
    ("090002", "테스트 민사집행법", "law", "법무부"),
    ("090003", "테스트 형의 집행 및 수용자의 처우에 관한 법률", "law", "법무부"),
    ("090004", "테스트 개인정보 보호법", "law", "개인정보보호위원회"),
    ("090005", "테스트 행정절차법", "law", "행정안전부"),
    ("9090001", "테스트 수용자 의료관리지침", "admrul", "법무부"),
    ("9090002", "테스트 교정시설 경비등급별 처우지침", "admrul", "법무부"),
    ("9090003", "테스트 보안장비 관리지침", "admrul", "법무부"),
    ("9090004", "테스트 정보통신 심의규정", "admrul", "방송통신심의위원회"),
    ("9090005", "테스트 전자금융감독규정", "admrul", "금융위원회"),
]


def _payload(law_id: str, name: str, source: str, ministry: str) -> dict:
    """tests/fixtures/law.json 과 같은 형태의 최소 법령 JSON.

    `basic_info.소관부처` 는 부처 기준 범위 판정이 읽는 유일한 자리다(law_scope._ministries_of).
    """
    version_uid = (f"{law_id}:900001:20260101" if source == "law"
                   else f"admrul:{law_id}:20260101")
    prefix = "law" if source == "law" else "admrul"
    return {
        "law_id": law_id,
        "mst": "900001",
        "version_uid": version_uid,
        "law_name": name,
        "law_type": "법률" if source == "law" else "고시",
        "promulgation_date": "20260101",
        "enforcement_date": "20260101",
        "revision_date": "20260101",
        "revision_type": "일부개정",
        "is_current": True,
        "is_future": False,
        "basic_info": {"소관부처": {"content": ministry, "소관부처코드": "9999"}},
        "source": {"source_url": f"https://www.law.go.kr/test/{law_id}"},
        "body": {"articles": [
            {"article_no": "제1조", "article_title": "목적",
             "chapter": "제1장 총칙",
             "content": f"제1조(목적) 이 규정은 {name}의 적재 범위 동작을 확인하기 위한 테스트 조문이다.",
             "enforcement_date": "20260101",
             "provision_id": f"{prefix}:{name.replace(' ', '')}#JO0001"},
            {"article_no": "제2조", "article_title": "정의",
             "chapter": "제1장 총칙",
             "content": f"제2조(정의) 이 규정에서 사용하는 용어의 뜻은 {name}에서 정하는 바에 따른다.",
             "enforcement_date": "20260101",
             "provision_id": f"{prefix}:{name.replace(' ', '')}#JO0002"},
        ]},
        "addenda": [],
        "appendices": [],
    }


def _document_record(law_id, name, source, ministry) -> dict:
    payload = _payload(law_id, name, source, ministry)
    return {
        "record_type": DOCUMENT, "op": "upsert",
        "law_id": law_id, "version_uid": payload["version_uid"],
        "payload": payload,
        "git_path": f"{name}/{name}.json",
    }


def _write(path: Path, package_id: str, source: str, body: list[dict]) -> int:
    header = {"record_type": HEADER, "package_id": package_id, "source": source,
              "mode": "delta", "strategy": "payload_with_optional_file_chunks",
              "generated_at": "2026-10-02T00:00:00Z"}
    footer = {"record_type": FOOTER, "package_id": package_id, "record_count": len(body)}
    lines = [header, *body, footer]
    # 생산자와 같은 원자적 쓰기 — 소비자의 *.jsonl glob 에 반쯤 쓰인 파일이 안 걸리게 한다.
    tmp = path.with_suffix(".jsonl.tmp")
    tmp.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in lines) + "\n",
                   encoding="utf-8")
    tmp.replace(path)
    return len(body)


def main() -> int:
    ap = argparse.ArgumentParser(description="적재 범위 확인용 테스트 package 생성")
    ap.add_argument("--dir", required=True, type=Path, help="소비 폴더 (consume-folder --dir 와 같은 값)")
    ap.add_argument("--count", type=int, default=len(SAMPLES),
                    help=f"만들 문서 수 (최대 {len(SAMPLES)}, 기본 전부)")
    ap.add_argument("--one-file", action="store_true",
                    help="전부 package 한 개에 담는다(기본은 문서당 한 개)")
    args = ap.parse_args()

    if not 1 <= args.count <= len(SAMPLES):
        print(f"--count 는 1~{len(SAMPLES)} 여야 합니다")
        return 1
    folder = args.dir
    folder.mkdir(parents=True, exist_ok=True)
    picked = SAMPLES[: args.count]

    if args.one_file:
        # source 가 섞이면 header 의 source 하나로 못 적으므로 source 별로 나눈다.
        total = 0
        for source in ("law", "admrul"):
            body = [_document_record(*s) for s in picked if s[2] == source]
            if not body:
                continue
            pid = f"test-{source}-bulk"
            n = _write(folder / f"{pid}.jsonl", pid, source, body)
            print(f"  {pid}.jsonl  ({source}, 문서 {n}건)")
            total += n
        print(f"\npackage {len(set(s[2] for s in picked))}개 · 문서 {total}건")
    else:
        for law_id, name, source, ministry in picked:
            pid = f"test-{source}-{law_id}"
            _write(folder / f"{pid}.jsonl", pid, source, [_document_record(law_id, name, source, ministry)])
            print(f"  {pid}.jsonl  [{source}] {name}")
        print(f"\npackage {len(picked)}개 · 문서 {len(picked)}건")

    print("\n관리 화면 '데이터 > 법 목록 관리' 에 아래에서 **일부만** 등록하고 돌리면 걸러집니다:")
    for _, name, source, _m in picked:
        print(f"  [{source}] {name}")
    print("\n  PYTHONPATH=src python -m law_indexer consume-folder --dir", folder)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
