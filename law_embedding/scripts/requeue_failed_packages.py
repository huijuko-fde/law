#!/usr/bin/env python3
"""격리된 package 를 소비 폴더로 되돌려 다시 태운다.

**왜 필요한가.** `consume_folder` 는 실패한 package 를 `failed/` 로 격리하면서 `nack/` 에
"이 문서들이 못 들어갔다" 를 남긴다. 원래 설계는 그 nack 을 **생산자**가 읽어 재발송하는
것인데, 수집기 → 적재기가 **단방향 파일 전달**인 구성에서는 생산자가 nack 을 읽을 경로가
아예 없다. 그래서 되돌림을 소비 측에서 끝낸다 — 폴더 안에서 파일을 옮기는 것뿐이라
역방향 통신이 필요 없다.

되돌려도 안전한 이유는 소비자가 **멱등**하기 때문이다. chunk_id → uuid5 결정적 UUID 라
같은 package 를 다시 태우면 중복 적재가 아니라 덮어쓰기가 된다. 격리 시점에
`_clear_attempts` 가 재시도 카운터를 지우므로 되돌린 package 는 재시도 5회를 새로 받는다.

**기본은 `transient_exhausted` 만 되돌린다.** `permanent` 는 같은 입력에 같은 결과라
그대로 되돌리면 5회 더 실패하고 다시 격리될 뿐이다 — 원인을 고친 뒤 `--kind permanent`
로 명시해 되돌린다.

  python scripts/requeue_failed_packages.py --dir /mnt/handoff/packages
      # 기본 dry-run. 무엇이 되돌아갈지만 보여준다

  python scripts/requeue_failed_packages.py --dir /mnt/handoff/packages --apply
      # 실제로 되돌린다 (transient_exhausted 만)

  python scripts/requeue_failed_packages.py --dir ... --kind permanent --apply
      # 원인을 고친 뒤 결정적 실패분까지

종료코드: 0 = 되돌릴 것이 있었거나 없었거나 정상, 1 = 폴더/인자 오류.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

# _write_nack 이 쓰는 값 (law_indexer/pipeline.py:406)
KIND_TRANSIENT = "transient_exhausted"
KIND_PERMANENT = "permanent"
# nack 이 없는 격리분의 취급 — nack 은 law_ids 가 비면 아예 안 써진다(pipeline.py:400-401).
KIND_UNKNOWN = "unknown"


def _read_kind(nack_path: Path) -> str:
    """nack 파일에서 격리 사유를 읽는다. 없거나 깨졌으면 unknown."""
    try:
        data = json.loads(nack_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return KIND_UNKNOWN
    kind = data.get("kind")
    return kind if kind in (KIND_TRANSIENT, KIND_PERMANENT) else KIND_UNKNOWN


def _law_count(nack_path: Path) -> int:
    try:
        return len(json.loads(nack_path.read_text(encoding="utf-8")).get("law_ids") or [])
    except (OSError, json.JSONDecodeError):
        return 0


def collect(folder: Path, failed_dir: Path, nack_dir: Path) -> list[dict]:
    """failed/ 의 package 들을 사유와 함께 모은다. 이미 inbox 에 같은 이름이 있으면 제외."""
    out = []
    for pkg in sorted(failed_dir.glob("*.jsonl")):
        package_id = pkg.name[: -len(".jsonl")]
        nack = nack_dir / f"{package_id}.nack.json"
        out.append({
            "path": pkg,
            "package_id": package_id,
            "kind": _read_kind(nack) if nack.exists() else KIND_UNKNOWN,
            "laws": _law_count(nack) if nack.exists() else 0,
            "nack": nack if nack.exists() else None,
            # inbox 에 같은 이름이 살아 있으면 덮어쓰지 않는다 — 소비 중일 수 있다.
            "blocked": (folder / pkg.name).exists(),
        })
    return out


def requeue(item: dict, folder: Path) -> None:
    """package 를 inbox 로 되돌리고 격리 부산물을 치운다.

    .errors.json 과 nack 은 되돌린 뒤엔 의미가 없다 — 남겨두면 다음 실행이
    같은 건을 또 후보로 올린다.
    """
    shutil.move(str(item["path"]), str(folder / item["path"].name))
    errors = item["path"].with_name(item["path"].name + ".errors.json")
    errors.unlink(missing_ok=True)
    if item["nack"] is not None:
        item["nack"].unlink(missing_ok=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="격리된 package 를 소비 폴더로 되돌린다")
    ap.add_argument("--dir", required=True, type=Path,
                    help="소비 폴더 (consume-folder --dir 와 같은 값)")
    ap.add_argument("--failed", type=Path, default=None, help="기본 <dir>/failed")
    ap.add_argument("--nack", type=Path, default=None, help="기본 <dir>/nack")
    ap.add_argument("--kind", choices=[KIND_TRANSIENT, KIND_PERMANENT, KIND_UNKNOWN, "all"],
                    default=KIND_TRANSIENT,
                    help=f"되돌릴 격리 사유 (기본 {KIND_TRANSIENT})")
    ap.add_argument("--max", type=int, default=50,
                    help="한 번에 되돌릴 최대 건수 (기본 50). 한꺼번에 쏟아 소비를 막지 않기 위한 상한")
    ap.add_argument("--apply", action="store_true",
                    help="실제로 되돌린다. 없으면 dry-run")
    args = ap.parse_args()

    folder = args.dir
    failed_dir = args.failed or (folder / "failed")
    nack_dir = args.nack or (folder / "nack")

    if not folder.is_dir():
        print(f"소비 폴더가 없습니다: {folder}", file=sys.stderr)
        return 1
    if not failed_dir.is_dir():
        print(f"격리 폴더가 없습니다(격리된 package 가 없다는 뜻일 수 있습니다): {failed_dir}")
        return 0

    items = collect(folder, failed_dir, nack_dir)
    if not items:
        print(f"격리된 package 가 없습니다: {failed_dir}")
        return 0

    wanted = [i for i in items
              if (args.kind == "all" or i["kind"] == args.kind) and not i["blocked"]]
    picked = wanted[: args.max]

    by_kind: dict[str, int] = {}
    for i in items:
        by_kind[i["kind"]] = by_kind.get(i["kind"], 0) + 1
    print(f"격리 {len(items)}건 — " + " · ".join(f"{k} {v}" for k, v in sorted(by_kind.items())))

    blocked = [i for i in items if i["blocked"]]
    if blocked:
        print(f"  건너뜀(같은 이름이 소비 폴더에 있음) {len(blocked)}건: "
              + ", ".join(i["package_id"] for i in blocked[:5])
              + (" …" if len(blocked) > 5 else ""))

    if not picked:
        print(f"되돌릴 대상 없음 (--kind {args.kind})")
        return 0

    if len(wanted) > len(picked):
        print(f"  ⚠ 대상 {len(wanted)}건 중 {len(picked)}건만 처리합니다 (--max {args.max}). "
              f"남은 {len(wanted) - len(picked)}건은 다음 실행에서.")

    verb = "되돌림" if args.apply else "되돌릴 예정"
    for i in picked:
        laws = f"법령 {i['laws']}건" if i["laws"] else "법령수 미상"
        print(f"  [{i['kind']}] {i['package_id']}  ({laws})  → {verb}")
        if args.apply:
            requeue(i, folder)

    if args.apply:
        print(f"\n{len(picked)}건을 소비 폴더로 되돌렸습니다. 다음 consume-folder 실행이 태웁니다.")
    else:
        print(f"\n[dry-run] 실제로 되돌리려면 --apply 를 붙이세요.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
