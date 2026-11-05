"""수신 → 변환 → 소비 한 번의 판정 규칙. 순수 함수만 — Temporal 워크플로(sandbox)와 로컬 러너가 같이 쓴다."""
from typing import List, Tuple

HALT, OK, WAITING = "halt", "ok", "waiting"


def decide(reports: list, summary: dict, *, halt_on_partial: bool = True) -> Tuple[str, List[str]]:
    """실행 1회의 판정 — (halt | waiting | ok, 사유 목록).

    멈추는 것: 변환 실패 ZIP, 적재 실패(failed), 부분 완료(partial — 정책상 기본 멈춤), 품질 가드,
    잠금 충돌(한 번에 하나만 도는 구조에선 생기면 안 되는 신호). 옛 버전 건너뜀(version_skipped)은
    보호가 작동한 것이라 멈추지 않는다 — 기록만 남는다. waiting 은 '다음 실행이 이어 간다'."""
    reasons = []
    for r in reports:
        if r.get("status") == "failed":
            reasons.append(f"convert_failed:{r.get('zip')}")
    if summary.get("skipped"):
        reasons.append("lock_conflict")
    if summary.get("failed"):
        reasons.append(f"zip_failed:{summary['failed']}")
    if halt_on_partial and summary.get("partial"):
        reasons.append(f"zip_partial:{summary['partial']}")
    for guard in summary.get("quality_guard") or []:
        reasons.append(f"quality_guard:{guard.get('zip')}:{guard.get('reason')}")
    if reasons:
        return HALT, reasons
    pending_manifest = sum(1 for r in reports if r.get("status") == "pending_manifest")
    if summary.get("waiting") or summary.get("deferred") or pending_manifest:
        return WAITING, []
    return OK, []


def trim_report(report: dict) -> dict:
    """변환 보고에서 payload 목록(크다)을 뺀다 — 실행 기록·워크플로 결과에 싣는 모양."""
    return {k: v for k, v in report.items() if k != "payloads"}


# 재시도가 한 실행을 여러 시도로 나누면 앞선 시도의 보고가 사라진다 — 1차 시도가 ZIP 일부를 failed/ 로 옮긴 뒤
# 죽으면 2차 시도는 빈 폴더만 보고 ok 를 내린다. 시도별 결과를 모아 합친 것으로 판정한다.
_SUM_KEYS = ("total", "done", "partial", "failed", "duplicate", "version_skipped")
_LIST_KEYS = ("quality_guard", "zips", "version_skipped_laws")


def merge_reports(attempts: List[list]) -> list:
    """변환 보고 합치기 — 같은 ZIP 은 나중 시도가 이기고, 순서는 처음 본 순."""
    by = {}
    for reports in attempts:
        for r in reports or []:
            by[r.get("zip")] = r
    return list(by.values())


def merge_summaries(attempts: List[dict]) -> dict:
    """소비 요약 합치기 — 건수는 합, 목록은 이어 붙이고, waiting/deferred/skipped 는 마지막 시도 것."""
    attempts = [a for a in attempts if a]
    if not attempts:
        return {}
    out = dict(attempts[-1])
    for key in _SUM_KEYS:
        out[key] = sum(int(a.get(key) or 0) for a in attempts)
    for key in _LIST_KEYS:
        if any(key in a for a in attempts):
            out[key] = [item for a in attempts for item in (a.get(key) or [])]
    out["attempts"] = len(attempts)
    return out
