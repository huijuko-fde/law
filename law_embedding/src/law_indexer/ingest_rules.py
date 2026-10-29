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
