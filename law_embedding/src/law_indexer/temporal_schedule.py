"""LAW-28 Temporal Schedule — 생성(멱등)·즉시 실행·일시정지/해제·상태·최근 실행.

수신 앱(ingest_app)이 Temporal 모드일 때 쓴다. Schedule 하나(`law-inbox`)가 LawInboxWorkflow 를 cron 으로
띄우고, 업로드가 오면 같은 Schedule 을 trigger 해 즉시 한 번 돈다(overlap=SKIP — 돌고 있으면 건너뛰고
다음 주기가 이어 받는다). 멈춤 사유는 워크플로 실패(ApplicationError) → pause_on_failure 로 Schedule 이
일시정지된다. **일시정지 = HALT** 이고, 해제는 unpause + trigger 다. 실행 이력·결과·실패 사유는 전부 Temporal
서버에 남아 `/temporal` UI 와 이 모듈의 조회 함수로 본다.
"""
import os
from datetime import timedelta
from typing import List, Optional

from temporalio.client import (Client, Schedule, ScheduleActionStartWorkflow, ScheduleAlreadyRunningError,
                               ScheduleHandle, ScheduleOverlapPolicy, SchedulePolicy, ScheduleSpec,
                               ScheduleUpdate, ScheduleUpdateInput, WorkflowExecutionStatus,
                               WorkflowFailureError)

from .worker_workflows import LawInboxRequest, LawInboxWorkflow

SCHEDULE_ID = "law-inbox"
WORKFLOW_TYPE = "LawInboxWorkflow"
# 한 실행의 상한. 워커가 죽어 Running 에 고착된 실행이 overlap=SKIP 으로 뒤 실행을 영원히 막지 않게 —
# 타임아웃도 실패라 pause_on_failure 로 일시정지(HALT)돼 사람 눈에 띈다.
RUN_TIMEOUT_HOURS = int(os.getenv("LAW_RUN_TIMEOUT_HOURS", "12"))


def definition(*, task_queue: str, req: LawInboxRequest, cron: str, tz: str = "Asia/Seoul",
               schedule_id: str = SCHEDULE_ID) -> Schedule:
    """Schedule 정의 한 곳 — 생성과 갱신이 같은 것을 쓴다."""
    return Schedule(
        action=ScheduleActionStartWorkflow(LawInboxWorkflow.run, req, id=f"{schedule_id}-run",
                                           task_queue=task_queue,
                                           run_timeout=timedelta(hours=RUN_TIMEOUT_HOURS)),
        spec=ScheduleSpec(cron_expressions=[cron], time_zone_name=tz),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP,
                              catchup_window=timedelta(hours=1), pause_on_failure=True),
    )


async def ensure_schedule(client: Client, *, task_queue: str, req: LawInboxRequest, cron: str,
                          tz: str = "Asia/Seoul", schedule_id: str = SCHEDULE_ID) -> ScheduleHandle:
    """없으면 만들고, 있으면 action·spec·policy 를 현재 설정으로 **갱신**한다(일시정지·메모 등 state 는 보존).

    Schedule 은 등록 때의 LAW_ROOT·큐·cron 을 스냅샷으로 박아 두므로, 갱신하지 않으면 env 를 바꿔
    재배포해도 옛 값으로 계속 돈다 — 옛 root 를 빈 채로 훑으며 매번 ok 를 내는 조용한 정지."""
    sched = definition(task_queue=task_queue, req=req, cron=cron, tz=tz, schedule_id=schedule_id)
    try:
        return await client.create_schedule(schedule_id, sched)
    except ScheduleAlreadyRunningError:
        handle = client.get_schedule_handle(schedule_id)

        def _update(inp: ScheduleUpdateInput) -> ScheduleUpdate:
            sched.state = inp.description.schedule.state          # 사람이 건 일시정지·메모는 그대로
            return ScheduleUpdate(schedule=sched)

        await handle.update(_update)
        return handle


def _calendar_text(spec) -> List[str]:
    """서버는 cron 문자열을 calendar 로 정규화해 돌려준다(describe 의 cron_expressions 는 늘 빈 목록) — 사람이
    읽을 수 있게 복원한다."""
    def rng(items) -> str:
        parts = []
        for r in items or []:
            s = f"{r.start}" if r.start == r.end else f"{r.start}-{r.end}"
            parts.append(s + (f"/{r.step}" if getattr(r, "step", 1) and r.step > 1 else ""))
        return ",".join(parts) or "*"
    return [f"minute={rng(c.minute)} hour={rng(c.hour)} dow={rng(c.day_of_week)} dom={rng(c.day_of_month)}"
            for c in (spec.calendars or [])]


async def describe(client: Client, schedule_id: str = SCHEDULE_ID) -> dict:
    d = await client.get_schedule_handle(schedule_id).describe()
    state, info, spec = d.schedule.state, d.info, d.schedule.spec
    return {
        "schedule_id": schedule_id,
        "paused": state.paused,
        "note": state.note,
        "cron": list(spec.cron_expressions) or _calendar_text(spec),
        "time_zone": spec.time_zone_name,
        "next_run_at": [t.isoformat() for t in info.next_action_times[:3]],
        "running": [getattr(a, "workflow_id", None) for a in info.running_actions],
        "runs_total": info.num_actions,
        "runs_skipped_overlap": info.num_actions_skipped_overlap,
        "last_action_at": info.recent_actions[-1].started_at.isoformat() if info.recent_actions else None,
    }


async def trigger(client: Client, schedule_id: str = SCHEDULE_ID) -> None:
    """즉시 한 번. 돌고 있으면 SKIP(다음 주기가 이어 받는다)."""
    await client.get_schedule_handle(schedule_id).trigger(overlap=ScheduleOverlapPolicy.SKIP)


async def pause(client: Client, note: str, schedule_id: str = SCHEDULE_ID) -> None:
    await client.get_schedule_handle(schedule_id).pause(note=note)


async def unpause(client: Client, note: str, schedule_id: str = SCHEDULE_ID) -> None:
    await client.get_schedule_handle(schedule_id).unpause(note=note)


async def recent_runs(client: Client, limit: int = 20) -> List[dict]:
    """최근 실행(최신순) — Temporal visibility. 리텐션(llmops 네임스페이스 7일) 안의 것만."""
    out = []
    async for wf in client.list_workflows(f'WorkflowType="{WORKFLOW_TYPE}"', limit=limit):
        out.append({"id": wf.id, "run_id": wf.run_id,
                    "status": wf.status.name if wf.status else None,
                    "started_at": wf.start_time.isoformat() if wf.start_time else None,
                    "closed_at": wf.close_time.isoformat() if wf.close_time else None})
    return out


async def run_outcome(client: Client, workflow_id: str, run_id: Optional[str] = None) -> dict:
    """끝난 실행의 결과(또는 멈춤 사유). 돌고 있으면 status 만."""
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    status = (await handle.describe()).status
    if status == WorkflowExecutionStatus.RUNNING:
        return {"status": "RUNNING"}
    try:
        return {"status": status.name, **(await handle.result())}
    except WorkflowFailureError as exc:
        cause = exc.cause
        details = list(getattr(cause, "details", None) or [])
        detail = details[0] if details and isinstance(details[0], dict) else {}
        # activity 실패는 ActivityError → ApplicationError 두 단계라 안쪽 원인까지 이어 붙인다.
        chain, inner = [], cause
        while inner is not None and len(chain) < 4:
            chain.append(str(inner))
            inner = getattr(inner, "cause", None) or getattr(inner, "__cause__", None)
        return {"status": status.name, "verdict": detail.get("verdict", "halt"), "message": " ← ".join(chain),
                **{k: v for k, v in detail.items() if k != "verdict"}}
    except Exception as exc:                       # 취소·강제 종료 등
        return {"status": status.name, "message": str(exc)}


async def last_run(client: Client) -> Optional[dict]:
    runs = await recent_runs(client, limit=1)
    if not runs:
        return None
    return {**runs[0], **(await run_outcome(client, runs[0]["id"], runs[0]["run_id"]))}
