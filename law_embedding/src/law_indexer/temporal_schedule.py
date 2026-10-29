"""LAW-28 Temporal Schedule — 생성(멱등)·즉시 실행·일시정지/해제·상태·최근 실행.

수신 앱(ingest_app)이 Temporal 모드일 때 쓴다. Schedule 하나(`law-inbox`)가 LawInboxWorkflow 를 cron 으로
띄우고, 업로드가 오면 같은 Schedule 을 trigger 해 즉시 한 번 돈다(overlap=SKIP — 돌고 있으면 건너뛰고
다음 주기가 이어 받는다). 멈춤 사유는 워크플로 실패(ApplicationError) → pause_on_failure 로 Schedule 이
일시정지된다. **일시정지 = HALT** 이고, 해제는 unpause + trigger 다. 실행 이력·결과·실패 사유는 전부 Temporal
서버에 남아 `/temporal` UI 와 이 모듈의 조회 함수로 본다.
"""
from datetime import timedelta
from typing import List, Optional

from temporalio.client import (Client, Schedule, ScheduleActionStartWorkflow, ScheduleAlreadyRunningError,
                               ScheduleHandle, ScheduleOverlapPolicy, SchedulePolicy, ScheduleSpec,
                               WorkflowExecutionStatus, WorkflowFailureError)

from .worker_workflows import LawInboxRequest, LawInboxWorkflow

SCHEDULE_ID = "law-inbox"
WORKFLOW_TYPE = "LawInboxWorkflow"


async def ensure_schedule(client: Client, *, task_queue: str, req: LawInboxRequest, cron: str,
                          tz: str = "Asia/Seoul", schedule_id: str = SCHEDULE_ID) -> ScheduleHandle:
    """없으면 만들고 있으면 그대로 쓴다 — 기존 Schedule(사람이 바꿨을 수 있다)은 덮어쓰지 않는다."""
    try:
        return await client.create_schedule(
            schedule_id,
            Schedule(
                action=ScheduleActionStartWorkflow(LawInboxWorkflow.run, req, id=f"{schedule_id}-run",
                                                   task_queue=task_queue),
                spec=ScheduleSpec(cron_expressions=[cron], time_zone_name=tz),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP,
                                      catchup_window=timedelta(hours=1), pause_on_failure=True),
            ),
        )
    except ScheduleAlreadyRunningError:
        return client.get_schedule_handle(schedule_id)


async def describe(client: Client, schedule_id: str = SCHEDULE_ID) -> dict:
    d = await client.get_schedule_handle(schedule_id).describe()
    state, info, spec = d.schedule.state, d.info, d.schedule.spec
    return {
        "schedule_id": schedule_id,
        "paused": state.paused,
        "note": state.note,
        "cron": list(spec.cron_expressions),
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
        return {"status": status.name, "verdict": detail.get("verdict", "halt"), "message": str(cause),
                **{k: v for k, v in detail.items() if k != "verdict"}}
    except Exception as exc:                       # 취소·강제 종료 등
        return {"status": status.name, "message": str(exc)}


async def last_run(client: Client) -> Optional[dict]:
    runs = await recent_runs(client, limit=1)
    if not runs:
        return None
    return {**runs[0], **(await run_outcome(client, runs[0]["id"], runs[0]["run_id"]))}
