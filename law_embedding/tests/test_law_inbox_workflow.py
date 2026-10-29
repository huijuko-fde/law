"""LawInboxWorkflow — Temporal 테스트 서버(time-skipping)에서 가짜 activity 로 흐름·판정·멈춤을 검증.

temporalio.testing 이 테스트 서버 바이너리를 내려받아야 한다(최초 1회, 네트워크). 못 받으면 건너뛴다."""
import uuid

import pytest

temporalio = pytest.importorskip("temporalio")
from temporalio import activity  # noqa: E402
from temporalio.client import WorkflowFailureError  # noqa: E402
from temporalio.exceptions import ApplicationError  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from law_indexer.worker_workflows import (CONSUME_ZIP_ACTIVITY, CONVERT_INBOX_ACTIVITY,  # noqa: E402
                                          LawInboxRequest, LawInboxWorkflow)


def _summary(**over):
    base = {"total": 0, "done": 0, "partial": 0, "failed": 0, "duplicate": 0, "waiting": 0, "deferred": 0,
            "version_skipped": 0, "quality_guard": [], "zips": []}
    base.update(over)
    return base


@pytest.fixture(scope="module")
def env():
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        e = loop.run_until_complete(WorkflowEnvironment.start_time_skipping())
    except Exception as exc:                        # 바이너리 다운로드 불가(폐쇄망 등)
        pytest.skip(f"temporal 테스트 서버를 띄우지 못함: {exc}")
    yield loop, e
    loop.run_until_complete(e.shutdown())
    loop.close()


def _run(env, convert_fn, consume_fn, req=None):
    loop, e = env

    @activity.defn(name=CONVERT_INBOX_ACTIVITY)
    async def convert(r: LawInboxRequest) -> list:
        return convert_fn(r)

    @activity.defn(name=CONSUME_ZIP_ACTIVITY)
    async def consume(r: LawInboxRequest) -> dict:
        return consume_fn(r)

    async def go():
        queue = f"law-test-{uuid.uuid4().hex[:8]}"
        async with Worker(e.client, task_queue=queue, workflows=[LawInboxWorkflow], activities=[convert, consume]):
            return await e.client.execute_workflow(
                LawInboxWorkflow.run, req or LawInboxRequest(root="/tmp/law"),
                id=f"law-inbox-test-{uuid.uuid4().hex[:8]}", task_queue=queue)

    return loop.run_until_complete(go())


def test_ok_path_returns_result(env):
    result = _run(env, lambda r: [{"zip": "a.zip", "status": "converted", "errors": []}],
                  lambda r: _summary(total=1, done=1))
    assert result["verdict"] == "ok" and result["sweeps"] == 1
    assert result["convert"][0]["zip"] == "a.zip" and result["consume"]["done"] == 1


def test_waiting_resweeps_until_done(env):
    """소비가 재시도 대기(waiting)면 잠시 자고 다시 소비한다 — 테스트 서버는 시간을 건너뛴다."""
    calls = []

    def consume(r):
        calls.append(1)
        return _summary(total=1, waiting=1) if len(calls) < 3 else _summary(total=1, done=1)

    result = _run(env, lambda r: [], consume, LawInboxRequest(root="/tmp/law", max_sweeps=5, sweep_wait_min=1))
    assert result["verdict"] == "ok" and result["sweeps"] == 3 and len(calls) == 3


def test_halt_fails_workflow_with_details(env):
    """멈춤 사유 → non-retryable ApplicationError(LawInboxHalt). details 에 결과 전체가 실린다."""
    with pytest.raises(WorkflowFailureError) as info:
        _run(env, lambda r: [{"zip": "bad.zip", "status": "failed", "errors": [{"error": "x"}]}],
             lambda r: _summary(total=1, partial=1))
    cause = info.value.cause
    assert isinstance(cause, ApplicationError) and cause.type == "LawInboxHalt" and cause.non_retryable
    assert "convert_failed:bad.zip" in str(cause) and "zip_partial:1" in str(cause)
    detail = cause.details[0]
    assert detail["verdict"] == "halt" and detail["reasons"] == ["convert_failed:bad.zip", "zip_partial:1"]


def test_partial_tolerated_by_request_policy(env):
    result = _run(env, lambda r: [], lambda r: _summary(total=1, partial=1),
                  LawInboxRequest(root="/tmp/law", halt_on_partial=False))
    assert result["verdict"] == "ok"
