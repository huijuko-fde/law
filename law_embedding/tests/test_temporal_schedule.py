"""Schedule 정의·갱신 — 실제 Temporal dev 서버(temporalio 가 CLI 를 내려받아 띄운다; 못 띄우면 건너뛴다).

time-skipping 테스트 서버는 Schedule 을 지원하지 않아 dev 서버를 쓴다."""
import asyncio
import uuid

import pytest

temporalio = pytest.importorskip("temporalio")
from temporalio.testing import WorkflowEnvironment  # noqa: E402

from law_indexer import temporal_schedule as TS  # noqa: E402
from law_indexer.worker_workflows import LawInboxRequest  # noqa: E402


@pytest.fixture(scope="module")
def env():
    loop = asyncio.new_event_loop()
    try:
        e = loop.run_until_complete(WorkflowEnvironment.start_local())
    except Exception as exc:                        # CLI 다운로드 불가(폐쇄망 등)
        pytest.skip(f"temporal dev 서버를 띄우지 못함: {exc}")
    yield loop, e
    loop.run_until_complete(e.shutdown())
    loop.close()


def test_ensure_schedule_updates_existing_but_keeps_pause(env):
    """재배포로 LAW_ROOT·큐·cron 이 바뀌면 기존 Schedule 을 갱신한다 — 사람이 건 일시정지·메모는 그대로
    (2026-11-05 전체 검증 TL-1: 갱신하지 않으면 첫 등록 값으로 영원히 돈다)."""
    loop, e = env
    sid = f"law-inbox-test-{uuid.uuid4().hex[:8]}"

    async def go():
        await TS.ensure_schedule(e.client, task_queue="q1", req=LawInboxRequest(root="/tmp/a"),
                                 cron="0 * * * *", schedule_id=sid)
        await TS.pause(e.client, "사람이 멈춤", schedule_id=sid)
        await TS.ensure_schedule(e.client, task_queue="q2", req=LawInboxRequest(root="/tmp/b"),
                                 cron="*/5 * * * *", schedule_id=sid)
        d = await e.client.get_schedule_handle(sid).describe()
        info = await TS.describe(e.client, schedule_id=sid)
        await e.client.get_schedule_handle(sid).delete()
        return d, info

    d, info = loop.run_until_complete(go())
    assert d.schedule.action.task_queue == "q2"
    assert d.schedule.state.paused and d.schedule.state.note == "사람이 멈춤"
    assert d.schedule.action.run_timeout.total_seconds() == TS.RUN_TIMEOUT_HOURS * 3600
    assert info["paused"] and info["cron"] and info["cron"][0].startswith("minute=")   # 서버는 cron 을 calendar 로 돌려준다
