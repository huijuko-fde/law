"""오래 걸리는 동기 activity 의 생존신호(heartbeat).

**왜 필요한가.** 색인 activity 는 start_to_close 가 길다(초기 full 24시간, package 소비 6시간).
`heartbeat_timeout` 이 없으면 워커가 OOM·재시작으로 죽었을 때 Temporal 은 그 긴 타임아웃이
다 지나야 activity 를 실패로 보고 재시도한다 — 무인 운영에서 크래시 한 번이 하루를 통째로
날린다. heartbeat 를 주기적으로 보내고 워크플로가 `heartbeat_timeout` 을 걸면 마지막 신호
이후 그 시간만 지나면 즉시 재시도된다(수분).

동기 activity 는 ThreadPoolExecutor 에서 돌고 본체가 임베딩·HTTP 로 블로킹되므로, 별도
스레드가 신호를 보낸다. activity 컨텍스트는 contextvars 로 넘긴다.
"""
import contextlib
import contextvars
import threading

from temporalio import activity

# 생존신호 주기(초). 워크플로의 heartbeat_timeout 은 이 값의 넉넉한 배수로 잡는다(오발동 방지).
INTERVAL = 30.0


@contextlib.contextmanager
def heartbeating(interval: float = INTERVAL):
    """블록이 도는 동안 주기적으로 activity.heartbeat() 를 보낸다."""
    ctx = contextvars.copy_context()
    stop = threading.Event()

    def _run():
        while not stop.wait(interval):
            try:
                ctx.run(activity.heartbeat)
            except Exception:                     # 컨텍스트 없음(테스트)·전송 실패는 무시
                pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()
