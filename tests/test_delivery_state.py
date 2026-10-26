import asyncio

from myproject.core.dispatcher import Dispatcher
from myproject.core.handler import Handler
from myproject.schema.event import AData, Event, EventType


class CountingHandler(Handler):
    """可配置前 N 次失败的消费者。"""

    def __init__(self, handler_id: str, fail_times: int = 0) -> None:
        self._handler_id = handler_id
        self.fail_times = fail_times
        self.calls = 0

    @property
    def handler_id(self) -> str:
        return self._handler_id

    async def __call__(self, e: Event) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("boom")


def make_event() -> Event:
    return Event(
        event_type=EventType.A,
        correlation_id="delivery-state",
        source="pytest",
        data=AData(data="幂等测试"),
    )


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待条件超时")


async def test_partial_success_only_retries_failed_handler():
    """一个 handler 成功、另一个失败时，重放只应重试失败的那个。"""
    dispatcher = Dispatcher(max_retry=0, timeout=5, check_interval=60)
    succeeded = CountingHandler("succeeded")
    retried = CountingHandler("retried", fail_times=1)
    dispatcher.register(EventType.A, succeeded)
    dispatcher.register(EventType.A, retried)
    event = make_event()

    runner = asyncio.create_task(dispatcher.run())
    await dispatcher.publish(event)
    await _wait_until(lambda: bool(dispatcher.dlq))

    assert event.eid not in dispatcher.event_state.dispatching
    assert event.eid not in dispatcher.event_state.completed
    assert dispatcher.handler_state.done == {(event.eid, "succeeded")}
    assert dispatcher.handler_state.running == set()
    assert succeeded.calls == 1
    assert retried.calls == 1

    # 重放：成功的 handler 跳过，只重试失败的 handler
    await dispatcher.publish(event)
    flow = dispatcher.flows["delivery-state"]
    await _wait_until(lambda: EventType.A in flow.completed)

    assert event.eid in dispatcher.event_state.completed
    assert dispatcher.handler_state.done == {
        (event.eid, "succeeded"),
        (event.eid, "retried"),
    }
    assert dispatcher.handler_state.running == set()
    assert succeeded.calls == 1
    assert retried.calls == 2

    # 已完成的事件再次投递：直接跳过，不重新分发
    await dispatcher.publish(event)
    await asyncio.sleep(0.05)
    assert event.eid in dispatcher.event_state.completed
    assert succeeded.calls == 1
    assert retried.calls == 2
    assert flow.completed == {EventType.A}

    await dispatcher.close()
    await runner
