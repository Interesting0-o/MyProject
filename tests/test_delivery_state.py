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
    assert not dispatcher.handler_state.done
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


async def test_completed_ttl_expires_and_allows_reprocessing():
    """固定 TTL 到期后，同一 eid 会被重新处理。"""
    dispatcher = Dispatcher(
        max_retry=0,
        timeout=5,
        check_interval=0.01,
        completed_ttl=0.05,
    )
    handler = CountingHandler("ttl-handler")
    dispatcher.register(EventType.A, handler)
    event = make_event()

    runner = asyncio.create_task(dispatcher.run())
    await dispatcher.publish(event)
    await _wait_until(lambda: event.eid in dispatcher.event_state.completed)
    assert handler.calls == 1

    await _wait_until(lambda: event.eid not in dispatcher.event_state.completed)

    await dispatcher.publish(event)
    await _wait_until(
        lambda: event.eid in dispatcher.event_state.completed
        and handler.calls == 2
    )

    await dispatcher.close()
    await runner


async def test_sealed_flow_ttl_rejects_events_and_deletes_flow():
    """seal_flow 后新事件进入 DLQ，空闲流程在 TTL 后删除。"""
    dispatcher = Dispatcher(
        max_retry=0,
        timeout=5,
        check_interval=0.01,
        sealed_flow_ttl=0.05,
    )
    handler = CountingHandler("sealed-handler")
    dispatcher.register(EventType.A, handler)
    event = make_event()

    runner = asyncio.create_task(dispatcher.run())
    await dispatcher.publish(event)
    await _wait_until(lambda: event.eid in dispatcher.event_state.completed)
    assert handler.calls == 1

    dispatcher.seal_flow("delivery-state")
    flow = dispatcher.flows["delivery-state"]
    assert flow.sealed_at is not None

    late = make_event()
    await dispatcher.publish(late)
    await _wait_until(lambda: len(dispatcher.dlq) == 1)

    assert dispatcher.dlq[0].e.eid == late.eid
    assert dispatcher.dlq[0].reason == "业务流程已关闭"
    assert handler.calls == 1

    await _wait_until(lambda: "delivery-state" not in dispatcher.flows)

    await dispatcher.close()
    await runner
