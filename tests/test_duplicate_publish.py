import asyncio

from myproject.core.dispatcher import Dispatcher
from myproject.core.handler import Handler
from myproject.schema.event import AData, Event, EventType


class RecordingHandler(Handler):
    """正常消费者，只记录被调用的次数。"""

    handler_id = "recording-handler"

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, e: Event) -> None:
        self.calls += 1


class FailOnceHandler(Handler):
    """第一次调用失败，之后成功，用于验证失败后的重放。"""

    handler_id = "fail-once-handler"

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, e: Event) -> None:
        self.calls += 1
        await asyncio.sleep(0.01)
        if self.calls == 1:
            raise RuntimeError("boom")


def make_event(correlation_id: str, data: str) -> Event:
    return Event(
        event_type=EventType.A,
        correlation_id=correlation_id,
        source="pytest",
        data=AData(data=data),
    )


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待条件超时")


async def test_a_concurrent_duplicate_processed_once(capsys):
    """测试 A：同一个 eid 并发 publish 两次，handler 正常时只处理一次。"""
    dispatcher = Dispatcher(timeout=5, check_interval=60)
    handler = RecordingHandler()
    dispatcher.register(EventType.A, handler)
    event = make_event("duplicate-success", "并发重复事件")

    # 两次 publish 都会入队，去重发生在 _dispatch 阶段
    await asyncio.gather(dispatcher.publish(event), dispatcher.publish(event))
    assert dispatcher.queue.qsize() == 2

    runner = asyncio.create_task(dispatcher.run())
    await dispatcher.close()
    await runner

    output = capsys.readouterr().out
    flow = dispatcher.flows["duplicate-success"]

    assert handler.calls == 1
    assert flow.completed == {EventType.A}
    assert not flow.failed
    assert not dispatcher.dlq
    assert event.eid in dispatcher.event_state.completed
    assert not dispatcher.handler_state.done
    assert not dispatcher.handler_state.running

    print(output)
    print(
        f"[test A] publish=2, 入队=2, handler 调用={handler.calls}, "
        f"completed=1, dlq=0"
    )


async def test_b_replay_after_failure_succeeds(capsys):
    """测试 B：事件失败进 DLQ 后，同一个 eid 可以重新入队并被重放。"""
    dispatcher = Dispatcher(max_retry=0, timeout=5, check_interval=60)
    handler = FailOnceHandler()
    dispatcher.register(EventType.A, handler)
    event = make_event("duplicate-replay", "失败后重放事件")

    runner = asyncio.create_task(dispatcher.run())

    # 第一次处理失败并进入 DLQ
    await dispatcher.publish(event)
    await _wait_until(lambda: bool(dispatcher.dlq))
    flow = dispatcher.flows["duplicate-replay"]
    assert handler.calls == 1
    assert len(dispatcher.dlq) == 1
    assert flow.failed == {EventType.A}
    assert not flow.completed
    assert event.eid not in dispatcher.event_state.dispatching
    assert event.eid not in dispatcher.event_state.completed
    assert not dispatcher.handler_state.done
    assert not dispatcher.handler_state.running

    # 失败后再次投递同一个 eid，允许重放并只重试失败的 handler
    await dispatcher.publish(event)
    assert dispatcher.queue.qsize() == 1
    await _wait_until(lambda: EventType.A in flow.completed)

    await dispatcher.close()
    await runner

    output = capsys.readouterr().out

    assert handler.calls == 2
    assert len(dispatcher.dlq) == 1
    assert flow.completed == {EventType.A}
    assert event.eid in dispatcher.event_state.completed
    assert not dispatcher.handler_state.done

    print(output)
    print(
        f"[test B] 首次失败 dlq=1, 重放后 handler 调用={handler.calls}, "
        f"completed=1, dlq=1"
    )
