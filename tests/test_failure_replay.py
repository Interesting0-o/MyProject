import asyncio

from myproject.core.dispatcher import Dispatcher
from myproject.core.handler import Handler
from myproject.schema.event import AData, BData, Event, EventType


class PaymentHandler(Handler):
    """第一次支付处理失败，之后成功。"""

    handler_id = "payment-handler"

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, e: Event) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("payment failed")


class ShippingHandler(Handler):
    """记录发货处理次数。"""

    handler_id = "shipping-handler"

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, e: Event) -> None:
        self.calls += 1


def make_payment() -> Event:
    return Event(
        event_type=EventType.A,
        correlation_id="order-123",
        source="pytest",
        data=AData(data="支付"),
    )


def make_shipping() -> Event:
    return Event(
        event_type=EventType.B,
        correlation_id="order-123",
        source="pytest",
        data=BData(data="发货"),
    )


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待条件超时")


async def test_replay_failed_dependency_does_not_revive_dependent():
    """A 失败导致 B 级联失败后，只重投 A 不会自动恢复 B。"""
    dispatcher = Dispatcher(max_retry=0, timeout=5, check_interval=60)
    payment = PaymentHandler()
    shipping = ShippingHandler()
    dispatcher.register(EventType.A, payment)
    dispatcher.register(EventType.B, shipping)
    dispatcher.add_dependency(EventType.B, EventType.A)

    payment_event = make_payment()
    shipping_event = make_shipping()
    payment_key = (payment_event.eid, "payment-handler")
    shipping_key = (shipping_event.eid, "shipping-handler")

    runner = asyncio.create_task(dispatcher.run())
    await dispatcher.publish(shipping_event)
    await dispatcher.publish(payment_event)
    await _wait_until(lambda: len(dispatcher.dlq) == 2)

    flow = dispatcher.flows["order-123"]

    # T1：A 失败，B 从 waiting 被级联失败，两个事件都进入 DLQ
    assert payment.calls == 1
    assert shipping.calls == 0
    assert flow.failed == {EventType.A, EventType.B}
    assert not flow.completed
    assert payment_event.eid not in dispatcher.event_state.dispatching
    assert payment_event.eid not in dispatcher.event_state.completed
    assert payment_key not in dispatcher.handler_state.done
    assert payment_key not in dispatcher.handler_state.running
    assert shipping_key not in dispatcher.handler_state.done
    assert shipping_key not in dispatcher.handler_state.running
    assert [dead.e.event_type for dead in dispatcher.dlq] == [
        EventType.A,
        EventType.B,
    ]
    assert dispatcher.dlq[1].reason == "必要的依赖执行失败"

    # T2 + T3：重投同一 eid 的 A，run 会再次分发 A 并重跑支付 handler
    await dispatcher.publish(payment_event)
    await _wait_until(lambda: EventType.A in flow.completed)

    assert payment.calls == 2
    assert shipping.calls == 0
    assert payment_event.eid in dispatcher.event_state.completed
    assert dispatcher.handler_state.done == {payment_key}
    assert flow.completed == {EventType.A}
    assert flow.failed == {EventType.A, EventType.B}
    assert len(dispatcher.dlq) == 2
    assert shipping_key not in dispatcher.handler_state.done
    assert shipping_key not in dispatcher.handler_state.running

    # B 不会因为 A 成功而自动复活，必须重新投递 B
    await dispatcher.publish(shipping_event)
    await _wait_until(lambda: EventType.B in flow.completed)

    assert shipping.calls == 1
    assert shipping_event.eid in dispatcher.event_state.completed
    assert dispatcher.handler_state.done == {payment_key, shipping_key}
    assert flow.completed == {EventType.A, EventType.B}
    assert len(dispatcher.dlq) == 2

    await dispatcher.close()
    await runner


async def test_idempotency_is_at_most_once_success_not_exactly_once():
    """失败后的同 eid 重投会再次执行 handler，系统并不保证 exactly-once。"""
    dispatcher = Dispatcher(max_retry=0, timeout=5, check_interval=60)
    payment = PaymentHandler()
    shipping = ShippingHandler()
    dispatcher.register(EventType.A, payment)
    dispatcher.register(EventType.B, shipping)
    dispatcher.add_dependency(EventType.B, EventType.A)

    payment_event = make_payment()
    shipping_event = make_shipping()
    payment_key = (payment_event.eid, "payment-handler")
    shipping_key = (shipping_event.eid, "shipping-handler")

    runner = asyncio.create_task(dispatcher.run())
    await dispatcher.publish(shipping_event)
    await dispatcher.publish(payment_event)
    await _wait_until(
        lambda: len(dispatcher.dlq) == 2
        and payment_event.eid not in dispatcher.event_state.dispatching
    )

    # T0-T1：A 第一次执行失败，B 被级联失败且没有执行
    assert payment.calls == 1
    assert shipping.calls == 0
    assert not dispatcher.handler_state.done
    assert not dispatcher.event_state.completed
    assert [dead.e.event_type for dead in dispatcher.dlq] == [
        EventType.A,
        EventType.B,
    ]

    # T2-T5：同一 eid 的 A 重投后再次执行 handler 并成功
    await dispatcher.publish(payment_event)
    await _wait_until(
        lambda: payment_event.eid in dispatcher.event_state.completed
    )

    assert payment.calls == 2  # 失败尝试会重复执行，不是 exactly-once
    assert shipping.calls == 0  # B 不会因为 A 后来成功而自动复活
    assert dispatcher.handler_state.done == {payment_key}
    assert payment_key not in dispatcher.handler_state.running
    assert shipping_key not in dispatcher.handler_state.done
    assert dispatcher.event_state.completed == {payment_event.eid}

    flow = dispatcher.flows["order-123"]
    assert flow.completed == {EventType.A}
    assert flow.failed == {EventType.A, EventType.B}  # 失败记录不会被成功覆盖
    assert len(dispatcher.dlq) == 2  # B 的 DeadEvent 仍然保留

    # 成功之后的再次重投会被 completed 集合拦截，handler 不再执行
    await dispatcher.publish(payment_event)
    await asyncio.sleep(0.05)
    assert payment.calls == 2
    assert dispatcher.handler_state.done == {payment_key}

    await dispatcher.close()
    await runner


async def test_replayed_a_success_does_not_execute_cascaded_b():
    """A 级联失败 B 后，即使重投 A 成功，B 也不会被自动执行。"""
    dispatcher = Dispatcher(max_retry=0, timeout=5, check_interval=60)
    payment = PaymentHandler()
    shipping = ShippingHandler()
    dispatcher.register(EventType.A, payment)
    dispatcher.register(EventType.B, shipping)
    dispatcher.add_dependency(EventType.B, EventType.A)

    payment_event = make_payment()
    shipping_event = make_shipping()
    payment_key = (payment_event.eid, "payment-handler")
    shipping_key = (shipping_event.eid, "shipping-handler")

    runner = asyncio.create_task(dispatcher.run())
    await dispatcher.publish(shipping_event)
    await dispatcher.publish(payment_event)
    await _wait_until(
        lambda: len(dispatcher.dlq) == 2
        and payment_event.eid not in dispatcher.event_state.dispatching
    )

    flow = dispatcher.flows["order-123"]

    # T0：A 失败，B 从 waiting 中被 pop 并级联失败
    assert flow.failed == {EventType.A, EventType.B}
    assert flow.waiting == {}
    assert [dead.e.event_type for dead in dispatcher.dlq] == [
        EventType.A,
        EventType.B,
    ]
    assert shipping.calls == 0

    # T1：同 eid 重投 A，A 成功并进入 completed
    await dispatcher.publish(payment_event)
    await _wait_until(
        lambda: payment_event.eid in dispatcher.event_state.completed
    )
    await asyncio.sleep(0.05)

    assert payment.calls == 2
    assert flow.completed == {EventType.A}
    assert flow.failed == {EventType.A, EventType.B}
    assert flow.waiting == {}
    assert dispatcher.event_state.completed == {payment_event.eid}
    assert dispatcher.handler_state.done == {payment_key}

    # T2：B 的等待记录已不存在，_on_success(A) 无法唤醒或重投 B
    assert shipping.calls == 0
    assert shipping_event.eid not in dispatcher.event_state.dispatching
    assert shipping_event.eid not in dispatcher.event_state.completed
    assert shipping_key not in dispatcher.handler_state.running
    assert shipping_key not in dispatcher.handler_state.done
    assert len(dispatcher.dlq) == 2

    await dispatcher.close()
    await runner
