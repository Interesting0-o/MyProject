import asyncio

from myproject.core.dispatcher import Dispatcher
from myproject.core.handler import Handler
from myproject.schema.event import AData, BData, CData, Event, EventType

PAYLOAD_TYPES = {
    EventType.A: AData,
    EventType.B: BData,
    EventType.C: CData,
}


class OrderHandler(Handler):
    """记录事件被处理的顺序。"""

    def __init__(self, handler_id: str, order: list[EventType]) -> None:
        self._handler_id = handler_id
        self.order = order

    @property
    def handler_id(self) -> str:
        return self._handler_id

    async def __call__(self, e: Event) -> None:
        self.order.append(e.event_type)


def make_event(event_type: EventType, correlation_id: str) -> Event:
    payload_type = PAYLOAD_TYPES[event_type]
    return Event(
        event_type=event_type,
        correlation_id=correlation_id,
        source="pytest",
        data=payload_type(data=f"{event_type.name} 数据"),
    )


async def test_dependency_order_and_flow_state():
    """A 依赖 B、B 依赖 C 时，事件应按 C -> B -> A 的顺序执行。"""
    dispatcher = Dispatcher(timeout=5, check_interval=60)
    order: list[EventType] = []
    for event_type in EventType:
        dispatcher.register(
            event_type, OrderHandler(f"order-{event_type.name}", order)
        )

    dispatcher.add_dependency(EventType.B, EventType.C)
    dispatcher.add_dependency(EventType.A, EventType.B)

    flow_id = "flow-order"
    runner = asyncio.create_task(dispatcher.run())
    for event_type in (EventType.A, EventType.B, EventType.C):
        await dispatcher.publish(make_event(event_type, flow_id))
    await dispatcher.close()
    await runner

    flow = dispatcher.flows[flow_id]
    assert order == [EventType.C, EventType.B, EventType.A]
    assert flow.completed == set(EventType)
    assert not flow.failed
    assert not flow.waiting
