import asyncio

from myproject.core.dispatcher import Dispatcher
from myproject.core.handler import Handler
from myproject.schema.event import AData, BData, CData, Event, EventType


class LogHandler(Handler):
    """简单消费者：等待指定时间后打印事件处理结果。"""

    def __init__(self, name: str, delay: float = 1.0) -> None:
        self.name = name
        self.delay = delay

    async def __call__(self, e: Event) -> None:
        await asyncio.sleep(self.delay)
        print(
            f"[{self.name}] 处理完成: event_type={e.event_type.name}, "
            f"correlation_id={e.correlation_id}, data={e.data.data}"
        )


def build_event(event_type: EventType, correlation_id: str, data: str) -> Event:
    """根据事件类型构造对应 payload 的事件。"""
    payload_type = {
        EventType.A: AData,
        EventType.B: BData,
        EventType.C: CData,
    }[event_type]
    return Event(
        event_type=event_type,
        correlation_id=correlation_id,
        source="demo-producer",
        data=payload_type(data=data),
    )


async def produce_events(dispatcher: Dispatcher, correlation_id: str) -> None:
    """按 A -> B -> C 的顺序生产事件，演示依赖等待和链式唤醒。"""
    for event_type, delay in (
        (EventType.A, 0),
        (EventType.B, 2),
        (EventType.C, 2),
    ):
        await asyncio.sleep(delay)
        event = build_event(event_type, correlation_id, f"{event_type.name} 的任务数据")
        print(f"[producer] 发布事件 {event.event_type.name}")
        await dispatcher.publish(event)


async def run_demo() -> None:
    """组装分发器、注册消费者并运行一次完整业务流程。"""
    dispatcher = Dispatcher(timeout=10, check_interval=1)
    dispatcher.register(EventType.A, LogHandler("consumer-A"))
    dispatcher.register(EventType.B, LogHandler("consumer-B"))
    dispatcher.register(EventType.C, LogHandler("consumer-C"))

    # C -> B -> A：C 完成后唤醒 B，B 完成后唤醒 A
    dispatcher.add_dependency(EventType.B, EventType.C)
    dispatcher.add_dependency(EventType.A, EventType.B)

    runner = asyncio.create_task(dispatcher.run())
    await produce_events(dispatcher, correlation_id="demo-flow-1")
    await dispatcher.close()
    await runner


def main() -> None:
    asyncio.run(run_demo())
