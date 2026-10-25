import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum

from myproject.schema.event import DeadEvent, Event, EventType

from .handler import Handler


@dataclass
class FlowState:
    """单个业务流程（correlation_id）内的顺序性状态。"""

    waiting: dict[EventType, list[Event]] = field(default_factory=dict)
    completed: set[EventType] = field(default_factory=set)
    failed: set[EventType] = field(default_factory=set)


class DeliveryStatus(Enum):
    """事件投递状态。"""

    DISPATCHING = "dispatching"
    COMPLETED = "completed"
    FAILED = "failed"


class HandlerStatus(Enum):
    """单个消费者对某个事件的处理状态。"""

    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass
class DeliveryState:
    """单个事件（eid）的幂等状态。"""

    status: DeliveryStatus = DeliveryStatus.DISPATCHING
    handlers: dict[str, HandlerStatus] = field(default_factory=dict)
    #  键指代handler_id 值为其状态


class Dispatcher:
    """事件分发器，维护事件队列、依赖关系以及事件的生命周期状态。"""

    def __init__(
        self,
        maxsize: int = 100,
        max_retry: int = 3,
        timeout: float = 30,
        check_interval: float = 5.0,
    ):
        """
        初始化分发器。

        Args:
            maxsize: 事件接收队列的最大长度。
            max_retry: 单个事件处理失败后的最大重试次数，总执行次数为 max_retry + 1。
            timeout: 事件等待依赖的超时时间，单位为秒。
            check_interval: 惰性超时检查的间隔时间，单位为秒。
        """

        self.queue = asyncio.Queue[Event | None](maxsize=maxsize)  # 事件接受队列

        self.dlq: list[DeadEvent] = []  # 死信队列
        self.flows: dict[str, FlowState] = {}  # correlation_id -> 业务流程状态

        self.max_retry = max_retry  # 最大重试机会
        self.timeout = timeout  # 事件超时时间,单位为秒
        self.check_interval = check_interval  # 超时检查间隔,单位为秒

        # 依赖关系表，元素为 (依赖方, 被依赖方)
        self.dependence_rule: list[tuple[EventType, EventType]] = []
        # 依赖关系为**前者依赖后者的完成**
        self.registry: dict[EventType, list[Handler]] = {}  # 消费者注册字典

        self.deliveries: dict[str, DeliveryState] = {}  # eid -> 投递幂等状态

        self.tasks: set[asyncio.Task] = set()  # 正在运行的分发任务，关闭时需要等待完成

    # =======================================
    #              事件的存入
    # =======================================

    async def publish(self, e: Event) -> None:
        """将事件放入接收队列，队列满时会阻塞等待。"""
        await self.queue.put(e)

    async def close(self) -> None:
        """发送毒丸（None），通知 run 处理完现有事件后退出。"""
        await self.queue.put(None)

    # ========================================
    #              消费者注册
    # ========================================

    def register(self, e_type: EventType, handler: Handler) -> None:
        """为指定事件类型注册消费者，同一类型可以注册多个消费者。"""
        self.registry.setdefault(e_type, []).append(handler)

    # ========================================
    #              依赖增加
    # ========================================

    def add_dependency(
        self,
        event_type: EventType,
        dependency_type: EventType,
    ) -> None:
        """
        添加依赖关系。

        event_type 依赖 dependency_type，即 dependency_type 完成后才能处理 event_type。
        这里只建立类型间的静态依赖，运行时还会结合 correlation_id 判断事件是否
        属于同一业务流程。若新依赖会导致循环依赖，则抛出 ValueError。
        """
        if event_type == dependency_type:
            raise ValueError(f"事件 {event_type.name} 不能依赖自身")

        if (event_type, dependency_type) in self.dependence_rule:
            return

        # 构建 "事件类型 -> 依赖类型列表" 的邻接表
        dependencies: dict[EventType, list[EventType]] = {}
        for e_type, dep_type in self.dependence_rule:
            dependencies.setdefault(e_type, []).append(dep_type)

        # 从新依赖出发沿依赖链搜索，若能回到 event_type 则说明会形成环
        stack = [dependency_type]
        visited: set[EventType] = set()
        while stack:
            current = stack.pop()
            if current == event_type:
                raise ValueError(
                    f"添加依赖 {event_type.name} -> {dependency_type.name} 会形成循环依赖"
                )
            if current in visited:
                continue
            visited.add(current)
            stack.extend(dependencies.get(current, []))

        self.dependence_rule.append((event_type, dependency_type))

    # =========================================
    #              成功和失败
    # =========================================

    def _get_flow(self, correlation_id: str) -> FlowState:
        """获取或创建指定业务流程的顺序状态。"""
        return self.flows.setdefault(correlation_id, FlowState())

    def _get_dependent(self, e_type: EventType) -> list[EventType]:
        """
        获取直接依赖指定事件类型的事件类型列表。

        返回的事件类型在 e_type 完成后可能被唤醒。
        """
        return [
            dependent
            for (dependent, dependence) in self.dependence_rule
            if dependence == e_type
        ]

    def _get_dependency(self, e_type: EventType) -> list[EventType]:
        """获取指定事件类型直接依赖的所有事件类型。"""
        return [
            dependency
            for (dependent, dependency) in self.dependence_rule
            if dependent == e_type
        ]

    async def _on_success(self, e: Event):
        """
        记录事件处理成功，并唤醒同一业务流程内依赖它的挂起事件。

        只有自身所有依赖都已完成的挂起事件才会被唤醒；已超时的挂起事件
        不再唤醒，直接按失败处理。被唤醒的事件会先从 waiting 中移除再重新
        分发，避免被重复唤醒。
        """
        flow = self._get_flow(e.correlation_id)
        flow.completed.add(e.event_type)
        print("[info]事件处理成功")

        expired: list[Event] = []
        ready: list[Event] = []

        # 依赖唤醒：只检查同一业务流程内直接依赖当前事件类型的挂起事件
        for dependent_type in self._get_dependent(e.event_type):
            pending = flow.waiting.get(dependent_type)
            if not pending:
                continue

            remaining: list[Event] = []
            for waiting_event in pending:
                if self.is_timeout(waiting_event.timestamp):
                    expired.append(waiting_event)
                elif self._is_dependency_complete(waiting_event, flow):
                    ready.append(waiting_event)
                else:
                    remaining.append(waiting_event)

            # 先更新 waiting 再分发，防止唤醒过程中重复处理
            if remaining:
                flow.waiting[dependent_type] = remaining
            else:
                flow.waiting.pop(dependent_type, None)

        # 先处理超时事件，避免依赖刚完成就执行已经过期的事件
        for waiting_event in expired:
            await self._on_failure(waiting_event, "依赖等待超时")

        for waiting_event in ready:
            await self._dispatch(waiting_event)

    async def _on_failure(self, e: Event, reason: str):
        """
        记录事件处理失败，并写入死信队列。

        同时级联失效同一业务流程中直接依赖该事件类型的挂起事件。
        """
        flow = self._get_flow(e.correlation_id)
        flow.failed.add(e.event_type)
        self.dlq.append(DeadEvent(e=e, reason=reason))
        print("[info]事件处理失败")

        # 级联失效：同一业务流程内依赖该事件类型的挂起事件也判定失败
        for dependent_type in self._get_dependent(e.event_type):
            affected = flow.waiting.pop(dependent_type, [])
            for waiting_event in affected:
                await self._on_failure(waiting_event, "必要的依赖执行失败")

    def _is_dependency_complete(self, e: Event, flow: FlowState) -> bool:
        """
        判断事件在所属业务流程内的所有依赖是否都已完成。

        依赖完成的标准：对应依赖类型已记录在 flow.completed 中。
        没有依赖的事件直接视为完成。
        """
        return all(dep in flow.completed for dep in self._get_dependency(e.event_type))

    def _is_dependency_failed(self, e: Event, flow: FlowState) -> bool:
        """
        判断事件在所属业务流程内的任意直接依赖是否已失败。

        任一依赖类型记录在 flow.failed 中即返回 True；没有依赖的事件返回 False。
        """
        return any(dep in flow.failed for dep in self._get_dependency(e.event_type))

    # ==========================================
    #                事件分发
    # ==========================================

    async def _with_retry(self, e: Event, handler: Handler) -> str | None:
        """
        执行单个消费者，失败时按指数退避重试。

        Returns:
            成功时返回 None；全部尝试都失败时返回最后一次的异常信息。
        """
        delivery = self.deliveries[e.eid]
        state = delivery.handlers.get(handler.handler_id)
        if state in (HandlerStatus.DONE, HandlerStatus.RUNNING):
            # 已完成或正在执行的消费者直接跳过；同一 gather 内会等待真正执行的那个
            return None

        delivery.handlers[handler.handler_id] = HandlerStatus.RUNNING
        for i in range(self.max_retry + 1):
            try:
                await handler(e)
                delivery.handlers[handler.handler_id] = HandlerStatus.DONE
                return None
            except Exception as exc:
                if i == self.max_retry:
                    delivery.handlers[handler.handler_id] = HandlerStatus.FAILED
                    return str(exc)
                print(f"[info]事件执行失败，{2**i}秒后重试")
                await asyncio.sleep(2**i)

    async def _dispatch(self, e: Event):
        """
        将事件分发给该类型注册的所有消费者。

        多个消费者通过 asyncio.gather 并发执行；只要有一个消费者最终失败，
        就按事件失败处理，否则记录为成功。未注册消费者时视为成功。
        同一个 eid 正在分发或已完成时不会重复分发；失败后再次投递则允许重放，
        已成功的消费者会跳过，只重试失败的消费者。
        """
        delivery = self.deliveries.get(e.eid)
        if delivery is None:
            delivery = DeliveryState()
            self.deliveries[e.eid] = delivery
        elif (
            delivery.status is DeliveryStatus.DISPATCHING
            or delivery.status is DeliveryStatus.COMPLETED
        ):
            return
        else:
            # 上一次投递失败，允许重放；失败的 handler 会重新执行
            delivery.status = DeliveryStatus.DISPATCHING

        results = await asyncio.gather(
            *(
                self._with_retry(e, handler)
                for handler in self.registry.get(e.event_type, [])
            )
        )
        failure_reason = next(
            (result for result in results if result is not None), None
        )
        if failure_reason is None:
            delivery.status = DeliveryStatus.COMPLETED
            await self._on_success(e)
        else:
            delivery.status = DeliveryStatus.FAILED
            await self._on_failure(e, failure_reason)

    # ===========================================
    #                 惰性检查
    # ===========================================
    def is_timeout(self, timestamp: datetime) -> bool:
        """
        判断事件时间戳距当前时间是否已达到超时阈值。

        timestamp 需要是带时区的 datetime，Event 默认使用 UTC 时间。
        """
        # 获取当前带时区的 UTC 时间
        now = datetime.now(UTC)

        # 计算从事件发生到现在，经过了多长时间
        elapsed = now - timestamp

        # 如果经过的时间 >= 超时时间，说明已超时
        return elapsed >= timedelta(seconds=self.timeout)

    async def check(self):
        """
        周期性检查挂起事件是否等待依赖超时。

        超时事件会先从 waiting 中移除，再按失败处理，防止后续依赖完成时
        被重复唤醒或重复写入死信队列。
        """
        while True:
            await asyncio.sleep(self.check_interval)

            expired: list[Event] = []
            for flow in list(self.flows.values()):
                for e_type, events in list(flow.waiting.items()):
                    remaining: list[Event] = []
                    for waiting_event in events:
                        if self.is_timeout(waiting_event.timestamp):
                            expired.append(waiting_event)
                        else:
                            remaining.append(waiting_event)

                    if remaining:
                        flow.waiting[e_type] = remaining
                    else:
                        flow.waiting.pop(e_type, None)

            # 统一处理超时事件，避免在遍历 waiting 时修改其结构
            for waiting_event in expired:
                await self._on_failure(waiting_event, "依赖等待超时")

    # ==========================================
    #               事件主循环
    # ==========================================

    async def run(self):
        """
        启动事件主循环。

        事件从队列中串行取出并按依赖状态处理：依赖已完成则并发分发，
        依赖已失败则直接判失败，否则放入 waiting 等待唤醒。同时启动 check
        任务做超时检查；收到毒丸（None）后取消检查并等待所有分发任务结束。
        """
        check = asyncio.create_task(self.check())
        while True:
            e = await self.queue.get()

            if not e:
                # 事件主循环遇到毒丸直接跳出
                self.queue.task_done()
                check.cancel()
                if self.tasks:
                    await asyncio.gather(*self.tasks)
                break

            flow = self._get_flow(e.correlation_id)
            if self._is_dependency_complete(e, flow):
                # 所有依赖完成之后再执行任务分发，分发任务并发运行
                task = asyncio.create_task(self._dispatch(e))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
            elif self._is_dependency_failed(e, flow):
                # 检测到依赖失败则直接执行失败处理
                await self._on_failure(e, "必要的依赖执行失败")
            else:
                # 依赖尚未完成，挂起等待后续事件唤醒
                flow.waiting.setdefault(e.event_type, []).append(e)

            self.queue.task_done()
