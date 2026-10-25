from abc import ABC, abstractmethod

from myproject.schema import Event


class Handler(ABC):
    """事件消费者基类，子类需要实现异步处理逻辑。"""

    # 要求子类必须实现一个异步方法
    @abstractmethod
    async def __call__(self, e: Event) -> None:
        """处理一个事件，处理失败时应抛出异常以便触发重试。"""
        ...
