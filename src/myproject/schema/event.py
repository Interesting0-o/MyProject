from datetime import UTC, datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field
from uuid_utils import uuid7


class EventType(Enum):
    A = 1
    B = 2
    C = 3


class AData(BaseModel):
    type: Literal[EventType.A] = EventType.A
    data: str


class CData(BaseModel):
    type: Literal[EventType.C] = EventType.C
    data: str


class BData(BaseModel):
    type: Literal[EventType.B] = EventType.B
    data: str


class Event(BaseModel):
    event_type: EventType = Field(..., description="事件的类型")
    eid: str = Field(default_factory=lambda: uuid7().hex, description="事件的唯一id")
    correlation_id: str = Field(..., description="同一业务流程的关联id")
    source: str
    timestamp: AwareDatetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="事件发生的时间 (UTC)",
    )
    data: Annotated[AData | BData | CData, Field(discriminator="type")]


class DeadEvent(BaseModel):
    e: Event = Field(..., description="失败的事件")
    reason: str = Field(..., description="事件失败的原因")
