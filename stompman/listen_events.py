from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from stompman.frames import (
    AckFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    NackFrame,
    UnknownFrame,
)

if TYPE_CHECKING:
    from stompman.client import Client


@dataclass
class MessageEvent:
    body: bytes = field(init=False)
    _frame: MessageFrame
    _client: "Client" = field(repr=False)

    def __post_init__(self) -> None:
        self.body = self._frame.body

    async def ack(self) -> None:
        await self._client._connection.write_frame(
            AckFrame(
                headers={
                    "subscription": self._frame.headers["subscription"],
                    "message-id": self._frame.headers["message-id"],
                },
            )
        )

    async def nack(self) -> None:
        await self._client._connection.write_frame(
            NackFrame(
                headers={
                    "subscription": self._frame.headers["subscription"],
                    "message-id": self._frame.headers["message-id"],
                }
            )
        )


@dataclass
class ErrorEvent:
    message_header: str = field(init=False)
    body: bytes = field(init=False)
    _frame: ErrorFrame
    _client: "Client" = field(repr=False)

    def __post_init__(self) -> None:
        self.message_header = self._frame.headers["message"]
        self.body = self._frame.body


@dataclass
class HeartbeatEvent:
    _frame: HeartbeatFrame
    _client: "Client" = field(repr=False)


@dataclass
class UnknownEvent:
    _frame: UnknownFrame
    _client: "Client" = field(repr=False)


AnyListeningEvent = MessageEvent | ErrorEvent | HeartbeatEvent | UnknownEvent
