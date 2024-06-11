from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from stompman.frames import (
    AckFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    NackFrame,
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
                headers={"id": self._frame.headers["message-id"], "subscription": self._frame.headers["subscription"]},
            )
        )

    async def nack(self) -> None:
        await self._client._connection.write_frame(
            NackFrame(
                headers={"id": self._frame.headers["message-id"], "subscription": self._frame.headers["subscription"]}
            )
        )

    async def await_with_auto_ack(
        self, awaitable: Awaitable[None], exception_types: tuple[type[Exception],] = (Exception,)
    ) -> None:
        called_nack = False

        try:
            await awaitable
        except exception_types:
            await self.nack()
            called_nack = True
            raise
        finally:
            if not called_nack:
                await self.ack()


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


AnyListeningEvent = MessageEvent | ErrorEvent | HeartbeatEvent
