from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from stompman.connection import AbstractConnection
from stompman.connection_manager import ConnectionManager
from stompman.frames import (
    AckFrame,
    AckMode,
    MessageFrame,
    NackFrame,
    SubscribeFrame,
    UnsubscribeFrame,
)

ActiveSubscriptions = dict[str, "AutoAckSubscription | ManualAckSubscription"]


@dataclass(kw_only=True, slots=True)
class BaseSubscription:
    id: str = field(default_factory=lambda: _make_subscription_id(), init=False)  # noqa: PLW0108
    destination: str
    headers: dict[str, str] | None
    ack: AckMode
    _connection_manager: ConnectionManager
    _active_subscriptions: ActiveSubscriptions

    async def _subscribe(self) -> None:
        await self._connection_manager.write_frame_reconnecting(
            SubscribeFrame.build(
                subscription_id=self.id, destination=self.destination, ack=self.ack, headers=self.headers
            )
        )
        self._active_subscriptions[self.id] = self  # type: ignore[assignment]

    async def unsubscribe(self) -> None:
        del self._active_subscriptions[self.id]
        await self._connection_manager.maybe_write_frame(UnsubscribeFrame(headers={"id": self.id}))

    async def _nack(self, frame: MessageFrame) -> None:
        if self.id in self._active_subscriptions and (ack_id := frame.headers.get("ack")):
            await self._connection_manager.maybe_write_frame(
                NackFrame(headers={"id": ack_id, "subscription": frame.headers["subscription"]})
            )

    async def _ack(self, frame: MessageFrame) -> None:
        if self.id in self._active_subscriptions and (ack_id := frame.headers["ack"]):
            await self._connection_manager.maybe_write_frame(
                AckFrame(headers={"id": ack_id, "subscription": frame.headers["subscription"]})
            )


@dataclass(kw_only=True, slots=True)
class AutoAckSubscription(BaseSubscription):
    handler: Callable[[MessageFrame], Awaitable[Any]]
    on_suppressed_exception: Callable[[Exception, MessageFrame], Any]
    suppressed_exception_classes: tuple[type[Exception], ...]
    _should_handle_ack_nack: bool = field(init=False)

    def __post_init__(self) -> None:
        self._should_handle_ack_nack = self.ack in {"client", "client-individual"}

    async def _subscribe(self) -> None:
        await self._connection_manager.write_frame_reconnecting(
            SubscribeFrame.build(
                subscription_id=self.id, destination=self.destination, ack=self.ack, headers=self.headers
            )
        )
        self._active_subscriptions[self.id] = self

    async def unsubscribe(self) -> None:
        del self._active_subscriptions[self.id]
        await self._connection_manager.maybe_write_frame(UnsubscribeFrame(headers={"id": self.id}))

    async def _run_handler(self, *, frame: MessageFrame) -> None:
        try:
            await self.handler(frame)
        except self.suppressed_exception_classes as exception:
            if self._should_handle_ack_nack:
                await self._nack(frame)
            self.on_suppressed_exception(exception, frame)
        else:
            if self._should_handle_ack_nack:
                await self._ack(frame)


@dataclass(kw_only=True, slots=True)
class ManualAckSubscription(BaseSubscription):
    handler: Callable[["AckableMessageFrame"], Coroutine[Any, Any, Any]]


@dataclass(frozen=True, kw_only=True, slots=True)
class AckableMessageFrame(MessageFrame):
    _subscription: ManualAckSubscription

    async def ack(self) -> None:
        await self._subscription._ack(self)  # noqa: SLF001

    async def nack(self) -> None:
        await self._subscription._nack(self)  # noqa: SLF001


def _make_subscription_id() -> str:
    return str(uuid4())


async def resubscribe_to_active_subscriptions(
    *, connection: AbstractConnection, active_subscriptions: ActiveSubscriptions
) -> None:
    for subscription in active_subscriptions.values():
        await connection.write_frame(
            SubscribeFrame.build(
                subscription_id=subscription.id,
                destination=subscription.destination,
                ack=subscription.ack,
                headers=subscription.headers,
            )
        )


async def unsubscribe_from_all_active_subscriptions(*, active_subscriptions: ActiveSubscriptions) -> None:
    for subscription in active_subscriptions.copy().values():
        await subscription.unsubscribe()
