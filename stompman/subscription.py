import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from functools import cached_property
from uuid import uuid4

from stompman.frames import (
    AckFrame,
    AckMode,
    AnyClientFrame,
    MessageFrame,
    NackFrame,
    SubscribeFrame,
    UnsubscribeFrame,
)

ActiveSubscriptions = dict[str, "Subscription"]


@dataclass(kw_only=True, slots=True)
class SubscriptionConfig:
    destination: str
    handler: Callable[[MessageFrame], Coroutine[None, None, None]]
    ack: AckMode
    on_suppressed_exception: Callable[[Exception, MessageFrame], None]
    supressed_exception_classes: tuple[type[Exception], ...]

    @cached_property
    def should_handle_ack_nack(self) -> bool:
        return self.ack in {"client", "client-individual"}


@dataclass(kw_only=True, slots=True)
class Subscription:
    id: str
    config: SubscriptionConfig
    _unsubscribe: Callable[[str], Awaitable[None]]


def _make_subscription_id() -> str:
    return str(uuid4())


@dataclass(kw_only=True, slots=True)
class ActiveSubscriptionsManager:
    _active_subscriptions: dict[str, SubscriptionConfig] = field(default_factory=dict, init=False)
    write_frame_reconnecting: Callable[[AnyClientFrame], Awaitable[None]]
    maybe_write_frame: Callable[[AnyClientFrame], Awaitable[bool]]
    _generate_subscription_id: Callable[[], str] = field(default=lambda: _make_subscription_id())  # noqa: PLW0108

    async def subscribe(self, subscription_config: SubscriptionConfig) -> Subscription:
        subscription_id = self._generate_subscription_id()
        await self.write_frame_reconnecting(
            SubscribeFrame(
                headers={
                    "id": subscription_id,
                    "destination": subscription_config.destination,
                    "ack": subscription_config.ack,
                }
            )
        )
        self._active_subscriptions[subscription_id] = subscription_config
        return Subscription(id=subscription_id, config=subscription_config, _unsubscribe=self.unsubscribe)

    async def unsubscribe(self, subscription_id: str) -> None:
        del self._active_subscriptions[subscription_id]
        await self.maybe_write_frame(UnsubscribeFrame(headers={"id": subscription_id}))

    async def resubscribe_to_active_subscriptions(self) -> None:
        for subscription_id, subscription_config in self._active_subscriptions.items():
            await self.maybe_write_frame(
                SubscribeFrame(
                    headers={
                        "id": subscription_id,
                        "destination": subscription_config.destination,
                        "ack": subscription_config.ack,
                    }
                )
            )

    async def unsubscribe_from_all_active_subscriptions(self) -> None:
        for subscription_id in self._active_subscriptions.copy():
            await self.unsubscribe(subscription_id)

    async def run_handler_for_frame(self, frame: MessageFrame) -> None:
        subscription_id = frame.headers["subscription"]
        if not (subscription_config := self._active_subscriptions.get(subscription_id)):
            return
        message_id = frame.headers["message-id"]

        try:
            await subscription_config.handler(frame)
        except subscription_config.supressed_exception_classes as exception:
            if subscription_config.should_handle_ack_nack:
                await self.maybe_write_frame(NackFrame(headers={"id": message_id, "subscription": subscription_id}))
            subscription_config.on_suppressed_exception(exception, frame)
        else:
            if subscription_config.should_handle_ack_nack:
                await self.maybe_write_frame(AckFrame(headers={"id": message_id, "subscription": subscription_id}))

    async def wait_forever_if_has_active_subscriptions(self) -> None:
        if self._active_subscriptions:
            await asyncio.Future()
