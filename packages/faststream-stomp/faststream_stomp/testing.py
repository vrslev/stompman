import uuid
from typing import Any
from unittest.mock import AsyncMock

import stompman
import stompman.frames
from faststream.broker.message import encode_message
from faststream.testing.broker import TestBroker
from faststream.types import SendableMessage
from stompman import MessageFrame

from faststream_stomp.broker import StompBroker
from faststream_stomp.publisher import StompProducer, StompPublisher
from faststream_stomp.subscriber import StompSubscriber


class TestStompBroker(TestBroker[StompBroker]):
    @staticmethod
    def create_publisher_fake_subscriber(
        broker: StompBroker, publisher: StompPublisher
    ) -> tuple[StompSubscriber, bool]:
        sub: StompSubscriber | None = None
        for handler in broker._subscribers.values():  # noqa: SLF001
            if handler.destination == publisher.destination:
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(publisher.destination)
        else:
            is_real = True

        return sub, is_real

    @staticmethod
    async def _fake_connect(
        broker: StompBroker,
        *args: Any,  # noqa: ANN401, ARG004
        **kwargs: Any,  # noqa: ANN401, ARG004
    ) -> None:
        broker._connection = AsyncMock()  # noqa: SLF001
        broker._producer = FakeStompProducer(broker)  # noqa: SLF001


class FakeStompProducer(StompProducer):
    def __init__(self, broker: StompBroker) -> None:
        self.broker = broker

    async def publish(  # type: ignore[override]
        self,
        message: SendableMessage,
        *,
        destination: str,
        correlation_id: str | None,
        headers: dict[str, str] | None,
    ) -> None:
        body, content_type = encode_message(message)
        all_headers: stompman.frames.MessageHeaders = (headers.copy() if headers else {}) | {  # type: ignore[assignment]
            "destination": destination,
            "message-id": str(uuid.uuid4()),
            "subscription": str(uuid.uuid4()),
        }
        if correlation_id:
            all_headers["correlation-id"] = correlation_id  # type: ignore[typeddict-unknown-key]
        if content_type:
            all_headers["content-type"] = content_type
        frame = MessageFrame(headers=all_headers, body=body)

        for handler in self.broker._subscribers.values():  # noqa: SLF001
            if handler.destination == destination:
                await handler.process_message(frame)
