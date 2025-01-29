import functools
import itertools
import typing

import stompman
from faststream.asyncapi.schema import Channel, CorrelationId, Message, Operation
from faststream.asyncapi.utils import resolve_payloads
from faststream.broker.message import encode_message
from faststream.broker.publisher.proto import ProducerProto
from faststream.broker.publisher.usecase import PublisherUsecase
from faststream.broker.types import AsyncCallable, BrokerMiddleware, PublisherMiddleware
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.types import AsyncFunc, SendableMessage


class StompProducer(ProducerProto):
    _parser: AsyncCallable
    _decoder: AsyncCallable

    def __init__(self, client: stompman.Client) -> None:
        self.client = client

    async def publish(  # type: ignore[override]
        self,
        *,
        message: SendableMessage,
        destination: str,
        correlation_id: str | None,
        headers: dict[str, str] | None,
    ) -> None:
        body, content_type = encode_message(message)
        all_headers = headers.copy() if headers else {}
        if correlation_id:  # TODO: test
            all_headers["correlation-id"] = correlation_id
        await self.client.send(body, destination, content_type=content_type, headers=all_headers)

    async def request(  # type: ignore[override]  # noqa: PLR6301
        self, message: SendableMessage, *, correlation_id: str | None, headers: dict[str, str] | None
    ) -> typing.Any:  # noqa: ANN401
        msg = "`StompProducer` can be used only to publish a response for `reply-to` or `RPC` messages."
        raise NotImplementedError(msg)


class StompPublisher(PublisherUsecase[stompman.MessageFrame]):
    _producer: StompProducer | None

    def __init__(
        self,
        destination: str,
        *,
        broker_middlewares: typing.Sequence[BrokerMiddleware[stompman.MessageFrame]],
        middlewares: typing.Sequence[PublisherMiddleware],
        schema_: typing.Any | None,  # noqa: ANN401
        title_: str | None,
        description_: str | None,
        include_in_schema: bool,
    ) -> None:
        self.destination = destination
        super().__init__(
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    create = __init__  # type: ignore[assignment]

    async def publish(  # type: ignore[override]
        self,
        message: SendableMessage,
        *,
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        _extra_middlewares: typing.Sequence[PublisherMiddleware] = (),
    ) -> None:
        assert self._producer, NOT_CONNECTED_YET  # noqa: S101

        call: AsyncFunc = self._producer.publish

        for one_middleware in itertools.chain(
            self._middlewares[::-1],  # type: ignore[arg-type]
            (
                _extra_middlewares  # type: ignore[arg-type]
                or (one_middleware(None).publish_scope for one_middleware in self._broker_middlewares[::-1])
            ),
        ):
            call = functools.partial(one_middleware, call)  # type: ignore[operator, arg-type, misc]
        await self._producer.publish(
            message=message, destination=self.destination, correlation_id=correlation_id, headers=headers or {}
        )

    async def request(  # type: ignore[override]
        self, message: SendableMessage, *, correlation_id: str | None = None, headers: dict[str, str] | None = None
    ) -> typing.Any:  # noqa: ANN401
        assert self._producer, NOT_CONNECTED_YET  # noqa: S101
        return await self._producer.request(message, correlation_id=correlation_id, headers=headers)

    def __hash__(self) -> int:
        return hash(f"publisher:{self.destination}")

    def get_name(self) -> str:
        return f"{self.destination}:Publisher"

    def get_schema(self) -> dict[str, Channel]:
        payloads = self.get_payloads()

        return {
            self.name: Channel(
                description=self.description,
                publish=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                        correlationId=CorrelationId(location="$message.header#/correlation_id"),
                    ),
                ),
            )
        }

    def add_prefix(self, prefix: str) -> None:
        self.destination = f"{prefix}{self.destination}"
