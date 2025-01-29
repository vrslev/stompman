from collections.abc import Callable, Iterable, Sequence
from typing import Any

import stompman
from fast_depends.dependencies import Depends
from faststream.asyncapi.schema import Channel, CorrelationId, Message, Operation
from faststream.asyncapi.utils import resolve_payloads
from faststream.broker.message import StreamMessage
from faststream.broker.publisher.fake import FakePublisher
from faststream.broker.publisher.proto import ProducerProto
from faststream.broker.subscriber.usecase import SubscriberUsecase
from faststream.broker.types import AsyncCallable, BrokerMiddleware, CustomCallable
from faststream.types import AnyDict, Decorator, LoggerProto

from faststream_stomp import parser


class StompSubscriber(SubscriberUsecase[stompman.MessageFrame]):
    def __init__(
        self,
        *,
        destination: str,
        ack: stompman.AckMode = "client-individual",
        headers: dict[str, str] | None = None,
        on_suppressed_exception: Callable[[Exception, stompman.MessageFrame], Any],
        suppressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
        retry: bool | int,
        broker_dependencies: Iterable[Depends],
        broker_middlewares: Sequence[BrokerMiddleware[stompman.MessageFrame]],
        default_parser: AsyncCallable = parser.parse_message,
        default_decoder: AsyncCallable = parser.decode_message,
        # AsyncAPI information
        title_: str | None,
        description_: str | None,
        include_in_schema: bool,
    ) -> None:
        self.destination = destination
        self.ack = ack
        self.headers = headers
        self.on_suppressed_exception = on_suppressed_exception
        self.suppressed_exception_classes = suppressed_exception_classes
        super().__init__(
            no_ack=self.ack == "auto",
            no_reply=True,
            retry=retry,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            default_parser=default_parser,
            default_decoder=default_decoder,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    def setup(  # type: ignore[override]
        self,
        client: stompman.Client,
        *,
        logger: LoggerProto | None,
        producer: ProducerProto | None,
        graceful_timeout: float | None,
        extra_context: AnyDict,
        broker_parser: CustomCallable | None,
        broker_decoder: CustomCallable | None,
        apply_types: bool,
        is_validate: bool,
        _get_dependant: Callable[..., Any] | None,
        _call_decorators: Iterable[Decorator],
    ) -> None:
        self.client = client
        return super().setup(
            logger=logger,
            producer=producer,
            graceful_timeout=graceful_timeout,
            extra_context=extra_context,
            broker_parser=broker_parser,
            broker_decoder=broker_decoder,
            apply_types=apply_types,
            is_validate=is_validate,
            _get_dependant=_get_dependant,
            _call_decorators=_call_decorators,
        )

    async def start(self) -> None:
        await super().start()
        self._subscription = await self.client.subscribe(
            destination=self.destination,
            handler=self.consume,
            ack=self.ack,
            headers=self.headers,
            on_suppressed_exception=self.on_suppressed_exception,
            suppressed_exception_classes=self.suppressed_exception_classes,
        )

    async def close(self) -> None:
        await self._subscription.unsubscribe()
        await super().close()

    async def get_one(self, *, timeout: float = 5) -> None: ...

    def _make_response_publisher(self, message: StreamMessage[stompman.MessageFrame]) -> Sequence[FakePublisher]:
        return (  # pragma: no cover
            (FakePublisher(self._producer.publish, publish_kwargs={"destination": message.reply_to}),)
            if self._producer
            else ()
        )

    def __hash__(self) -> int:
        return hash(self.destination)

    def add_prefix(self, prefix: str) -> None:
        self.destination = f"{prefix}{self.destination}"

    def get_name(self) -> str:
        return f"{self.destination}:{self.call_name}"

    def get_schema(self) -> dict[str, Channel]:
        payloads = self.get_payloads()

        return {
            self.name: Channel(
                description=self.description,
                subscribe=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                        correlationId=CorrelationId(location="$message.header#/correlation_id"),
                    ),
                ),
            )
        }
