import typing

import stompman
from fast_depends.dependencies import Depends
from faststream.broker.core.abc import ABCBroker
from faststream.broker.types import CustomCallable, PublisherMiddleware, SubscriberMiddleware
from faststream.broker.utils import default_filter

from faststream_stomp.publisher import StompPublisher
from faststream_stomp.subscriber import StompSubscriber


def noop_handle_suppressed_exception(exception: Exception, message: stompman.MessageFrame) -> None: ...


class StompRegistrator(ABCBroker[stompman.MessageFrame]):
    _subscribers: typing.Mapping[int, StompSubscriber]
    _publishers: typing.Mapping[int, StompPublisher]

    def subscriber(  # type: ignore[override]
        self,
        destination: str,
        *,
        ack: stompman.AckMode = "client-individual",
        headers: dict[str, str] | None = None,
        on_suppressed_exception: typing.Callable[
            [Exception, stompman.MessageFrame], typing.Any
        ] = noop_handle_suppressed_exception,
        suppressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
        # other args
        dependencies: typing.Iterable[Depends] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        middlewares: typing.Sequence[SubscriberMiddleware[stompman.MessageFrame]] = (),
        retry: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> StompSubscriber:
        subscriber = typing.cast(
            StompSubscriber,
            super().subscriber(
                StompSubscriber(
                    destination=destination,
                    ack=ack,
                    headers=headers,
                    on_suppressed_exception=on_suppressed_exception,
                    suppressed_exception_classes=suppressed_exception_classes,
                    retry=retry,
                    broker_middlewares=self._middlewares,
                    broker_dependencies=self._dependencies,
                    title_=title,
                    description_=description,
                    include_in_schema=self._solve_include_in_schema(include_in_schema),
                )
            ),
        )
        return subscriber.add_call(
            filter_=default_filter,
            parser_=parser or self._parser,  # type: ignore[arg-type]
            decoder_=decoder or self._decoder,  # type: ignore[arg-type]
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    def publisher(  # type: ignore[override]
        self,
        destination: str,
        *,
        middlewares: typing.Sequence[PublisherMiddleware] = (),
        schema_: typing.Any | None = None,  # noqa: ANN401
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
    ) -> StompPublisher:
        return typing.cast(
            StompPublisher,
            super().publisher(
                StompPublisher(
                    destination,
                    broker_middlewares=self._middlewares,
                    middlewares=middlewares,
                    schema_=schema_,
                    title_=title_,
                    description_=description_,
                    include_in_schema=include_in_schema,
                )
            ),
        )
