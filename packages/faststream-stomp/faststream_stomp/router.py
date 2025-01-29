import typing

import stompman
from fast_depends.dependencies import Depends
from faststream.broker.router import ArgsContainer, BrokerRouter, SubscriberRoute
from faststream.broker.types import BrokerMiddleware, CustomCallable, PublisherMiddleware, SubscriberMiddleware
from faststream.types import SendableMessage

from faststream_stomp.registrator import StompRegistrator, noop_handle_suppressed_exception


class StompRoutePublisher(ArgsContainer): # todo: test
    """Delayed StompPublisher registration object.

    Just a copy of StompRegistrator.publisher(...) arguments.
    """

    def __init__(
        self,
        destination: str,
        *,
        middlewares: typing.Sequence[PublisherMiddleware] = (),
        schema_: typing.Any | None = None,  # noqa: ANN401
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            destination=destination,
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )


class StompRoute(SubscriberRoute):
    """Class to store delayed StompBroker subscriber registration.

    Just a copy of StompRegistrator.subscriber(...) arguments + `call` and `publishers` argument.
    """

    def __init__(
        self,
        call: typing.Callable[..., SendableMessage] | typing.Callable[..., typing.Awaitable[SendableMessage]],
        destination: str,
        *,
        ack: stompman.AckMode = "client-individual",
        headers: dict[str, str] | None = None,
        on_suppressed_exception: typing.Callable[
            [Exception, stompman.MessageFrame], typing.Any
        ] = noop_handle_suppressed_exception,
        suppressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
        # other args
        publishers: typing.Iterable[StompRoutePublisher] = (),
        dependencies: typing.Iterable[Depends] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        middlewares: typing.Sequence[SubscriberMiddleware[stompman.MessageFrame]] = (),
        retry: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call=call,
            destination=destination,
            ack=ack,
            headers=headers,
            on_suppressed_exception=on_suppressed_exception,
            suppressed_exception_classes=suppressed_exception_classes,
            publishers=publishers,  # type: ignore[arg-type]
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            retry=retry,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class StompRouter(StompRegistrator, BrokerRouter[stompman.MessageFrame]):
    """Includable to StompBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: typing.Iterable[StompRoute] = (),
        *,
        dependencies: typing.Iterable[Depends] = (),
        middlewares: typing.Sequence[BrokerMiddleware[stompman.MessageFrame]] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        include_in_schema: bool | None = None,
    ) -> None:
        super().__init__(
            handlers=handlers,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            parser=parser,
            decoder=decoder,
            include_in_schema=include_in_schema,
        )
