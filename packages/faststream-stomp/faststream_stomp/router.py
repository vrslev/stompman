from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Any

import stompman
from fast_depends.dependencies import Depends
from faststream.broker.router import ArgsContainer, BrokerRouter, SubscriberRoute
from faststream.broker.types import BrokerMiddleware, CustomCallable, PublisherMiddleware, SubscriberMiddleware
from faststream.types import SendableMessage

from faststream_stomp.registrator import StompRegistrator, noop_handle_suppressed_exception


class StompRoutePublisher(ArgsContainer):
    """Delayed StompPublisher registration object.

    Just a copy of StompRegistrator.publisher(...) arguments.
    """

    def __init__(
        self,
        destination: str,
        *,
        middlewares: Sequence[PublisherMiddleware] = (),
        schema_: Any | None = None,  # noqa: ANN401
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
        call: Callable[..., SendableMessage] | Callable[..., Awaitable[SendableMessage]],
        destination: str,
        *,
        ack: stompman.AckMode = "client-individual",
        headers: dict[str, str] | None = None,
        on_suppressed_exception: Callable[[Exception, stompman.MessageFrame], Any] = noop_handle_suppressed_exception,
        suppressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
        # other args
        publishers: Iterable[StompRoutePublisher] = (),
        dependencies: Iterable[Depends] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        middlewares: Sequence[SubscriberMiddleware[stompman.MessageFrame]] = (),
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
            publishers=publishers,
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
        handlers: Iterable[StompRoute] = (),
        *,
        dependencies: Iterable[Depends] = (),
        middlewares: Sequence[BrokerMiddleware[stompman.MessageFrame]] = (),
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
