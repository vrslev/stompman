import asyncio
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Literal, Self

from stompman.connection import AbstractConnection, Connection
from stompman.errors import FailedAllConnectAttemptsError
from stompman.frames import ErrorFrame, MessageFrame
from stompman.protocol import ConnectionParameters, Heartbeat, StompProtocol, Subscription, Transaction


@dataclass(kw_only=True, slots=True)
class Client:
    servers: list[ConnectionParameters] = field(kw_only=False)
    heartbeat: Heartbeat = field(default=Heartbeat(1000, 1000))
    connect_retry_attempts: int = 3
    connect_retry_interval: int = 1
    connect_timeout: int = 2
    connection_confirmation_timeout: int = 2
    on_error_frame: Callable[[ErrorFrame], None] | None = None
    on_heartbeat: Callable[[], None] | None = None
    read_timeout: int = 2
    read_max_chunk_size: int = 1024 * 1024
    connection_class: type[AbstractConnection] = Connection

    _protocol: StompProtocol = field(init=False)

    async def __aenter__(self) -> Self:
        connection, connection_parameters = await self._connect_to_any_server()
        self._protocol = await StompProtocol(
            connection=connection,
            connection_parameters=connection_parameters,
            heartbeat=self.heartbeat,
            connection_confirmation_timeout=self.connection_confirmation_timeout,
            on_error_frame=self.on_error_frame,
            on_heartbeat=self.on_heartbeat,
            read_timeout=self.read_timeout,
            read_max_chunk_size=self.read_max_chunk_size,
        ).__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self._protocol.__aexit__(exc_type, exc_value, traceback)

    async def _connect_to_one_server(
        self, server: ConnectionParameters
    ) -> tuple[AbstractConnection, ConnectionParameters] | None:
        for attempt in range(self.connect_retry_attempts):
            if connection := await self.connection_class.connect(
                host=server.host, port=server.port, timeout=self.connect_timeout
            ):
                return connection, server
            await asyncio.sleep(self.connect_retry_interval * (attempt + 1))
        return None

    async def _connect_to_any_server(self) -> tuple[AbstractConnection, ConnectionParameters]:
        for maybe_connection_future in asyncio.as_completed(
            [self._connect_to_one_server(server) for server in self.servers]
        ):
            if maybe_result := await maybe_connection_future:
                return maybe_result
        raise FailedAllConnectAttemptsError(
            servers=self.servers,
            retry_attempts=self.connect_retry_attempts,
            retry_interval=self.connect_retry_interval,
            timeout=self.connect_timeout,
        )

    @asynccontextmanager
    async def begin(self) -> AsyncGenerator[Transaction, None]:
        async with self._protocol.begin() as transaction:
            yield transaction

    async def send(
        self, body: bytes, destination: str, content_type: str | None = None, headers: dict[str, str] | None = None
    ) -> None:
        return await self._protocol.send(body=body, destination=destination, content_type=content_type, headers=headers)

    async def subscribe(  # noqa: PLR0913
        self,
        destination: str,
        handler: Callable[[MessageFrame], Coroutine[None, None, None]],
        on_suppressed_exception: Callable[[Exception, MessageFrame], Any],
        supressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
        ack: Literal["client", "client-individual", "auto"] = "client-individual",
    ) -> Subscription:
        return await self._protocol.subscribe(
            destination=destination,
            handler=handler,
            on_suppressed_exception=on_suppressed_exception,
            supressed_exception_classes=supressed_exception_classes,
            ack=ack,
        )
