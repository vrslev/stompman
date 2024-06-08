import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import NamedTuple, Self
from uuid import uuid4

from stompman.connection import AbstractConnection, Connection, ConnectionParameters
from stompman.contexts import enter_connection_context
from stompman.errors import (
    ConnectError,
    FailedAllConnectAttemptsError,
)
from stompman.frames import (
    AbortFrame,
    BeginFrame,
    CommitFrame,
    ConnectedFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    ReceiptFrame,
    SendFrame,
    SubscribeFrame,
    UnsubscribeFrame,
)
from stompman.listen_events import AnyListenEvent, ErrorEvent, HeartbeatEvent, MessageEvent, UnknownEvent


class Heartbeat(NamedTuple):
    will_send_interval_ms: int
    want_to_receive_interval_ms: int

    def dump(self) -> str:
        return f"{self.will_send_interval_ms},{self.want_to_receive_interval_ms}"

    @classmethod
    def load(cls, value: str) -> Self:
        first, second = value.split(",", maxsplit=1)
        return cls(int(first), int(second))


@dataclass
class Client:
    servers: list[ConnectionParameters]
    heartbeat: Heartbeat = field(default=Heartbeat(1000, 1000))
    connect_retry_attempts: int = 3
    connect_retry_interval: int = 1
    connect_timeout: int = 2
    connection_confirmation_timeout = 2
    read_timeout: int = 2
    read_max_chunk_size: int = 1024 * 1024
    connection_class: type[AbstractConnection] = Connection

    _heartbeat_exitstack: contextlib.AsyncExitStack = field(init=False, default_factory=contextlib.AsyncExitStack)
    _connection: AbstractConnection = field(init=False)
    _server_heartbeat: Heartbeat = field(init=False)

    async def _connect_to_one_server(self, server: ConnectionParameters) -> AbstractConnection | None:
        for attempt in range(self.connect_retry_attempts):
            connection = self.connection_class(
                connection_parameters=server,
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                read_max_chunk_size=self.read_max_chunk_size,
            )
            try:
                await connection.connect()
            except ConnectError:
                await asyncio.sleep(self.connect_retry_interval * (attempt + 1))
            else:
                return connection
        return None

    async def _connect_to_any_server(self) -> AbstractConnection:
        for maybe_connection_future in asyncio.as_completed(
            [self._connect_to_one_server(server) for server in self.servers]
        ):
            maybe_connection = await maybe_connection_future
            if maybe_connection:
                return maybe_connection
        raise FailedAllConnectAttemptsError(
            servers=self.servers,
            retry_attempts=self.connect_retry_attempts,
            retry_interval=self.connect_retry_interval,
            timeout=self.connect_timeout,
        )

    async def __aenter__(self) -> Self:
        self._connection = await self._connect_to_any_server()
        await self._heartbeat_exitstack.enter_async_context(
            enter_connection_context(
                connection=self._connection,
                client_heartbeat=self.heartbeat,
                connection_confirmation_timeout=self.connection_confirmation_timeout,
            )
        )
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self._heartbeat_exitstack.aclose()

    @asynccontextmanager
    async def subscribe(self, destination: str) -> AsyncGenerator[None, None]:
        subscription_id = str(uuid4())
        await self._connection.write_frame(
            SubscribeFrame(headers={"id": subscription_id, "destination": destination, "ack": "client-individual"})
        )
        try:
            yield
        finally:
            await self._connection.write_frame(UnsubscribeFrame(headers={"id": subscription_id}))

    async def listen(self) -> AsyncIterator[AnyListenEvent]:
        async for frame in self._connection.read_frames():
            match frame:
                case MessageFrame():
                    yield MessageEvent(_client=self, _frame=frame)
                case ErrorFrame():
                    yield ErrorEvent(_client=self, _frame=frame)
                case HeartbeatFrame():
                    yield HeartbeatEvent(_client=self, _frame=frame)
                case ConnectedFrame() | ReceiptFrame():
                    raise AssertionError("Should be unreachable! Report the issue.", frame)
                case _:
                    yield UnknownEvent(_client=self, _frame=frame)

    async def send(
        self,
        body: bytes,
        destination: str,
        transaction: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        full_headers = headers or {}
        full_headers["destination"] = destination
        full_headers["content-length"] = str(len(body))
        if transaction is not None:
            full_headers["transaction"] = transaction
        await self._connection.write_frame(SendFrame(headers=full_headers, body=body))

    @asynccontextmanager
    async def enter_transaction(self) -> AsyncGenerator[str, None]:
        transaction_id = str(uuid4())
        await self._connection.write_frame(BeginFrame(headers={"transaction": transaction_id}))

        try:
            yield transaction_id
        except Exception:
            await self._connection.write_frame(AbortFrame(headers={"transaction": transaction_id}))
            raise
        else:
            await self._connection.write_frame(CommitFrame(headers={"transaction": transaction_id}))
