import asyncio
import typing
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import NamedTuple, Self
from uuid import uuid4

from stompman.connection import AbstractConnection, Connection, ConnectionParameters
from stompman.errors import (
    ConnectError,
    ConnectionConfirmationTimeoutError,
    FailedAllConnectAttemptsError,
    UnsupportedProtocolVersionError,
)
from stompman.frames import (
    AbortFrame,
    BeginFrame,
    CommitFrame,
    ConnectedFrame,
    ConnectFrame,
    DisconnectFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    ReceiptFrame,
    SendFrame,
    SubscribeFrame,
    UnsubscribeFrame,
)
from stompman.listen_events import AnyListenEvent, ErrorEvent, HeartbeatEvent, MessageEvent, UnknownEvent
from stompman.protocol import HEARTBEAT_MARKER, PROTOCOL_VERSION


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

    async def _raise_connection_unconfirmed(self) -> None:
        await asyncio.sleep(self.connection_confirmation_timeout)
        raise ConnectionConfirmationTimeoutError(self.connection_confirmation_timeout)

    async def __aenter__(self) -> Self:
        self._connection = await self._connect_to_any_server()
        await self._connection.write_frame(
            ConnectFrame(
                headers={
                    "accept-version": PROTOCOL_VERSION,
                    "heart-beat": self.heartbeat.dump(),
                    "login": self._connection.connection_parameters.login,
                    "passcode": self._connection.connection_parameters.passcode,
                },
            )
        )
        async with asyncio.TaskGroup() as task_group:
            raise_connection_unconfirmed_task = task_group.create_task(self._raise_connection_unconfirmed())
            async for frame in self._connection.read_frames():
                match frame:
                    case ConnectedFrame(headers={"version": version, "heart-beat": raw_heart_beat}):
                        if version != PROTOCOL_VERSION:
                            raise UnsupportedProtocolVersionError(
                                given_version=version,
                                supported_version=PROTOCOL_VERSION,
                            )
                        self._server_heartbeat = Heartbeat.load(raw_heart_beat)
                        raise_connection_unconfirmed_task.cancel()
                        break

        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        receipt_id = str(uuid4())
        await self._connection.write_frame(DisconnectFrame(headers={"receipt": receipt_id}))

        async for frame in self._connection.read_frames():
            match frame:
                case ReceiptFrame():
                    return await self._connection.close()
        return None  # pragma: no cover

    @asynccontextmanager
    async def subscribe(self, destination: str) -> typing.AsyncGenerator[None, None]:
        subscription_id = str(uuid4())
        await self._connection.write_frame(
            SubscribeFrame(
                headers={
                    "id": subscription_id,
                    "destination": destination,
                    "ack": "client-individual",
                }
            )
        )

        try:
            yield
        finally:
            await self._connection.write_frame(UnsubscribeFrame(headers={"id": subscription_id}))

    @asynccontextmanager
    async def _start_sending_heartbeats(self) -> typing.AsyncGenerator[None, None]:
        async def start() -> None:
            while True:
                self._connection.write_raw(HEARTBEAT_MARKER)
                await asyncio.sleep(send_interval)

        send_interval = (
            max(
                self.heartbeat.will_send_interval_ms,
                self._server_heartbeat.want_to_receive_interval_ms,
            )
            / 1000
        )

        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(start())
            yield

    async def listen(self) -> AsyncIterator[AnyListenEvent]:
        async with self._start_sending_heartbeats():
            async for frame in self._connection.read_frames():
                match frame:
                    case MessageFrame():
                        yield MessageEvent(_client=self, _frame=frame)
                    case ErrorFrame():
                        yield ErrorEvent(_client=self, _frame=frame)
                    case HeartbeatFrame():
                        yield HeartbeatEvent(_client=self, _frame=frame)
                    case ConnectedFrame() | ReceiptFrame():  # pragma: no cover
                        raise AssertionError("unreachable")
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
    async def enter_transaction(self) -> typing.AsyncGenerator[str, None]:
        transaction_id = str(uuid4())
        await self._connection.write_frame(BeginFrame(headers={"transaction": transaction_id}))

        try:
            yield transaction_id
        except Exception:
            await self._connection.write_frame(AbortFrame(headers={"transaction": transaction_id}))
            raise
        else:
            await self._connection.write_frame(CommitFrame(headers={"transaction": transaction_id}))
