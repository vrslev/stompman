import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import NamedTuple, Self, TypedDict
from uuid import uuid4

from stompman.connection import AbstractConnection, Connection
from stompman.errors import (
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
    SendHeaders,
    SubscribeFrame,
    UnsubscribeFrame,
)
from stompman.listening_events import AnyListeningEvent, ErrorEvent, HeartbeatEvent, MessageEvent


class Heartbeat(NamedTuple):
    will_send_interval_ms: int
    want_to_receive_interval_ms: int

    def to_header(self) -> str:
        return f"{self.will_send_interval_ms},{self.want_to_receive_interval_ms}"

    @classmethod
    def from_header(cls, header: str) -> Self:
        first, second = header.split(",", maxsplit=1)
        return cls(int(first), int(second))


class MultiHostHostLike(TypedDict):
    username: str | None
    password: str | None
    host: str | None
    port: int | None


@dataclass
class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str = field(repr=False)

    @classmethod
    def from_pydantic_multihost_hosts(cls, hosts: list[MultiHostHostLike]) -> list[Self]:
        """Create connection parameters from a list of `MultiHostUrl` objects.

        .. code-block:: python
        import stompman.

        ArtemisDsn = typing.Annotated[
            pydantic_core.MultiHostUrl,
            pydantic.UrlConstraints(
                host_required=True,
                allowed_schemes=["tcp"],
            ),
        ]

        async with stompman.Client(
            servers=stompman.ConnectionParameters.from_pydantic_multihost_hosts(
                ArtemisDsn("tcp://lev:pass@host1:61616,lev:pass@host1:61617,lev:pass@host2:61616").hosts()
            ),
        ):
            ...
        """
        servers: list[Self] = []
        for host in hosts:
            if host["host"] is None:
                raise ValueError("host must be set")
            if host["port"] is None:
                raise ValueError("port must be set")
            if host["username"] is None:
                raise ValueError("username must be set")
            if host["password"] is None:
                raise ValueError("password must be set")
            servers.append(cls(host=host["host"], port=host["port"], login=host["username"], passcode=host["password"]))
        return servers


PROTOCOL_VERSION = "1.2"  # https://stomp.github.io/stomp-specification-1.2.html


@dataclass
class Client:
    servers: list[ConnectionParameters]
    heartbeat: Heartbeat = field(default=Heartbeat(1000, 1000))
    connect_retry_attempts: int = 3
    connect_retry_interval: int = 1
    connect_timeout: int = 2
    connection_confirmation_timeout: int = 2
    read_timeout: int = 2
    read_max_chunk_size: int = 1024 * 1024
    connection_class: type[AbstractConnection] = Connection

    _connection: AbstractConnection = field(init=False)
    _connection_parameters: ConnectionParameters = field(init=False)
    _exit_stack: AsyncExitStack = field(default_factory=AsyncExitStack, init=False)

    async def __aenter__(self) -> Self:
        self._connection, self._connection_parameters = await self._connect_to_any_server()
        await self._exit_stack.enter_async_context(self._connection_lifespan())
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self._exit_stack.aclose()
        await self._connection.close()

    async def _connect_to_one_server(
        self, server: ConnectionParameters
    ) -> tuple[AbstractConnection, ConnectionParameters] | None:
        for attempt in range(self.connect_retry_attempts):
            if connection := await self.connection_class.connect(
                host=server.host, port=server.port, connect_timeout=self.connect_timeout
            ):
                return connection, server
            await asyncio.sleep(self.connect_retry_interval * (attempt + 1))
        return None

    async def _connect_to_any_server(self) -> tuple[AbstractConnection, ConnectionParameters]:
        for maybe_connection_future in asyncio.as_completed(
            [self._connect_to_one_server(server) for server in self.servers]
        ):
            maybe_result = await maybe_connection_future
            if maybe_result:
                return maybe_result
        raise FailedAllConnectAttemptsError(
            servers=self.servers,
            retry_attempts=self.connect_retry_attempts,
            retry_interval=self.connect_retry_interval,
            timeout=self.connect_timeout,
        )

    @asynccontextmanager
    async def _connection_lifespan(self) -> AsyncGenerator[None, None]:
        # On startup:
        # - send CONNECT frame
        # - wait for CONNECTED frame
        # - start heartbeats
        # On shutdown:
        # - stop heartbeats
        # - send DISCONNECT frame
        # - wait for RECEIPT frame

        await self._connection.write_frame(
            ConnectFrame(
                headers={
                    "accept-version": PROTOCOL_VERSION,
                    "heart-beat": self.heartbeat.to_header(),
                    "host": self._connection_parameters.host,
                    "login": self._connection_parameters.login,
                    "passcode": self._connection_parameters.passcode,
                },
            )
        )
        try:
            connected_frame = await asyncio.wait_for(
                self._connection.read_frame_of_type(
                    ConnectedFrame, read_max_chunk_size=self.read_max_chunk_size, read_timeout=self.read_timeout
                ),
                timeout=self.connection_confirmation_timeout,
            )
        except TimeoutError as exception:
            raise ConnectionConfirmationTimeoutError(self.connection_confirmation_timeout) from exception

        if connected_frame.headers["version"] != PROTOCOL_VERSION:
            raise UnsupportedProtocolVersionError(
                given_version=connected_frame.headers["version"], supported_version=PROTOCOL_VERSION
            )

        server_heartbeat = Heartbeat.from_header(connected_frame.headers["heart-beat"])
        heartbeat_interval = (
            max(self.heartbeat.will_send_interval_ms, server_heartbeat.want_to_receive_interval_ms) / 1000
        )

        async def send_heartbeats_forever() -> None:
            while True:
                self._connection.write_heartbeat()
                await asyncio.sleep(heartbeat_interval)

        async with asyncio.TaskGroup() as task_group:
            task = task_group.create_task(send_heartbeats_forever())
            try:
                yield
            finally:
                task.cancel()

        await self._connection.write_frame(DisconnectFrame(headers={"receipt": str(uuid4())}))
        await self._connection.read_frame_of_type(
            ReceiptFrame, read_max_chunk_size=self.read_max_chunk_size, read_timeout=self.read_timeout
        )

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

    async def listen(self) -> AsyncIterator[AnyListeningEvent]:
        async for frame in self._connection.read_frames(
            read_max_chunk_size=self.read_max_chunk_size, read_timeout=self.read_timeout
        ):
            match frame:
                case MessageFrame():
                    yield MessageEvent(_client=self, _frame=frame)
                case ErrorFrame():
                    yield ErrorEvent(_client=self, _frame=frame)
                case HeartbeatFrame():
                    yield HeartbeatEvent(_client=self, _frame=frame)
                case ConnectedFrame() | ReceiptFrame():
                    raise AssertionError("Should be unreachable! Report the issue.", frame)

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

    async def send(  # noqa: PLR0913
        self,
        body: bytes,
        destination: str,
        transaction: str | None = None,
        content_type: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        full_headers: SendHeaders = headers or {}  # type: ignore[assignment]
        full_headers["destination"] = destination
        full_headers["content-length"] = str(len(body))
        if content_type is not None:
            full_headers["content-type"] = content_type
        if transaction is not None:
            full_headers["transaction"] = transaction
        await self._connection.write_frame(SendFrame(headers=full_headers, body=body))
