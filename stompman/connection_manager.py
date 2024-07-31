import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from types import TracebackType
from typing import NamedTuple, Self, TypedDict, TypeVar
from urllib.parse import unquote

from stompman.connection import AbstractConnection
from stompman.errors import ConnectionLostError, FailedAllConnectAttemptsError
from stompman.frames import AnyClientFrame, AnyServerFrame


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


@dataclass(frozen=True, slots=True)
class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str = field(repr=False)

    @property
    def unescaped_passcode(self) -> str:
        return unquote(self.passcode)

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
                msg = "host must be set"
                raise ValueError(msg)
            if host["port"] is None:
                msg = "port must be set"
                raise ValueError(msg)
            if host["username"] is None:
                msg = "username must be set"
                raise ValueError(msg)
            if host["password"] is None:
                msg = "password must be set"
                raise ValueError(msg)

            servers.append(cls(host=host["host"], port=host["port"], login=host["username"], passcode=host["password"]))
        return servers


@dataclass(frozen=True, kw_only=True, slots=True)
class ActiveConnectionState:
    connection: AbstractConnection
    connection_parameters: ConnectionParameters
    lifespan: AbstractAsyncContextManager[None]


T = TypeVar("T")


@dataclass(kw_only=True, slots=True)
class ConnectionManager:
    servers: list[ConnectionParameters]
    lifespan: Callable[
        [AbstractConnection, ConnectionParameters], AbstractAsyncContextManager[None]
    ]  # It shouldn't raise any runtime error, such as version error

    connection_class: type[AbstractConnection]
    connect_retry_attempts: int
    connect_retry_interval: int
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int

    _active_connection_state: ActiveConnectionState | None = None
    _exit_stack: AsyncExitStack = field(default_factory=AsyncExitStack, init=False)

    active: bool = True

    async def __aenter__(self) -> None:
        self._active_connection_state = await self._connect()

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        if not self._active_connection_state:
            return
        await self._active_connection_state.lifespan.__aexit__(exc_type, exc_value, traceback)
        await self._active_connection_state.connection.close()

    async def _connect_to_one_server(
        self, server: ConnectionParameters
    ) -> tuple[AbstractConnection, ConnectionParameters] | None:
        for attempt in range(self.connect_retry_attempts):
            if connection := await self.connection_class.connect(
                host=server.host,
                port=server.port,
                timeout=self.connect_timeout,
                read_max_chunk_size=self.read_max_chunk_size,
                read_timeout=self.read_timeout,
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

    async def _connect(self) -> ActiveConnectionState:
        if self._active_connection_state:
            return self._active_connection_state
        connection, connection_parameters = await self._connect_to_any_server()
        lifespan = self.lifespan(connection, connection_parameters)
        self._active_connection_state = ActiveConnectionState(
            connection=connection, connection_parameters=connection_parameters, lifespan=lifespan
        )
        await lifespan.__aenter__()  # noqa: PLC2801
        return self._active_connection_state

    def _remove_active_connection(self) -> None:
        self._active_connection_state = None

    async def _reconnect_if_not_already(self) -> ActiveConnectionState:
        return await self._connect()

    async def _run_with_ensured_connection(self, func: Callable[[AbstractConnection], Awaitable[T]]) -> T:
        while True:
            connection_state = await self._reconnect_if_not_already()
            try:
                return await func(connection_state.connection)
            except ConnectionLostError:
                self._remove_active_connection()

    async def write_heartbeat(self) -> None:
        async def inner(connection: AbstractConnection) -> None:  # noqa: RUF029
            return connection.write_heartbeat()

        return await self._run_with_ensured_connection(inner)

    async def write_frame(self, frame: AnyClientFrame) -> None:
        return await self._run_with_ensured_connection(lambda connection: connection.write_frame(frame))

    async def read_frames(self) -> AsyncGenerator[AnyServerFrame, None]:
        if not self._active_connection_state:
            self._active_connection_state = await self._reconnect_if_not_already()
        while True:
            try:
                async for frame in self._active_connection_state.connection.read_frames():
                    yield frame
            except ConnectionLostError:
                self._active_connection_state = await self._reconnect_if_not_already()
            else:
                return
