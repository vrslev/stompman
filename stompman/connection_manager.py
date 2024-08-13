import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from stompman.config import ConnectionParameters
from stompman.connection import AbstractConnection
from stompman.errors import (
    AllServersUnavailable,
    AnyConnectionIssue,
    ConnectionLost,
    ConnectionLostError,
    FailedAllConnectAttemptsError,
    FailedAllWriteAttemptsError,
)
from stompman.frames import AnyClientFrame, AnyServerFrame

if TYPE_CHECKING:
    from stompman.connection_lifespan import AbstractConnectionLifespan, ConnectionLifespanFactory


@dataclass(frozen=True, kw_only=True, slots=True)
class ActiveConnectionState:
    connection: AbstractConnection
    lifespan: "AbstractConnectionLifespan"


Sleep = Callable[[float], Awaitable[None]]


async def attempt_to_connect(
    *,
    connect: Callable[[], Awaitable[ActiveConnectionState | AnyConnectionIssue]],
    connect_retry_interval: int,
    connect_retry_attempts: int,
    sleep: Sleep,
) -> ActiveConnectionState:
    connection_issues = []

    for attempt in range(connect_retry_attempts):
        connection_result = await connect()
        if isinstance(connection_result, ActiveConnectionState):
            return connection_result

        connection_issues.append(connection_result)
        await sleep(connect_retry_interval * (attempt + 1))

    raise FailedAllConnectAttemptsError(retry_attempts=connect_retry_attempts, issues=connection_issues)


async def connect_to_first_server(
    connect_awaitables: list[Awaitable[ActiveConnectionState | None]],
) -> ActiveConnectionState | None:
    for maybe_connection_future in asyncio.as_completed(connect_awaitables):
        if connection_state := await maybe_connection_future:
            return connection_state
    return None


async def make_healthy_connection(
    *, active_connection_state: ActiveConnectionState | None, servers: list[ConnectionParameters], connect_timeout: int
) -> ActiveConnectionState | AnyConnectionIssue:
    if not active_connection_state:
        return AllServersUnavailable(servers=servers, timeout=connect_timeout)

    try:
        connection_issue = await active_connection_state.lifespan.enter()
    except ConnectionLostError:
        return ConnectionLost()

    return active_connection_state if connection_issue is None else connection_issue


@dataclass(kw_only=True, slots=True)
class ConnectionManager:
    servers: list[ConnectionParameters]
    lifespan_factory: "ConnectionLifespanFactory"
    connection_class: type[AbstractConnection]
    connect_retry_attempts: int
    connect_retry_interval: int
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int
    write_retry_attempts: int

    _active_connection_state: ActiveConnectionState | None = field(default=None, init=False)
    _reconnect_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def enter(self) -> None:
        self._active_connection_state = await self._get_active_connection_state()

    async def exit(self) -> None:
        if not self._active_connection_state:
            return
        try:
            await self._active_connection_state.lifespan.exit()
        except ConnectionLostError:
            return
        await self._active_connection_state.connection.close()

    async def _create_connection_to_one_server(self, server: ConnectionParameters) -> ActiveConnectionState | None:
        if connection := await self.connection_class.connect(
            host=server.host,
            port=server.port,
            timeout=self.connect_timeout,
            read_max_chunk_size=self.read_max_chunk_size,
            read_timeout=self.read_timeout,
        ):
            return ActiveConnectionState(
                connection=connection,
                lifespan=self.lifespan_factory(connection=connection, connection_parameters=server),
            )
        return None

    async def _connect_to_any_server(self) -> ActiveConnectionState | AnyConnectionIssue:
        active_connection_state = await connect_to_first_server(
            [self._create_connection_to_one_server(server) for server in self.servers]
        )
        return await make_healthy_connection(
            active_connection_state=active_connection_state, servers=self.servers, connect_timeout=self.connect_timeout
        )

    async def _get_active_connection_state(self) -> ActiveConnectionState:
        if self._active_connection_state:
            return self._active_connection_state

        async with self._reconnect_lock:
            if self._active_connection_state:
                return self._active_connection_state
            self._active_connection_state = await attempt_to_connect(
                connect=self._connect_to_any_server,
                connect_retry_interval=self.connect_retry_interval,
                connect_retry_attempts=self.connect_retry_attempts,
                sleep=asyncio.sleep,
            )
            return self._active_connection_state

    def _clear_active_connection_state(self) -> None:
        self._active_connection_state = None

    async def write_heartbeat_reconnecting(self) -> None:
        for _ in range(self.write_retry_attempts):
            connection_state = await self._get_active_connection_state()
            try:
                return connection_state.connection.write_heartbeat()
            except ConnectionLostError:
                self._clear_active_connection_state()

        raise FailedAllWriteAttemptsError(retry_attempts=self.write_retry_attempts)

    async def write_frame_reconnecting(self, frame: AnyClientFrame) -> None:
        for _ in range(self.write_retry_attempts):
            connection_state = await self._get_active_connection_state()
            try:
                return await connection_state.connection.write_frame(frame)
            except ConnectionLostError:
                self._clear_active_connection_state()

        raise FailedAllWriteAttemptsError(retry_attempts=self.write_retry_attempts)

    async def read_frames_reconnecting(self) -> AsyncGenerator[AnyServerFrame, None]:
        while True:
            connection_state = await self._get_active_connection_state()
            try:
                async for frame in connection_state.connection.read_frames():
                    yield frame
            except ConnectionLostError:
                self._clear_active_connection_state()

    async def maybe_write_frame(self, frame: AnyClientFrame) -> bool:
        if not self._active_connection_state:
            return False
        try:
            await self._active_connection_state.connection.write_frame(frame)
        except ConnectionLostError:
            self._clear_active_connection_state()
            return False
        return True
