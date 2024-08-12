import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from types import TracebackType
from typing import Protocol, Self

from stompman.config import ConnectionParameters
from stompman.connection import AbstractConnection
from stompman.errors import (
    AllServersUnavailable,
    AnyConnectionIssue,
    ConnectionLost,
    ConnectionLostDuringOperationError,
    ConnectionLostError,
    FailedAllConnectAttemptsError,
    StompProtocolConnectionIssue,
)
from stompman.frames import AnyClientFrame, AnyServerFrame


class AbstractConnectionLifespan(Protocol):
    async def enter(self) -> StompProtocolConnectionIssue | None: ...
    async def exit(self) -> None: ...


class ConnectionLifespanFactory(Protocol):
    def __call__(
        self, *, connection: AbstractConnection, connection_parameters: ConnectionParameters
    ) -> AbstractConnectionLifespan: ...


@dataclass(frozen=True, kw_only=True, slots=True)
class ActiveConnectionState:
    connection: AbstractConnection
    lifespan: AbstractConnectionLifespan


@dataclass(kw_only=True, slots=True)
class ConnectionManager:
    servers: list[ConnectionParameters]
    lifespan_factory: ConnectionLifespanFactory
    connection_class: type[AbstractConnection]
    connect_retry_attempts: int
    connect_retry_interval: int
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int
    write_retry_attempts: int

    _active_connection_state: ActiveConnectionState | None = field(default=None, init=False)
    _reconnect_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def __aenter__(self) -> Self:
        self._active_connection_state = await self._get_active_connection_state()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        if not self._active_connection_state:
            return
        try:
            await self._active_connection_state.lifespan.exit()
        except ConnectionLostError:
            return
        await self._active_connection_state.connection.close()

    async def _connect_to_one_server(
        self, server: ConnectionParameters
    ) -> tuple[AbstractConnection, ConnectionParameters] | None:
        if connection := await self.connection_class.connect(
            host=server.host,
            port=server.port,
            timeout=self.connect_timeout,
            read_max_chunk_size=self.read_max_chunk_size,
            read_timeout=self.read_timeout,
        ):
            return connection, server
        return None

    async def _connect_to_any_server(self) -> tuple[AbstractConnection, ConnectionParameters] | None:
        for maybe_connection_future in asyncio.as_completed(
            [self._connect_to_one_server(server) for server in self.servers]
        ):
            if maybe_result := await maybe_connection_future:
                return maybe_result
        return None

    async def _connect_to_any_server_and_lifespan(self) -> ActiveConnectionState | AnyConnectionIssue:
        if not (connection_and_connection_parameters := await self._connect_to_any_server()):
            return AllServersUnavailable(servers=self.servers, timeout=self.connect_timeout)
        connection, connection_parameters = connection_and_connection_parameters

        active_connection_state = ActiveConnectionState(
            connection=connection,
            lifespan=self.lifespan_factory(connection=connection, connection_parameters=connection_parameters),
        )

        try:
            connection_issue = await active_connection_state.lifespan.enter()
        except ConnectionLostError:
            return ConnectionLost()

        if connection_issue is None:
            return active_connection_state
        return connection_issue

    async def _get_active_connection_state(self) -> ActiveConnectionState:
        if self._active_connection_state:
            return self._active_connection_state

        connection_issues: list[AnyConnectionIssue] = []

        async with self._reconnect_lock:
            for attempt in range(self.connect_retry_attempts):
                connection_result = await self._connect_to_any_server_and_lifespan()

                if isinstance(connection_result, ActiveConnectionState):
                    self._active_connection_state = connection_result
                    return connection_result

                connection_issues.append(connection_result)
                await asyncio.sleep(self.connect_retry_interval * (attempt + 1))

        raise FailedAllConnectAttemptsError(retry_attempts=self.connect_retry_attempts, issues=connection_issues)

    def _clear_active_connection_state(self) -> None:
        self._active_connection_state = None

    async def write_heartbeat_reconnecting(self) -> None:
        for _ in range(self.write_retry_attempts):
            connection_state = await self._get_active_connection_state()
            try:
                return connection_state.connection.write_heartbeat()
            except ConnectionLostError:
                self._clear_active_connection_state()

        raise ConnectionLostDuringOperationError(retry_attempts=self.connect_retry_attempts)

    async def write_frame_reconnecting(self, frame: AnyClientFrame) -> None:
        for _ in range(self.write_retry_attempts):
            connection_state = await self._get_active_connection_state()
            try:
                return await connection_state.connection.write_frame(frame)
            except ConnectionLostError:
                self._clear_active_connection_state()

        raise ConnectionLostDuringOperationError(retry_attempts=self.connect_retry_attempts)

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
