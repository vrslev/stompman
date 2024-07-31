from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from types import TracebackType

from stompman.client import ConnectionParameters
from stompman.connection import AbstractConnection
from stompman.frames import AnyClientFrame, AnyServerFrame


@dataclass(kw_only=True, slots=True)
class ConnectionManager:
    servers: list[ConnectionParameters]
    lifespan: Callable[
        [AbstractConnection, ConnectionParameters], AbstractAsyncContextManager[None]
    ]  # It shouldn't raise any runtime error, such as version error

    _active_connection: AbstractConnection
    _active_connection_parameters: ConnectionParameters
    _active_lifespan: AbstractAsyncContextManager[None]
    _exit_stack: AsyncExitStack = field(default_factory=AsyncExitStack, init=False)

    async def __aenter__(self) -> None: ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self._active_lifespan.__aexit__(exc_type, exc_value, traceback)
        await self._active_connection.close()

    async def _connect_to_any_server(self) -> tuple[AbstractConnection, ConnectionParameters]: ...

    async def _reconnect(self) -> None:
        self._active_connection, self._active_connection_parameters = await self._connect_to_any_server()
        self._active_lifespan = self.lifespan(self._active_connection, self._active_connection_parameters)
        await self._active_lifespan.__aenter__()  # noqa: PLC2801

    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: AnyClientFrame) -> None: ...
    def read_frames(self) -> AsyncGenerator[AnyServerFrame, None]: ...  # type: ignore[empty-body]
