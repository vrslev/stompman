import asyncio
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass, field
from typing import Protocol, Self, TypedDict, TypeVar, cast

from stompman.errors import ConnectionLostError
from stompman.frames import AnyClientFrame, AnyServerFrame
from stompman.protocol import NEWLINE, Parser, dump_frame


class _MultiHostHostLike(TypedDict):
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
    def from_pydantic_multihost_hosts(cls, hosts: list[_MultiHostHostLike]) -> list[Self]:
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
                ArtemisDsn("tcp://lev:pass@host1:61616,host1:61617,host2:61616").hosts()
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


FrameT = TypeVar("FrameT", bound=AnyClientFrame | AnyServerFrame)


@dataclass
class AbstractConnection(Protocol):
    connection_parameters: ConnectionParameters
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int

    async def connect(self) -> bool: ...
    async def close(self) -> None: ...
    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: AnyClientFrame) -> None: ...
    def read_frames(self) -> AsyncGenerator[AnyServerFrame, None]: ...

    async def read_frame_of_type(self, type_: type[FrameT]) -> FrameT:
        while True:
            async for frame in self.read_frames():
                if isinstance(frame, type_):
                    return frame


@dataclass
class Connection(AbstractConnection):
    connection_parameters: ConnectionParameters
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int
    reader: asyncio.StreamReader = field(init=False)
    writer: asyncio.StreamWriter = field(init=False)

    async def connect(self) -> bool:
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.connection_parameters.host, self.connection_parameters.port),
                timeout=self.connect_timeout,
            )
        except (TimeoutError, ConnectionError):
            return False
        return True

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    def write_heartbeat(self) -> None:
        return self.writer.write(NEWLINE)

    async def write_frame(self, frame: AnyClientFrame) -> None:
        self.writer.write(dump_frame(frame))
        await self.writer.drain()

    async def _read_non_empty_bytes(self) -> bytes:
        while (
            chunk := await self.reader.read(self.read_max_chunk_size)
        ) == b"":  # pragma: no cover (it defenitely happens)
            await asyncio.sleep(0)
        return chunk

    async def read_frames(self) -> AsyncGenerator[AnyServerFrame, None]:
        parser = Parser()

        while True:
            try:
                raw_frames = await asyncio.wait_for(self._read_non_empty_bytes(), timeout=self.read_timeout)
            except TimeoutError as exception:
                raise ConnectionLostError(self.read_timeout) from exception

            for frame in cast(Iterator[AnyServerFrame], parser.load_frames(raw_frames)):
                yield frame
