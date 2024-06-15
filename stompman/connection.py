import asyncio
import socket
from collections.abc import AsyncGenerator, Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Protocol, Self, TypedDict, TypeVar, cast

from stompman.errors import ConnectionLostError
from stompman.frames import AnyClientFrame, AnyServerFrame
from stompman.protocol import NEWLINE, Parser, dump_frame


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


FrameT = TypeVar("FrameT", bound=AnyClientFrame | AnyServerFrame)


@dataclass
class AbstractConnection(Protocol):
    @classmethod
    async def from_connection_parameters(cls, host: str, port: int, connect_timeout: int) -> Self | None: ...

    async def close(self) -> None: ...
    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: AnyClientFrame) -> None: ...
    def read_frames(self, read_max_chunk_size: int, read_timeout: int) -> AsyncGenerator[AnyServerFrame, None]: ...

    async def read_frame_of_type(self, type_: type[FrameT], read_max_chunk_size: int, read_timeout: int) -> FrameT:
        while True:
            async for frame in self.read_frames(read_max_chunk_size=read_max_chunk_size, read_timeout=read_timeout):
                if isinstance(frame, type_):
                    return frame


@contextmanager
def _reraise_connection_lost(*causes: type[Exception]) -> Generator[None, None, None]:
    try:
        yield
    except causes as exception:
        raise ConnectionLostError from exception


@dataclass
class Connection(AbstractConnection):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    @classmethod
    async def from_connection_parameters(cls, host: str, port: int, connect_timeout: int) -> Self | None:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=connect_timeout)
        except (TimeoutError, ConnectionError, socket.gaierror):
            return None
        else:
            return cls(reader=reader, writer=writer)

    async def close(self) -> None:
        self.writer.close()
        with _reraise_connection_lost(ConnectionError):
            await self.writer.wait_closed()

    def write_heartbeat(self) -> None:
        return self.writer.write(NEWLINE)

    async def write_frame(self, frame: AnyClientFrame) -> None:
        self.writer.write(dump_frame(frame))
        with _reraise_connection_lost(ConnectionError):
            await self.writer.drain()

    async def _read_non_empty_bytes(self, read_max_chunk_size: int) -> bytes:
        while (chunk := await self.reader.read(read_max_chunk_size)) == b"":  # pragma: no cover (it defenitely happens)
            await asyncio.sleep(0)
        return chunk

    async def read_frames(self, read_max_chunk_size: int, read_timeout: int) -> AsyncGenerator[AnyServerFrame, None]:
        parser = Parser()

        while True:
            with _reraise_connection_lost(ConnectionError, TimeoutError):
                raw_frames = await asyncio.wait_for(
                    self._read_non_empty_bytes(read_max_chunk_size), timeout=read_timeout
                )

            for frame in cast(Iterator[AnyServerFrame], parser.load_frames(raw_frames)):
                yield frame
