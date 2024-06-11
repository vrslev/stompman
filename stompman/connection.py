import asyncio
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass, field
from typing import Protocol, TypeVar, cast

from stompman.errors import ConnectionLostError
from stompman.frames import AnyClientFrame, AnyServerFrame
from stompman.protocol import NEWLINE, Parser, dump_frame


@dataclass
class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str = field(repr=False)


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
