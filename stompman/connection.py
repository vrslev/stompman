import asyncio
from collections.abc import AsyncGenerator, Iterable
from dataclasses import dataclass, field
from typing import Protocol, TypeVar, cast

from stompman.errors import ConnectError, ReadTimeoutError
from stompman.frames import ClientFrame, ServerFrame, UnknownFrame
from stompman.protocol import HEARTBEAT_MARKER, dump_frame, load_frames, separate_complete_and_incomplete_packet_parts


@dataclass
class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str = field(repr=False)


ServerFrameT = TypeVar("ServerFrameT", bound=ServerFrame | UnknownFrame)


@dataclass
class AbstractConnection(Protocol):
    connection_parameters: ConnectionParameters
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: ClientFrame | UnknownFrame) -> None: ...
    def read_frames(self) -> AsyncGenerator[ServerFrame | UnknownFrame, None]: ...

    async def read_frame_of_type(self, type_: type[ServerFrameT]) -> ServerFrameT:
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

    async def connect(self) -> None:
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.connection_parameters.host, self.connection_parameters.port),
                timeout=self.connect_timeout,
            )
        except (TimeoutError, ConnectionError) as exception:
            raise ConnectError(self.connection_parameters) from exception

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    def write_heartbeat(self) -> None:
        return self.writer.write(HEARTBEAT_MARKER)

    async def write_frame(self, frame: ClientFrame | UnknownFrame) -> None:
        self.writer.write(dump_frame(frame))
        await self.writer.drain()

    async def _read_non_empty_bytes(self) -> bytes:
        while (
            chunk := await self.reader.read(self.read_max_chunk_size)
        ) == b"":  # pragma: no cover (it defenitely happens)
            await asyncio.sleep(0)
        return chunk

    async def read_frames(self) -> AsyncGenerator[ServerFrame | UnknownFrame, None]:
        incomplete_bytes = b""

        while True:
            try:
                received_bytes = await asyncio.wait_for(self._read_non_empty_bytes(), timeout=self.read_timeout)
            except TimeoutError as exception:
                raise ReadTimeoutError(timeout=self.read_timeout) from exception

            complete_bytes, incomplete_bytes = separate_complete_and_incomplete_packet_parts(
                incomplete_bytes + received_bytes
            )
            for frame in cast(Iterable[ServerFrame | UnknownFrame], load_frames(complete_bytes)):
                yield frame
