import asyncio
from collections.abc import AsyncGenerator, Iterable
from dataclasses import dataclass, field
from typing import Protocol, cast

from stompman.errors import ConnectError, ReadTimeoutError
from stompman.frames import ClientFrame, ServerFrame, UnknownFrame
from stompman.protocol import dump_frame, load_frames, separate_complete_and_incomplete_packets


@dataclass
class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str = field(repr=False)


@dataclass
class AbstractConnection(Protocol):
    connection_parameters: ConnectionParameters
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    def write_raw(self, data: bytes) -> None: ...
    async def write_frame(self, frame: ClientFrame | UnknownFrame) -> None: ...
    def read_frames(self) -> AsyncGenerator[ServerFrame | UnknownFrame, None]: ...


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

    def write_raw(self, data: bytes) -> None:
        self.writer.write(data)

    async def write_frame(self, frame: ClientFrame | UnknownFrame) -> None:
        self.writer.write(dump_frame(frame))
        await self.writer.drain()

    async def _read_non_empty_bytes(self) -> bytes:
        while (
            read_bytes := await self.reader.read(self.read_max_chunk_size)
        ) == b"":  # pragma: no cover (it defenitely happens)
            await asyncio.sleep(0)
        return read_bytes

    async def read_frames(self) -> AsyncGenerator[ServerFrame | UnknownFrame, None]:
        bytes_to_prepend_in_next_round = b""

        while True:
            try:
                read_bytes = await asyncio.wait_for(self._read_non_empty_bytes(), timeout=self.read_timeout)
            except TimeoutError as exception:
                raise ReadTimeoutError(timeout=self.read_timeout) from exception

            frame_read_bytes, bytes_to_prepend_in_next_round = separate_complete_and_incomplete_packets(
                bytes_to_prepend_in_next_round + read_bytes
            )
            for frame in cast(Iterable[ServerFrame], load_frames(frame_read_bytes)):
                yield frame
