import asyncio
import platform
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any
from unittest import mock

import pytest

from stompman import (
    ConnectedFrame,
    ConnectError,
    Connection,
    ConnectionParameters,
    HeartbeatFrame,
    ReadTimeoutError,
    ServerFrame,
    UnknownFrame,
)


@pytest.fixture()
def connection() -> Connection:
    return Connection(
        connection_parameters=ConnectionParameters("localhost", 12345, login="login", passcode="passcode"),
        connect_timeout=2,
        read_timeout=2,
        read_max_chunk_size=2,
    )


@asynccontextmanager
async def create_server(
    handle_connected: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]],
) -> AsyncGenerator[tuple[str, int], None]:
    async def handle_connected_full(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_connected(reader, writer)

        async def shutdown() -> None:
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        task_group.create_task(shutdown())

    host, port = "localhost", 61637

    async with asyncio.TaskGroup() as task_group:  # noqa: SIM117
        async with await asyncio.start_server(handle_connected_full, host, port):
            yield host, port


def mock_wait_for(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_impl(future: Awaitable[Any], timeout: int) -> Any:  # noqa: ANN401, ARG001
        return await original_wait_for(future, timeout=0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", mock_impl)


@pytest.mark.skipif(platform.system() != "Darwin", reason='reader.read() reads b"" on linux')
async def test_connection_lifespan(connection: Connection) -> None:
    async def handle_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async def validate_content() -> None:
            await asyncio.sleep(0)
            assert await reader.read() == b"\nSOME_COMMAND\nheader:1.0\n\n\x00"

        task_group.create_task(validate_content())

        writer.write(b"\n")
        writer.write(b"\n")
        writer.write(b"\n")
        writer.write(b"CONNECTED\nheart-beat:0,0\nserver:some server\nversion:1.1\n\n\x00")

    async with asyncio.TaskGroup() as task_group:  # noqa: SIM117
        async with create_server(handle_connected) as (host, port):
            connection.connection_parameters.host = host
            connection.connection_parameters.port = port
            await connection.connect()

            connection.write_heartbeat()
            await connection.write_frame(UnknownFrame(command="SOME_COMMAND", headers={"header": "1.0"}))

            async def take_frames(count: int) -> list[ServerFrame | UnknownFrame]:
                frames = []
                async for frame in connection.read_frames():
                    frames.append(frame)
                    if len(frames) == count:
                        break

                return frames

            expected_frames = [
                HeartbeatFrame(),
                HeartbeatFrame(),
                HeartbeatFrame(),
                ConnectedFrame(headers={"heart-beat": "0,0", "version": "1.1", "server": "some server"}),
            ]
            assert await take_frames(len(expected_frames)) == expected_frames
            await connection.close()


async def test_connection_timeout(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    mock_wait_for(monkeypatch)
    with pytest.raises(ConnectError):
        await connection.connect()


async def test_connection_error(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    monkeypatch.setattr("asyncio.open_connection", mock.AsyncMock(side_effect=ConnectionError))
    with pytest.raises(ConnectError):
        await connection.connect()


async def test_read_timeout(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    async with create_server(mock.AsyncMock()) as (host, port):
        connection.connection_parameters.host = host
        connection.connection_parameters.port = port
        await connection.connect()
        mock_wait_for(monkeypatch)
        with pytest.raises(ReadTimeoutError):
            [frame async for frame in connection.read_frames()]
