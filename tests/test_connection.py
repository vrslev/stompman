import asyncio
from collections.abc import Awaitable
from functools import partial
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
)
from stompman.frames import CommitFrame


@pytest.fixture()
def connection() -> Connection:
    return Connection(
        connection_parameters=ConnectionParameters("localhost", 12345, login="login", passcode="passcode"),
        connect_timeout=2,
        read_timeout=2,
        read_max_chunk_size=2,
    )


def mock_wait_for(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_impl(future: Awaitable[Any], timeout: int) -> Any:  # noqa: ANN401, ARG001
        return await original_wait_for(future, timeout=0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", mock_impl)


async def test_connection_lifespan(connection: Connection, monkeypatch: pytest.MonkeyPatch) -> None:
    class MockWriter:
        close = mock.Mock()
        wait_closed = mock.AsyncMock()
        write = mock.Mock()
        drain = mock.AsyncMock()

    read_bytes = [
        b"\n\n",
        b"\nC",
        b"ON",
        b"NE",
        b"CT",
        b"ED",
        b"\n",
        b"he",
        b"ar",
        b"t-",
        b"be",
        b"at",
        b":0",
        b",0",
        b"\nse",
        b"rv",
        b"er:",
        b"som",
        b"e server\nversion:1.2\n\n\x00",
    ]

    class MockReader:
        read = mock.AsyncMock(side_effect=read_bytes)

    monkeypatch.setattr("asyncio.open_connection", mock.AsyncMock(return_value=(MockReader(), MockWriter())))

    await connection.connect()
    connection.write_heartbeat()
    await connection.write_frame(CommitFrame(headers={"transaction": "transaction"}))

    async def take_frames(count: int) -> list[ServerFrame]:
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
        ConnectedFrame(headers={"heart-beat": "0,0", "version": "1.2", "server": "some server"}),
    ]
    assert await take_frames(len(expected_frames)) == expected_frames
    await connection.close()

    MockWriter.close.assert_called_once_with()
    MockWriter.wait_closed.assert_called_once_with()
    MockWriter.drain.assert_called_once_with()
    MockReader.read.mock_calls = [mock.call(connection.read_timeout)] * len(read_bytes)  # type: ignore[assignment]
    assert MockWriter.write.mock_calls == [mock.call(b"\n"), mock.call(b"COMMIT\ntransaction:transaction\n\n\x00")]


async def test_connection_timeout(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    mock_wait_for(monkeypatch)
    with pytest.raises(ConnectError):
        await connection.connect()


async def test_connection_error(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    monkeypatch.setattr("asyncio.open_connection", mock.AsyncMock(side_effect=ConnectionError))
    with pytest.raises(ConnectError):
        await connection.connect()


async def test_read_timeout(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    monkeypatch.setattr(
        "asyncio.open_connection",
        mock.AsyncMock(return_value=(mock.AsyncMock(read=partial(asyncio.sleep, 5)), mock.AsyncMock())),
    )
    await connection.connect()
    mock_wait_for(monkeypatch)
    with pytest.raises(ReadTimeoutError):
        [frame async for frame in connection.read_frames()]
